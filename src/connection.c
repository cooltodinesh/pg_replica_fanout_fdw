/*-------------------------------------------------------------------------
 *
 * connection.c
 *		  Connection cache, concurrent connect, streaming loop and remote
 *		  transaction management for pg_replica_fdw.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "libpq/libpq-be-fe-helpers.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "pg_replica_fdw.h"

/* custom wait events, registered on first use */
static uint32 we_connect = 0;
static uint32 we_stream = 0;

/* cap on how many WaitEvents we ask WaitEventSetWait() to return at once */
#define REP_MAX_WAIT_EVENTS 64

/* connection cache: user mapping OID -> ReplicaSet */
static HTAB *ReplicaSetHash = NULL;

/* long-lived context backing the variable-size parts of cache entries */
static MemoryContext RepFdwCacheContext = NULL;

static void pgreplicafdw_xact_callback(XactEvent event, void *arg);
static void ensure_connected(ReplicaSet *rset, RepFdwOptions *opts,
							  UserMapping *user);
static PGconn *start_connect(ReplicaConn *rconn, RepFdwOptions *opts,
							  UserMapping *user);
static void abort_pending_connects(ReplicaSet *rset,
									PostgresPollingStatusType *pollstatus);
static void drain_conn(ReplicaConn *rconn, int fetch_size);

/*
 * RepFdwGetConnections
 *		Find or create the cached ReplicaSet for this user mapping, and make
 *		sure every replica connection in it is live.
 */
ReplicaSet *
RepFdwGetConnections(UserMapping *user, RepFdwOptions *opts)
{
	bool		found;
	ReplicaSet *rset;
	Oid			key = user->umid;

	if (ReplicaSetHash == NULL)
	{
		HASHCTL		ctl;

		RepFdwCacheContext = AllocSetContextCreate(TopMemoryContext,
													"pg_replica_fdw connection cache",
													ALLOCSET_SMALL_SIZES);

		if (we_connect == 0)
			we_connect = WaitEventExtensionNew("PgReplicaFdwConnect");
		if (we_stream == 0)
			we_stream = WaitEventExtensionNew("PgReplicaFdwStream");

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(ReplicaSet);
		ReplicaSetHash = hash_create("pg_replica_fdw replica sets", 8, &ctl,
									 HASH_ELEM | HASH_BLOBS);

		RegisterXactCallback(pgreplicafdw_xact_callback, NULL);
	}

	rset = (ReplicaSet *) hash_search(ReplicaSetHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		int			nconns = list_length(opts->replicas);
		ListCell   *lc;
		int			i;
		MemoryContext oldcxt;

		rset->nconns = nconns;
		rset->xact_open = false;

		oldcxt = MemoryContextSwitchTo(RepFdwCacheContext);
		rset->conns = palloc0_array(ReplicaConn, nconns);
		i = 0;
		foreach(lc, opts->replicas)
		{
			RepHostPort *hp = (RepHostPort *) lfirst(lc);
			ReplicaConn *rconn = &rset->conns[i];

			rconn->conn = NULL;
			rconn->index = i;
			rconn->host = pstrdup(hp->host);
			rconn->port = hp->port;
			rconn->state = REP_DISCONNECTED;
			rconn->rowqueue = NIL;
			rconn->cur_row = 0;
			rconn->paused = false;
			i++;
		}
		MemoryContextSwitchTo(oldcxt);
	}

	ensure_connected(rset, opts, user);

	return rset;
}

/*
 * start_connect
 *		Kick off a non-blocking connection attempt to one replica.
 */
static PGconn *
start_connect(ReplicaConn *rconn, RepFdwOptions *opts, UserMapping *user)
{
	const char *keywords[12];
	const char *values[12];
	char		portbuf[16];
	char		timeoutbuf[16];
	int			n = 0;
	ListCell   *lc;
	PGconn	   *conn;

	snprintf(portbuf, sizeof(portbuf), "%d", rconn->port);
	snprintf(timeoutbuf, sizeof(timeoutbuf), "%d", opts->connect_timeout);

	keywords[n] = "host";
	values[n++] = rconn->host;
	keywords[n] = "port";
	values[n++] = portbuf;
	keywords[n] = "dbname";
	values[n++] = opts->dbname;
	keywords[n] = "application_name";
	values[n++] = opts->application_name;
	keywords[n] = "connect_timeout";
	values[n++] = timeoutbuf;

	foreach(lc, user->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "user") == 0)
		{
			keywords[n] = "user";
			values[n++] = defGetString(def);
		}
		else if (strcmp(def->defname, "password") == 0)
		{
			keywords[n] = "password";
			values[n++] = defGetString(def);
		}
	}

	keywords[n] = "fallback_application_name";
	values[n++] = "pg_replica_fdw";
	keywords[n] = NULL;
	values[n] = NULL;

	conn = libpqsrv_connect_params_start(keywords, values, false);
	if (conn != NULL)
		PQsetNoticeReceiver(conn, libpqsrv_notice_receiver, "pg_replica_fdw");

	return conn;
}

/*
 * abort_pending_connects
 *		Disconnect every replica still mid-connect, on the way out through
 *		an error.  Prevents leaking sockets/external-FD slots.
 */
static void
abort_pending_connects(ReplicaSet *rset, PostgresPollingStatusType *pollstatus)
{
	int			i;

	for (i = 0; i < rset->nconns; i++)
	{
		ReplicaConn *rconn = &rset->conns[i];

		if (rconn->state == REP_CONNECTING)
		{
			libpqsrv_disconnect(rconn->conn);
			rconn->conn = NULL;
			rconn->state = REP_DISCONNECTED;
		}
	}
}

/*
 * ensure_connected
 *		Concurrently (re)connect every replica that isn't currently usable.
 *		On any failure or timeout, ereport(ERROR) naming the offending
 *		replica; there is no degraded mode (a failed replica fails the scan).
 */
static void
ensure_connected(ReplicaSet *rset, RepFdwOptions *opts, UserMapping *user)
{
	int			i;
	int			nneed = 0;
	PostgresPollingStatusType *pollstatus;
	TimestampTz endtime;

	for (i = 0; i < rset->nconns; i++)
		if (rset->conns[i].state == REP_DISCONNECTED ||
			rset->conns[i].state == REP_DEAD)
			nneed++;

	if (nneed == 0)
		return;

	pollstatus = palloc(sizeof(PostgresPollingStatusType) * rset->nconns);

	PG_TRY();
	{
		for (i = 0; i < rset->nconns; i++)
		{
			ReplicaConn *rconn = &rset->conns[i];

			pollstatus[i] = PGRES_POLLING_WRITING;

			if (rconn->state != REP_DISCONNECTED && rconn->state != REP_DEAD)
				continue;

			if (rconn->conn != NULL)
			{
				libpqsrv_disconnect(rconn->conn);
				rconn->conn = NULL;
			}

			rconn->conn = start_connect(rconn, opts, user);
			rconn->state = REP_CONNECTING;
		}

		endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
											  opts->connect_timeout * 1000);

		for (;;)
		{
			WaitEventSet *wes;
			WaitEvent	occurred[REP_MAX_WAIT_EVENTS];
			int			noccurred;
			int			nleft = 0;
			long		timeout_ms;

			for (i = 0; i < rset->nconns; i++)
				if (rset->conns[i].state == REP_CONNECTING)
					nleft++;

			if (nleft == 0)
				break;

			timeout_ms = (long) ((endtime - GetCurrentTimestamp()) / 1000);
			if (timeout_ms <= 0)
			{
				StringInfoData hosts;
				initStringInfo(&hosts);
				for (i = 0; i < rset->nconns; i++)
					if (rset->conns[i].state == REP_CONNECTING)
						appendStringInfo(&hosts, "%s%s:%d",
										 hosts.len ? ", " : "",
										 rset->conns[i].host,
										 rset->conns[i].port);
				abort_pending_connects(rset, pollstatus);
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("timed out connecting to replica(s): %s",
								hosts.data)));
			}

			wes = CreateWaitEventSet(CurrentResourceOwner, rset->nconns + 2);
			AddWaitEventToSet(wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
							  NULL, NULL);
			AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET,
							  MyLatch, NULL);

			for (i = 0; i < rset->nconns; i++)
			{
				ReplicaConn *rconn = &rset->conns[i];
				uint32		ev;

				if (rconn->state != REP_CONNECTING)
					continue;

				ev = (pollstatus[i] == PGRES_POLLING_READING) ?
					WL_SOCKET_READABLE : WL_SOCKET_WRITEABLE;
				AddWaitEventToSet(wes, ev, PQsocket(rconn->conn), NULL, rconn);
			}

			noccurred = WaitEventSetWait(wes, timeout_ms, occurred,
										 Min(rset->nconns + 2, REP_MAX_WAIT_EVENTS),
										 we_connect);
			FreeWaitEventSet(wes);

			for (i = 0; i < noccurred; i++)
			{
				WaitEvent  *w = &occurred[i];

				if (w->events & WL_LATCH_SET)
				{
					ResetLatch(MyLatch);
					CHECK_FOR_INTERRUPTS();
				}

				if (w->events & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE))
				{
					ReplicaConn *rconn = (ReplicaConn *) w->user_data;
					int			idx = rconn->index;

					pollstatus[idx] = PQconnectPoll(rconn->conn);

					if (pollstatus[idx] == PGRES_POLLING_OK)
					{
						rconn->state = REP_IN_TXN;
					}
					else if (pollstatus[idx] == PGRES_POLLING_FAILED)
					{
						char	   *msg = pstrdup(PQerrorMessage(rconn->conn));
						char	   *host = pstrdup(rconn->host);
						int			port = rconn->port;

						abort_pending_connects(rset, pollstatus);
						ereport(ERROR,
								(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
								 errmsg("could not connect to replica \"%s:%d\"",
										host, port),
								 errdetail_internal("%s", pchomp(msg))));
					}
				}
			}
		}
	}
	PG_CATCH();
	{
		abort_pending_connects(rset, pollstatus);
		PG_RE_THROW();
	}
	PG_END_TRY();

	pfree(pollstatus);
}

/*
 * RepFdwBeginRemoteXact
 *		Open a REPEATABLE READ READ ONLY transaction on every replica, once
 *		per local transaction.
 */
void
RepFdwBeginRemoteXact(ReplicaSet *rset)
{
	int			i;

	if (rset->xact_open)
		return;

	for (i = 0; i < rset->nconns; i++)
	{
		ReplicaConn *rconn = &rset->conns[i];
		PGresult   *res;

		res = libpqsrv_exec(rconn->conn,
							"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
							we_stream);
		if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK)
			RepFdwReportError(res, rconn,
							  "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
		PQclear(res);
	}

	rset->xact_open = true;
}

/*
 * RepFdwExecSync
 *		Run one non-streaming command/query on a replica and return its
 *		result, interruptibly.  ereport(ERROR) on any failure.
 */
PGresult *
RepFdwExecSync(ReplicaConn *rconn, const char *sql)
{
	PGresult   *res = libpqsrv_exec(rconn->conn, sql, we_stream);

	if (res == NULL ||
		(PQresultStatus(res) != PGRES_COMMAND_OK &&
		 PQresultStatus(res) != PGRES_TUPLES_OK))
		RepFdwReportError(res, rconn, sql);

	return res;
}

/*
 * RepFdwStartQueries
 *		Send the per-replica SELECT (ctid range bound via $1/$2) to the
 *		first nactive connections, in chunked-rows streaming mode.
 */
void
RepFdwStartQueries(ReplicaSet *rset, int nactive, char **sqls,
				   RepFdwCtidBound *bounds, int fetch_size)
{
	int			i;

	for (i = 0; i < nactive; i++)
	{
		ReplicaConn *rconn = &rset->conns[i];
		Oid			paramTypes[2];
		const char *paramValues[2];
		int			nparams = 0;

		if (bounds[i].lo != NULL)
		{
			paramTypes[nparams] = TIDOID;
			paramValues[nparams] = bounds[i].lo;
			nparams++;
		}
		if (bounds[i].hi != NULL)
		{
			paramTypes[nparams] = TIDOID;
			paramValues[nparams] = bounds[i].hi;
			nparams++;
		}

		if (!PQsendQueryParams(rconn->conn, sqls[i], nparams, paramTypes,
							   paramValues, NULL, NULL, 0))
			RepFdwReportError(NULL, rconn, sqls[i]);

		/* Must be called after PQsendQueryParams, before consuming any results. */
		if (PQsetChunkedRowsMode(rconn->conn, fetch_size) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("could not enable chunked rows mode for replica \"%s:%d\"",
							rconn->host, rconn->port)));

		rconn->state = REP_STREAMING;
		rconn->rowqueue = NIL;
		rconn->cur_row = 0;
		rconn->paused = false;
	}
}

/*
 * RepFdwQueuedRowCount
 *		Number of not-yet-consumed rows currently buffered for a replica.
 */
int
RepFdwQueuedRowCount(ReplicaConn *rconn)
{
	ListCell   *lc;
	int			n = 0;
	bool		first = true;

	foreach(lc, rconn->rowqueue)
	{
		PGresult   *res = (PGresult *) lfirst(lc);

		if (first)
		{
			n += PQntuples(res) - rconn->cur_row;
			first = false;
		}
		else
			n += PQntuples(res);
	}
	return n;
}

/*
 * drain_conn
 *		Pull every result currently available (without blocking) from one
 *		replica connection, queueing chunks and detecting completion/errors.
 */
static void
drain_conn(ReplicaConn *rconn, int fetch_size)
{
	if (PQconsumeInput(rconn->conn) == 0)
	{
		char	   *msg = pstrdup(PQerrorMessage(rconn->conn));

		rconn->state = REP_DEAD;
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("lost connection to replica \"%s:%d\"",
						rconn->host, rconn->port),
				 errdetail_internal("%s", pchomp(msg))));
	}

	while (!PQisBusy(rconn->conn))
	{
		PGresult   *res = PQgetResult(rconn->conn);

		if (res == NULL)
			break;				/* fully drained until more bytes arrive */

		switch (PQresultStatus(res))
		{
			case PGRES_TUPLES_CHUNK:
				rconn->rowqueue = lappend(rconn->rowqueue, res);
				if (RepFdwQueuedRowCount(rconn) > 2 * fetch_size)
					rconn->paused = true;
				continue;
			case PGRES_TUPLES_OK:
				PQclear(res);
				rconn->state = REP_DONE;
				continue;
			case PGRES_FATAL_ERROR:
				RepFdwReportError(res, rconn, NULL);
				break;			/* unreachable */
			default:
				PQclear(res);
				continue;
		}
	}
}

/*
 * RepStreamPump
 *		Advance the streaming loop until at least one replica makes
 *		progress (gains a queued chunk or finishes) or there is nothing
 *		left to wait for.
 */
void
RepStreamPump(ReplicaSet *rset, int nactive, int fetch_size)
{
	int			i;
	int			nevents;

	nevents = 0;
	for (i = 0; i < nactive; i++)
		if (rset->conns[i].state == REP_STREAMING && !rset->conns[i].paused)
			nevents++;

	if (nevents == 0)
		return;

	for (;;)
	{
		WaitEventSet *wes;
		WaitEvent	occurred[REP_MAX_WAIT_EVENTS];
		int			noccurred;
		bool		made_progress = false;

		wes = CreateWaitEventSet(CurrentResourceOwner, nactive + 2);
		AddWaitEventToSet(wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
						  NULL, NULL);
		AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);

		for (i = 0; i < nactive; i++)
		{
			ReplicaConn *rconn = &rset->conns[i];

			if (rconn->state != REP_STREAMING || rconn->paused)
				continue;

			AddWaitEventToSet(wes, WL_SOCKET_READABLE, PQsocket(rconn->conn),
							  NULL, rconn);
		}

		noccurred = WaitEventSetWait(wes, -1, occurred,
									 Min(nactive + 2, REP_MAX_WAIT_EVENTS),
									 we_stream);
		FreeWaitEventSet(wes);

		for (i = 0; i < noccurred; i++)
		{
			WaitEvent  *w = &occurred[i];

			if (w->events & WL_LATCH_SET)
			{
				ResetLatch(MyLatch);
				CHECK_FOR_INTERRUPTS();
			}

			if (w->events & WL_SOCKET_READABLE)
			{
				ReplicaConn *rconn = (ReplicaConn *) w->user_data;
				int			before_rows = RepFdwQueuedRowCount(rconn);
				RepConnState before_state = rconn->state;

				drain_conn(rconn, fetch_size);

				if (rconn->state != before_state ||
					RepFdwQueuedRowCount(rconn) != before_rows)
					made_progress = true;
			}
		}

		if (made_progress)
			break;
	}
}

/*
 * RepFdwCancelAndDrain
 *		Cancel any in-flight query on the first nactive replicas and drain
 *		results so the connections go back to idle-in-transaction.  Used by
 *		both ReScan (before resending) and EndForeignScan (before caching
 *		the connection for the next query).
 */
void
RepFdwCancelAndDrain(ReplicaSet *rset, int nactive)
{
	int			i;
	TimestampTz endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 5000);

	for (i = 0; i < nactive; i++)
	{
		ReplicaConn *rconn = &rset->conns[i];
		ListCell   *lc;

		if (rconn->conn == NULL)
			continue;

		if (rconn->state == REP_STREAMING)
		{
			const char *err = libpqsrv_cancel(rconn->conn, endtime);

			if (err != NULL)
				ereport(WARNING,
						(errmsg("could not cancel query on replica \"%s:%d\": %s",
								rconn->host, rconn->port, err)));

			/*
			 * Keep fetching until PQgetResult truly returns NULL -- that is
			 * the only thing that resets libpq's asyncStatus to IDLE, so we
			 * must not stop merely because we've seen PGRES_TUPLES_OK.
			 */
			for (;;)
			{
				PGresult   *res = libpqsrv_get_result(rconn->conn, we_stream);

				if (res == NULL)
					break;
				if (PQresultStatus(res) == PGRES_TUPLES_OK)
					rconn->state = REP_DONE;
				PQclear(res);
			}
			rconn->state = REP_DONE;
		}

		foreach(lc, rconn->rowqueue)
			PQclear((PGresult *) lfirst(lc));
		list_free(rconn->rowqueue);
		rconn->rowqueue = NIL;
		rconn->cur_row = 0;
		rconn->paused = false;
		if (rconn->state != REP_DEAD)
			rconn->state = REP_IN_TXN;
	}
}

/*
 * RepFdwReportError
 *		Report a remote error or connection failure, always as ERROR,
 *		naming the offending replica.  Marks the connection dead.
 */
void
RepFdwReportError(PGresult *res, ReplicaConn *rconn, const char *sql)
{
	char	   *diag_sqlstate = res ? PQresultErrorField(res, PG_DIAG_SQLSTATE) : NULL;
	char	   *message_primary = res ? PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY) : NULL;
	char	   *message_detail = res ? PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL) : NULL;
	char	   *message_hint = res ? PQresultErrorField(res, PG_DIAG_MESSAGE_HINT) : NULL;
	int			sqlstate;

	if (diag_sqlstate)
		sqlstate = MAKE_SQLSTATE(diag_sqlstate[0], diag_sqlstate[1],
								 diag_sqlstate[2], diag_sqlstate[3],
								 diag_sqlstate[4]);
	else
		sqlstate = ERRCODE_CONNECTION_FAILURE;

	if (message_primary == NULL && rconn->conn != NULL)
		message_primary = pchomp(PQerrorMessage(rconn->conn));

	rconn->state = REP_DEAD;

	ereport(ERROR,
			(errcode(sqlstate),
			 (message_primary != NULL && message_primary[0] != '\0') ?
			 errmsg_internal("%s", message_primary) :
			 errmsg("could not obtain message string for remote error"),
			 message_detail ? errdetail_internal("%s", message_detail) : 0,
			 message_hint ? errhint("%s", message_hint) : 0,
			 sql ?
			 errcontext("remote SQL command (replica %s:%d): %s",
						rconn->host, rconn->port, sql) :
			 errcontext("replica %s:%d", rconn->host, rconn->port)));
}

/*
 * pgreplicafdw_xact_callback
 *		At local transaction end, close out any open remote transactions
 *		and reset per-scan bookkeeping.  Connections themselves are kept
 *		open (cached) unless something went wrong.
 */
static void
pgreplicafdw_xact_callback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS scan;
	ReplicaSet *rset;

	if (ReplicaSetHash == NULL)
		return;

	hash_seq_init(&scan, ReplicaSetHash);
	while ((rset = (ReplicaSet *) hash_seq_search(&scan)) != NULL)
	{
		int			i;

		if (!rset->xact_open)
			continue;

		for (i = 0; i < rset->nconns; i++)
		{
			ReplicaConn *rconn = &rset->conns[i];
			ListCell   *lc;

			if (rconn->conn != NULL && rconn->state != REP_DEAD)
			{
				PGresult   *res;

				switch (event)
				{
					case XACT_EVENT_PRE_COMMIT:
						res = libpqsrv_exec(rconn->conn, "COMMIT", we_stream);
						if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK)
							ereport(WARNING,
									(errmsg("could not commit remote transaction on replica \"%s:%d\"",
											rconn->host, rconn->port)));
						if (res)
							PQclear(res);
						break;

					case XACT_EVENT_ABORT:
						if (rconn->state == REP_STREAMING)
							(void) libpqsrv_cancel(rconn->conn,
												   TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 5000));
						res = libpqsrv_exec(rconn->conn, "ROLLBACK", we_stream);
						if (res)
							PQclear(res);
						break;

					default:
						break;
				}
			}

			foreach(lc, rconn->rowqueue)
				PQclear((PGresult *) lfirst(lc));
			list_free(rconn->rowqueue);
			rconn->rowqueue = NIL;
			rconn->cur_row = 0;
			rconn->paused = false;

			if (rconn->conn != NULL && PQstatus(rconn->conn) != CONNECTION_OK)
			{
				libpqsrv_disconnect(rconn->conn);
				rconn->conn = NULL;
				rconn->state = REP_DISCONNECTED;
			}
			else if (rconn->state != REP_DEAD)
				rconn->state = REP_IN_TXN;
		}

		rset->xact_open = false;
	}
}
