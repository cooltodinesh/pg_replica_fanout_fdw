/*-------------------------------------------------------------------------
 *
 * connection.c
 *		  Connection cache, concurrent connect, streaming loop and remote
 *		  transaction management for pg_replica_fanout_fdw.
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
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "pg_replica_fanout_fdw.h"

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
static void rep_inval_callback(Datum arg, SysCacheIdentifier cacheid,
								uint32 hashvalue);
static void ensure_connected(ReplicaSet *rset, RepFdwOptions *opts,
							  UserMapping *user);
static PGconn *start_connect(ReplicaConn *rconn, RepFdwOptions *opts,
							  UserMapping *user);
static void begin_conn_xact(ReplicaConn *rconn);
static PGconn *connect_one(const char *host, int port, RepFdwOptions *opts,
						   UserMapping *user);
static void abort_pending_connects(ReplicaSet *rset);
static void drain_conn(ReplicaConn *rconn, int fetch_size);
static void destroy_replicaset_conns(ReplicaSet *rset);

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
													"pg_replica_fanout_fdw connection cache",
													ALLOCSET_SMALL_SIZES);

		if (we_connect == 0)
			we_connect = WaitEventExtensionNew("PgReplicaFanoutFdwConnect");
		if (we_stream == 0)
			we_stream = WaitEventExtensionNew("PgReplicaFanoutFdwStream");

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(ReplicaSet);
		ReplicaSetHash = hash_create("pg_replica_fanout_fdw replica sets", 8, &ctl,
									 HASH_ELEM | HASH_BLOBS);

		RegisterXactCallback(pgreplicafdw_xact_callback, NULL);
		CacheRegisterSyscacheCallback(FOREIGNSERVEROID, rep_inval_callback,
									  (Datum) 0);
		CacheRegisterSyscacheCallback(USERMAPPINGOID, rep_inval_callback,
									  (Datum) 0);
	}

	for (;;)
	{
		rset = (ReplicaSet *) hash_search(ReplicaSetHash, &key, HASH_ENTER, &found);

		if (found && rset->invalidated)
		{
			/*
			 * ALTER SERVER / ALTER USER MAPPING touched this entry.  If
			 * nothing is using it right now, tear it down and loop back to
			 * rebuild it from scratch with fresh options below.  If a scan
			 * or remote transaction is still active, leave it as-is for
			 * this transaction; pgreplicafdw_xact_callback tears it down at
			 * xact end instead, since we can't drop connections a scan is
			 * mid-read on.
			 */
			if (!rset->xact_open)
			{
				destroy_replicaset_conns(rset);
				hash_search(ReplicaSetHash, &key, HASH_REMOVE, NULL);
				continue;
			}
		}
		break;
	}

	if (!found)
	{
		int			nconns = list_length(opts->replicas);
		ListCell   *lc;
		int			i;
		MemoryContext oldcxt;

		rset->nconns = nconns;
		rset->overflow_conns = NIL;
		rset->xact_open = false;
		rset->invalidated = false;
		rset->server_hashvalue =
			GetSysCacheHashValue1(FOREIGNSERVEROID,
								  ObjectIdGetDatum(user->serverid));
		rset->mapping_hashvalue =
			GetSysCacheHashValue1(USERMAPPINGOID,
								  ObjectIdGetDatum(user->umid));

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
			rconn->in_use = false;
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
	values[n++] = "pg_replica_fanout_fdw";
	keywords[n] = NULL;
	values[n] = NULL;

	conn = libpqsrv_connect_params_start(keywords, values, false);
	if (conn != NULL)
		PQsetNoticeReceiver(conn, libpqsrv_notice_receiver, "pg_replica_fanout_fdw");

	return conn;
}

/*
 * abort_pending_connects
 *		Disconnect every replica still mid-connect, on the way out through
 *		an error.  Prevents leaking sockets/external-FD slots.
 */
static void
abort_pending_connects(ReplicaSet *rset)
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
 * destroy_replicaset_conns
 *		Disconnect every connection in a ReplicaSet and free the per-conn
 *		allocations backing it (the conns array and each host string, both
 *		palloc'd out of RepFdwCacheContext).  Used when rebuilding an
 *		invalidated entry -- nconns/replicas may have changed, so the array
 *		can't just be reused in place.  Does not touch the hash entry
 *		itself; the caller removes or repopulates it.
 */
static void
destroy_replicaset_conns(ReplicaSet *rset)
{
	int			i;
	ListCell   *lc_over;

	for (i = 0; i < rset->nconns; i++)
	{
		ReplicaConn *rconn = &rset->conns[i];
		ListCell   *lc;

		if (rconn->conn != NULL)
		{
			libpqsrv_disconnect(rconn->conn);
			rconn->conn = NULL;
		}

		foreach(lc, rconn->rowqueue)
			PQclear((PGresult *) lfirst(lc));
		list_free(rconn->rowqueue);

		if (rconn->host != NULL)
			pfree(rconn->host);
	}

	/* Overflow conns are normally torn down at xact end; be defensive. */
	foreach(lc_over, rset->overflow_conns)
	{
		ReplicaConn *oc = (ReplicaConn *) lfirst(lc_over);
		ListCell   *lc;

		if (oc->conn != NULL)
			libpqsrv_disconnect(oc->conn);
		foreach(lc, oc->rowqueue)
			PQclear((PGresult *) lfirst(lc));
		list_free(oc->rowqueue);
		if (oc->host != NULL)
			pfree(oc->host);
		pfree(oc);
	}
	list_free(rset->overflow_conns);
	rset->overflow_conns = NIL;

	if (rset->conns != NULL)
		pfree(rset->conns);
	rset->conns = NULL;
	rset->nconns = 0;
}

/*
 * ensure_connected
 *		Concurrently (re)connect every replica that isn't currently usable.
 *		On any failure or timeout, ereport(ERROR) naming the offending
 *		replica; there is no degraded mode (a failed replica fails the scan).
 *
 *		The WaitEventSet is built once and reused across wakeups, rebuilt
 *		only when the in-flight connection count drops (a conn reached OK
 *		or FAILED) -- not on every wakeup.  To avoid also having to rebuild
 *		on every read/write direction flip, in-flight conns are always
 *		registered for both WL_SOCKET_READABLE and WL_SOCKET_WRITEABLE and
 *		PQconnectPoll() is left to sort out which one actually applies.
 */
static void
ensure_connected(ReplicaSet *rset, RepFdwOptions *opts, UserMapping *user)
{
	int			i;
	int			nneed = 0;
	TimestampTz endtime;
	WaitEventSet *wes = NULL;
	int			wes_nleft = -1;

	for (i = 0; i < rset->nconns; i++)
		if (rset->conns[i].state == REP_DISCONNECTED ||
			rset->conns[i].state == REP_DEAD)
			nneed++;

	if (nneed == 0)
		return;

	PG_TRY();
	{
		for (i = 0; i < rset->nconns; i++)
		{
			ReplicaConn *rconn = &rset->conns[i];

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
				abort_pending_connects(rset);
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("timed out connecting to replica(s): %s",
								hosts.data)));
			}

			if (wes == NULL || nleft != wes_nleft)
			{
				if (wes != NULL)
					FreeWaitEventSet(wes);

				wes = CreateWaitEventSet(CurrentResourceOwner, rset->nconns + 2);
				AddWaitEventToSet(wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET,
								  NULL, NULL);
				AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET,
								  MyLatch, NULL);

				for (i = 0; i < rset->nconns; i++)
				{
					ReplicaConn *rconn = &rset->conns[i];

					if (rconn->state != REP_CONNECTING)
						continue;

					AddWaitEventToSet(wes,
									  WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE,
									  PQsocket(rconn->conn), NULL, rconn);
				}

				wes_nleft = nleft;
			}

			noccurred = WaitEventSetWait(wes, timeout_ms, occurred,
										 Min(rset->nconns + 2, REP_MAX_WAIT_EVENTS),
										 we_connect);

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
					PostgresPollingStatusType status;

					status = PQconnectPoll(rconn->conn);

					if (status == PGRES_POLLING_OK)
					{
						rconn->state = REP_IN_TXN;
					}
					else if (status == PGRES_POLLING_FAILED)
					{
						char	   *msg = pstrdup(PQerrorMessage(rconn->conn));
						char	   *host = pstrdup(rconn->host);
						int			port = rconn->port;

						abort_pending_connects(rset);
						ereport(ERROR,
								(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
								 errmsg("could not connect to replica \"%s:%d\"",
										host, port),
								 errdetail_internal("%s", pchomp(msg))));
					}
				}
			}
		}

		if (wes != NULL)
		{
			FreeWaitEventSet(wes);
			wes = NULL;
		}
	}
	PG_CATCH();
	{
		abort_pending_connects(rset);
		PG_RE_THROW();
	}
	PG_END_TRY();
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
 * begin_conn_xact
 *		Open a REPEATABLE READ READ ONLY transaction on a single connection
 *		(used for overflow connections created mid-transaction).
 */
static void
begin_conn_xact(ReplicaConn *rconn)
{
	PGresult   *res = libpqsrv_exec(rconn->conn,
									"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
									we_stream);

	if (res == NULL || PQresultStatus(res) != PGRES_COMMAND_OK)
		RepFdwReportError(res, rconn,
						  "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
	PQclear(res);
	rconn->state = REP_IN_TXN;
}

/*
 * connect_one
 *		Blockingly (but interruptibly) connect one new replica connection to
 *		host:port and return it CONNECTION_OK; ereport(ERROR) on failure.  Used
 *		for overflow connections; the cached primaries are still connected
 *		concurrently in ensure_connected().
 */
static PGconn *
connect_one(const char *host, int port, RepFdwOptions *opts, UserMapping *user)
{
	const char *keywords[12];
	const char *values[12];
	char		portbuf[16];
	char		timeoutbuf[16];
	int			n = 0;
	ListCell   *lc;
	PGconn	   *conn;

	snprintf(portbuf, sizeof(portbuf), "%d", port);
	snprintf(timeoutbuf, sizeof(timeoutbuf), "%d", opts->connect_timeout);

	keywords[n] = "host";
	values[n++] = host;
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
	values[n++] = "pg_replica_fanout_fdw";
	keywords[n] = NULL;
	values[n] = NULL;

	conn = libpqsrv_connect_params(keywords, values, false, we_connect);
	if (conn == NULL || PQstatus(conn) != CONNECTION_OK)
	{
		char	   *msg = conn ? pchomp(PQerrorMessage(conn)) : "out of memory";

		if (conn != NULL)
			libpqsrv_disconnect(conn);
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not connect to replica \"%s:%d\"", host, port),
				 errdetail_internal("%s", msg)));
	}
	PQsetNoticeReceiver(conn, libpqsrv_notice_receiver, "pg_replica_fanout_fdw");
	return conn;
}

/*
 * RepFdwCheckoutConn
 *		Claim a connection to replica `index` for one live scan node, with its
 *		remote REPEATABLE READ transaction open.  Reuses the cached primary if
 *		idle; otherwise reuses (or creates) an overflow connection -- so two
 *		concurrently live scans of the same server (e.g. a self-join) each get
 *		their own connection instead of colliding on one.
 */
ReplicaConn *
RepFdwCheckoutConn(ReplicaSet *rset, RepFdwOptions *opts, UserMapping *user,
				   int index)
{
	ReplicaConn *primary = &rset->conns[index];
	ReplicaConn *oc;
	ListCell   *lc;
	MemoryContext oldcxt;

	/* 1. The cached primary, if idle (its xact was opened by BeginRemoteXact). */
	if (!primary->in_use)
	{
		primary->in_use = true;
		return primary;
	}

	/* 2. An idle overflow connection to the same replica, if any. */
	foreach(lc, rset->overflow_conns)
	{
		oc = (ReplicaConn *) lfirst(lc);
		if (oc->index == index && !oc->in_use && oc->state != REP_DEAD)
		{
			oc->in_use = true;
			return oc;
		}
	}

	/* 3. Create a new overflow connection (tracked for xact-end cleanup). */
	oldcxt = MemoryContextSwitchTo(RepFdwCacheContext);
	oc = palloc0_object(ReplicaConn);
	oc->index = index;
	oc->host = pstrdup(primary->host);
	oc->port = primary->port;
	oc->rowqueue = NIL;
	rset->overflow_conns = lappend(rset->overflow_conns, oc);
	MemoryContextSwitchTo(oldcxt);

	oc->conn = connect_one(oc->host, oc->port, opts, user);
	begin_conn_xact(oc);
	oc->in_use = true;
	return oc;
}

/*
 * RepFdwReturnConn
 *		Release a checked-out connection: cancel/drain any in-flight query back
 *		to idle-in-transaction and clear the in-use claim.  The remote
 *		transaction stays open until local xact end.
 */
void
RepFdwReturnConn(ReplicaConn *rconn)
{
	if (rconn == NULL)
		return;
	RepFdwCancelDrainOne(rconn);
	rconn->in_use = false;
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
 * RepFdwStartOneQuery
 *		Send one ctid-bounded slice query on a single replica connection in
 *		chunked-rows streaming mode (v2 async path -- each Append child owns
 *		one connection and drives it independently).
 */
void
RepFdwStartOneQuery(ReplicaConn *rconn, const char *sql,
					const RepFdwCtidBound *bound, int fetch_size)
{
	Oid			paramTypes[2];
	const char *paramValues[2];
	int			nparams = 0;

	if (bound->lo != NULL)
	{
		paramTypes[nparams] = TIDOID;
		paramValues[nparams] = bound->lo;
		nparams++;
	}
	if (bound->hi != NULL)
	{
		paramTypes[nparams] = TIDOID;
		paramValues[nparams] = bound->hi;
		nparams++;
	}

	if (!PQsendQueryParams(rconn->conn, sql, nparams, paramTypes,
						   paramValues, NULL, NULL, 0))
		RepFdwReportError(NULL, rconn, sql);

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

/*
 * RepFdwDrainConn
 *		Non-blocking: consume whatever input is already available on one
 *		connection, queueing chunks and detecting completion.  Used by the
 *		async ForeignAsyncNotify callback after the socket signals readable.
 */
void
RepFdwDrainConn(ReplicaConn *rconn, int fetch_size)
{
	drain_conn(rconn, fetch_size);
}

/*
 * RepFdwPumpOne
 *		Blocking (but interruptible): wait on one connection's socket and drain
 *		until it makes progress (gains a queued row or finishes).  The
 *		synchronous fallback for the async streaming path -- used by
 *		IterateForeignScan when a child is executed outside an async Append.
 */
void
RepFdwPumpOne(ReplicaConn *rconn, int fetch_size)
{
	while (rconn->state == REP_STREAMING)
	{
		WaitEventSet *wes;
		WaitEvent	occurred[REP_MAX_WAIT_EVENTS];
		int			noccurred;
		int			i;
		int			before_rows = RepFdwQueuedRowCount(rconn);
		RepConnState before_state = rconn->state;

		wes = CreateWaitEventSet(CurrentResourceOwner, 3);
		AddWaitEventToSet(wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
		AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
		AddWaitEventToSet(wes, WL_SOCKET_READABLE, PQsocket(rconn->conn),
						  NULL, rconn);

		noccurred = WaitEventSetWait(wes, -1, occurred, REP_MAX_WAIT_EVENTS,
									 we_stream);

		for (i = 0; i < noccurred; i++)
		{
			if (occurred[i].events & WL_LATCH_SET)
			{
				ResetLatch(MyLatch);
				CHECK_FOR_INTERRUPTS();
			}
			if (occurred[i].events & WL_SOCKET_READABLE)
				drain_conn(rconn, fetch_size);
		}

		FreeWaitEventSet(wes);

		if (rconn->state != before_state ||
			RepFdwQueuedRowCount(rconn) != before_rows)
			return;
	}
}

/*
 * RepFdwCancelDrainOne
 *		Cancel any in-flight query on one connection and drain it back to
 *		idle-in-transaction; used by the per-child ReScan/End and by
 *		RepFdwReturnConn.
 */
void
RepFdwCancelDrainOne(ReplicaConn *rconn)
{
	TimestampTz endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 5000);
	ListCell   *lc;

	if (rconn->conn == NULL)
		return;

	if (rconn->state == REP_STREAMING)
	{
		const char *err = libpqsrv_cancel(rconn->conn, endtime);

		if (err != NULL)
			ereport(WARNING,
					(errmsg("could not cancel query on replica \"%s:%d\": %s",
							rconn->host, rconn->port, err)));

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

/*
 * RepFdwReportError
 *		Report a remote error or connection failure, always as ERROR,
 *		naming the offending replica.  Only marks the connection REP_DEAD
 *		when the socket itself is no longer usable; a benign query-level
 *		error (e.g. relation does not exist) on an otherwise-healthy
 *		connection is drained back to idle instead, so the next statement
 *		can reuse it rather than paying for a reconnect.
 */
void
RepFdwReportError(PGresult *res, ReplicaConn *rconn, const char *sql)
{
	char	   *diag_sqlstate = res ? PQresultErrorField(res, PG_DIAG_SQLSTATE) : NULL;
	char	   *message_primary = res ? PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY) : NULL;
	char	   *message_detail = res ? PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL) : NULL;
	char	   *message_hint = res ? PQresultErrorField(res, PG_DIAG_MESSAGE_HINT) : NULL;
	char	   *ctxmsg;
	int			sqlstate;
	bool		conn_ok = (rconn->conn != NULL &&
						   PQstatus(rconn->conn) == CONNECTION_OK);

	if (diag_sqlstate)
		sqlstate = MAKE_SQLSTATE(diag_sqlstate[0], diag_sqlstate[1],
								 diag_sqlstate[2], diag_sqlstate[3],
								 diag_sqlstate[4]);
	else
		sqlstate = ERRCODE_CONNECTION_FAILURE;

	/*
	 * The message fields above point into res; copy the ones we still need so
	 * we can PQclear(res) now rather than leaking it when we longjmp out via
	 * ereport() below.
	 */
	if (message_primary)
		message_primary = pstrdup(message_primary);
	if (message_detail)
		message_detail = pstrdup(message_detail);
	if (message_hint)
		message_hint = pstrdup(message_hint);
	if (res)
		PQclear(res);

	if (message_primary == NULL && rconn->conn != NULL)
		message_primary = pchomp(PQerrorMessage(rconn->conn));

	if (conn_ok)
	{
		/*
		 * The socket is fine; this was a query-level error, not a
		 * connection failure.  Drain to the terminating NULL result so
		 * libpq's asyncStatus returns to IDLE (a FATAL_ERROR result does
		 * not do this by itself), and leave the connection usable.  Ignore
		 * whatever turns up here -- we're already erroring out, and this
		 * must not itself recurse back into RepFdwReportError.
		 */
		PGresult   *r;

		while ((r = libpqsrv_get_result(rconn->conn, we_stream)) != NULL)
			PQclear(r);
		rconn->state = REP_IN_TXN;
	}
	else
		rconn->state = REP_DEAD;

	if (sql)
		ctxmsg = psprintf("remote SQL command (replica %s:%d): %s",
						  rconn->host, rconn->port, sql);
	else
		ctxmsg = psprintf("replica %s:%d", rconn->host, rconn->port);

	ereport(ERROR,
			(errcode(sqlstate),
			 (message_primary != NULL && message_primary[0] != '\0') ?
			 errmsg_internal("%s", message_primary) :
			 errmsg("could not obtain message string for remote error"),
			 message_detail ? errdetail_internal("%s", message_detail) : 0,
			 message_hint ? errhint("%s", message_hint) : 0,
			 errcontext("%s", ctxmsg)));
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
		ListCell   *lc_over;

		if (rset->xact_open)
		{
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
				rconn->in_use = false;

				if (rconn->conn != NULL && PQstatus(rconn->conn) != CONNECTION_OK)
				{
					libpqsrv_disconnect(rconn->conn);
					rconn->conn = NULL;
					rconn->state = REP_DISCONNECTED;
				}
				else if (rconn->state != REP_DEAD)
					rconn->state = REP_IN_TXN;
			}

			/*
			 * Tear down any overflow connections (created by RepFdwCheckoutConn
			 * for concurrently live scans): close their remote xact and
			 * disconnect -- they are per-local-xact, not cached across
			 * statements.
			 */
			foreach(lc_over, rset->overflow_conns)
			{
				ReplicaConn *oc = (ReplicaConn *) lfirst(lc_over);
				ListCell   *lc2;

				if (oc->conn != NULL && oc->state != REP_DEAD)
				{
					PGresult   *res;

					if (event == XACT_EVENT_PRE_COMMIT)
					{
						res = libpqsrv_exec(oc->conn, "COMMIT", we_stream);
						if (res)
							PQclear(res);
					}
					else if (event == XACT_EVENT_ABORT)
					{
						if (oc->state == REP_STREAMING)
							(void) libpqsrv_cancel(oc->conn,
												   TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 5000));
						res = libpqsrv_exec(oc->conn, "ROLLBACK", we_stream);
						if (res)
							PQclear(res);
					}
				}

				foreach(lc2, oc->rowqueue)
					PQclear((PGresult *) lfirst(lc2));
				list_free(oc->rowqueue);
				if (oc->conn != NULL)
					libpqsrv_disconnect(oc->conn);
				if (oc->host != NULL)
					pfree(oc->host);
				pfree(oc);
			}
			list_free(rset->overflow_conns);
			rset->overflow_conns = NIL;

			rset->xact_open = false;
		}

		/*
		 * An ALTER SERVER/ALTER USER MAPPING landed while this entry was
		 * busy (rep_inval_callback couldn't safely rebuild it on the spot).
		 * It's idle now -- tear it down so the next RepFdwGetConnections
		 * rebuilds it with current options.
		 */
		if (rset->invalidated)
		{
			destroy_replicaset_conns(rset);
			hash_search(ReplicaSetHash, &rset->umid, HASH_REMOVE, NULL);
		}
	}
}

/*
 * rep_inval_callback
 *		Syscache invalidation callback for pg_foreign_server and
 *		pg_user_mapping: mark every cached ReplicaSet whose server or user
 *		mapping just changed so it gets rebuilt (immediately if idle, or at
 *		xact end if busy -- see RepFdwGetConnections/pgreplicafdw_xact_callback).
 *		Registered on both FOREIGNSERVEROID and USERMAPPINGOID; hashvalue==0
 *		means a full cache reset, in which case every entry is marked.
 */
static void
rep_inval_callback(Datum arg, SysCacheIdentifier cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	ReplicaSet *rset;

	if (ReplicaSetHash == NULL)
		return;

	Assert(cacheid == FOREIGNSERVEROID || cacheid == USERMAPPINGOID);

	hash_seq_init(&scan, ReplicaSetHash);
	while ((rset = (ReplicaSet *) hash_seq_search(&scan)) != NULL)
	{
		if (hashvalue == 0 ||
			(cacheid == FOREIGNSERVEROID &&
			 rset->server_hashvalue == hashvalue) ||
			(cacheid == USERMAPPINGOID &&
			 rset->mapping_hashvalue == hashvalue))
			rset->invalidated = true;
	}
}
