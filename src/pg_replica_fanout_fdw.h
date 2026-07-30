/*-------------------------------------------------------------------------
 *
 * pg_replica_fanout_fdw.h
 *		  Foreign-data wrapper that fans a sliced scan across N streaming
 *		  replicas and merges rows on the coordinator.  Read-only.  Pushes
 *		  down shippable (immutable) WHERE quals and unqualified count(*);
 *		  no remote sort/other-aggregate pushdown.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_REPLICA_FANOUT_FDW_H
#define PG_REPLICA_FANOUT_FDW_H

#include "foreign/foreign.h"
#include "funcapi.h"
#include "libpq/libpq-be-fe.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "storage/block.h"
#include "storage/waiteventset.h"

/* one entry of the parsed "replicas" server option */
typedef struct RepHostPort
{
	char	   *host;
	int			port;
} RepHostPort;

/* one slice's ctid bound, as bind-parameter values; NULL side = unbounded */
typedef struct RepFdwCtidBound
{
	char	   *lo;			/* tid literal "(block,0)", or NULL */
	char	   *hi;			/* tid literal "(block,0)", or NULL */
} RepFdwCtidBound;

/* parsed FDW options, valid for the duration of planning or of one scan */
typedef struct RepFdwOptions
{
	List	   *replicas;		/* list of RepHostPort *, order = replica index */
	char	   *dbname;			/* NULL => coordinator's current database */
	int			fetch_size;
	int			connect_timeout;	/* seconds */
	char	   *application_name;
	/* per-table */
	char	   *schema_name;
	char	   *table_name;
	int			min_blocks_per_slice;
} RepFdwOptions;

/* kept in baserel->fdw_private between GetForeignRelSize and GetForeignPlan */
typedef struct RepFdwPlanState
{
	RepFdwOptions *opts;
	Oid			foreigntableid;
	Bitmapset  *attrs_used;	/* columns needed, encoded like pull_varattnos() */
	bool		is_count_agg;	/* set by GetForeignUpperPaths on the upper
								 * (GROUP_AGG) rel's copy of this struct;
								 * read back by GetForeignPlan */
	List	   *remote_conds;	/* RestrictInfos safe to ship (see
								 * RepFdwIsForeignQual); on the upper rel,
								 * copied from the input baserel's fpinfo */
	List	   *local_conds;	/* RestrictInfos that must stay local */
} RepFdwPlanState;

typedef enum RepConnState
{
	REP_DISCONNECTED,
	REP_CONNECTING,
	REP_IN_TXN,					/* connected, remote txn open, idle */
	REP_STREAMING,				/* query sent, chunks may still be coming */
	REP_DONE,					/* current scan's query fully drained */
	REP_DEAD,
} RepConnState;

typedef struct ReplicaConn
{
	PGconn	   *conn;
	int			index;			/* replica index this conn talks to */
	char	   *host;
	int			port;
	RepConnState state;
	List	   *rowqueue;		/* queued PGresult chunks (PGRES_TUPLES_CHUNK) */
	int			cur_row;		/* cursor within linitial(rowqueue) */
	bool		paused;			/* backpressure: temporarily not polled */
	bool		in_use;			/* checked out by a live scan node (v2) */
} ReplicaConn;

struct RepFdwScanState;

/* cached per user-mapping; keyed by user mapping OID in a process-local HTAB */
typedef struct ReplicaSet
{
	Oid			umid;			/* hash key, must be first */
	int			nconns;
	ReplicaConn *conns;			/* array[nconns]: the cached "primary" conn per
								 * replica, reused across statements */
	List	   *overflow_conns; /* extra ReplicaConn * created when a replica's
								 * primary is already in use by a concurrently
								 * live scan (e.g. a self-join); torn down at
								 * local xact end (v2) */
	bool		xact_open;		/* remote REPEATABLE READ READ ONLY open on all */
	uint32		server_hashvalue;	/* GetSysCacheHashValue1(FOREIGNSERVEROID) */
	uint32		mapping_hashvalue;	/* GetSysCacheHashValue1(USERMAPPINGOID) */
	bool		invalidated;	/* an ALTER touched this server/mapping;
								 * conns need to be rebuilt when it's safe */
} ReplicaSet;

/*
 * fdw_private (plan -> exec), one positional layout for both the plain-scan
 * and count(*) pushdown plan shapes:
 *   {sql_template, retrieved_attrs, remote_pred, is_count_agg, foreigntableid}
 * remote_pred is an empty string, not NIL, when there is no pushed predicate
 * (a plain string node keeps the list a fixed-arity 5-tuple).
 */
typedef struct RepFdwScanState
{
	ReplicaSet *rset;
	RepFdwOptions *opts;
	Oid			foreigntableid;
	char	   *sql_template;	/* "SELECT ... FROM ...", no WHERE yet */
	List	   *retrieved_attrs;	/* ascending attnums fetched from replicas */
	char	   *remote_pred;	/* pushed-down WHERE predicate, no leading
								 * "WHERE" and no ctid bound; NULL if none */
	int			fetch_size;

	/* slicing, computed once in BeginForeignScan */
	BlockNumber nblocks;
	int			nslices;		/* P: number of replicas actually queried */
	char	  **replica_sqls;	/* array[nslices], final per-replica SQL
								 * (with $1/$2 ctid placeholders), kept
								 * around so ReScan can resend it unchanged */
	RepFdwCtidBound *replica_bounds;	/* array[nslices], bind values to go
										 * with replica_sqls */

	/* round-robin merge state */
	int			rr_cursor;

	AttInMetadata *attinmeta;
	MemoryContext batch_cxt;	/* holds queued PGresults */
	MemoryContext row_cxt;		/* reset per output row */

	bool		eof;

	/* persistent streaming WaitEventSet -- see RepStreamPump */
	WaitEventSet *stream_wes;
	bool		wes_dirty;		/* active-socket membership changed since
								 * stream_wes was last built */

	/*
	 * true for the pushed-down "SELECT count(*)" plan shape (scanrelid==0,
	 * no fan-out Agg node above this scan): each replica returns one
	 * partial int8 count and RepFdwNextCountTuple sums them into a single
	 * emitted row instead of merging raw rows.
	 */
	bool		is_count_agg;

	/*
	 * v2 (M0) per-replica scan state.  In the v2 architecture each replica's
	 * slice is a separate ForeignScan node under an Append (see
	 * notes/v2-append-architecture.md); this node owns exactly one replica's
	 * connection (rset->conns[my_index]) and streams it in chunked-rows mode.
	 * An async-capable Append (M0b) drives all N children's sockets
	 * concurrently via the ForeignAsync* callbacks; the same streaming state
	 * feeds the synchronous IterateForeignScan fallback.
	 */
	int			my_index;		/* this node's replica/slice index */
	int			nreplicas;		/* total replicas = Append child count */
	ReplicaConn *rconn;			/* this node's checked-out connection */
	bool		my_active;		/* false when my_index >= P (no slice) */
	bool		my_started;		/* streaming query has been sent */
} RepFdwScanState;

/* in option.c */
extern void RepFdwGetOptions(Oid foreigntableid, RepFdwOptions **opts);
extern List *RepFdwParseReplicas(const char *replicas_str);

/* in connection.c */
extern ReplicaSet *RepFdwGetConnections(UserMapping *user, RepFdwOptions *opts);
extern void RepFdwBeginRemoteXact(ReplicaSet *rset);
extern ReplicaConn *RepFdwCheckoutConn(ReplicaSet *rset, RepFdwOptions *opts,
									   UserMapping *user, int index);
extern void RepFdwReturnConn(ReplicaConn *rconn);
extern void RepFdwStartQueries(ReplicaSet *rset, int nactive, char **sqls,
								RepFdwCtidBound *bounds, int fetch_size);
extern void RepStreamPump(struct RepFdwScanState *fsstate);
extern int	RepFdwQueuedRowCount(ReplicaConn *rconn);
extern void RepFdwCancelAndDrain(ReplicaSet *rset, int nactive);
extern void RepFdwStartOneQuery(ReplicaConn *rconn, const char *sql,
								const RepFdwCtidBound *bound, int fetch_size);
extern void RepFdwDrainConn(ReplicaConn *rconn, int fetch_size);
extern void RepFdwPumpOne(ReplicaConn *rconn, int fetch_size);
extern void RepFdwCancelDrainOne(ReplicaConn *rconn);
extern PGresult *RepFdwExecSync(ReplicaConn *rconn, const char *sql);
extern PGresult *RepFdwExecBounded(ReplicaConn *rconn, const char *sql,
								   const RepFdwCtidBound *bound);
pg_noreturn extern void RepFdwReportError(PGresult *res, ReplicaConn *rconn,
										   const char *sql);

/* in slice.c */
extern BlockNumber RepFdwGetNBlocks(ReplicaConn *rconn, const char *schema,
									 const char *table);
extern int	RepFdwComputeSlices(BlockNumber nblocks, int nconns,
								 int min_blocks_per_slice,
								 RepFdwCtidBound **bounds_out);

/* in deparse.c */
extern char *RepFdwDeparseTemplate(Oid foreigntableid, List *retrieved_attrs,
									const char *schema, const char *table);
extern char *RepFdwDeparseCountTemplate(const char *schema, const char *table);
extern char *RepFdwBuildBoundedSql(const char *base_sql,
									const char *remote_pred,
									const RepFdwCtidBound *bound);
extern bool RepFdwIsForeignQual(PlannerInfo *root, RelOptInfo *baserel,
								 Expr *expr);
extern char *RepFdwDeparseQuals(Oid foreigntableid, List *remote_exprs);

/* in merge.c */
extern TupleTableSlot *RepFdwNextTuple(RepFdwScanState *fsstate,
										ForeignScanState *node);
extern TupleTableSlot *RepFdwNextCountTuple(RepFdwScanState *fsstate,
											 ForeignScanState *node);

#endif							/* PG_REPLICA_FANOUT_FDW_H */
