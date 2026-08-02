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

/*
 * How a scan of one foreign table is executed, chosen once during planning
 * (see repfdwGetForeignRelSize) from the query's shape and what the local
 * planner would do with the co-located copy.  The partition dimension and the
 * per-replica predicate are a single coherent choice per mode: the ctid bound
 * is emitted ONLY in CTID_SLICE -- a keyed mode that also carried a ctid bound
 * would silently drop rows.
 */
typedef enum RepFdwFanoutMode
{
	REPFDW_MODE_CTID_SLICE = 0,	/* fan out; each replica scans a heap block
								 * range of its slice (default) */
	REPFDW_MODE_SERVE_LOCAL,	/* no fan-out; answer from the co-located local
								 * table, whose planner picks an index scan */
	REPFDW_MODE_VALUE_SPLIT,	/* fan out; partition a large IN (=ANY) list of
								 * values across replicas, no ctid bound */
} RepFdwFanoutMode;

/* human-readable mode name for EXPLAIN */
extern const char *RepFdwModeName(RepFdwFanoutMode mode);

/* parsed FDW options, valid for the duration of planning of one scan */
typedef struct RepFdwOptions
{
	List	   *replicas;		/* list of RepHostPort *, order = replica index */
	char	   *dbname;			/* always this session's database (same-cluster
								 * assumption); not a user option */
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
	RepFdwFanoutMode mode;	/* execution mode chosen in GetForeignRelSize */
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
	bool		in_use;			/* checked out by a live scan node */
} ReplicaConn;

struct RepFdwScanState;

/* cached per user-mapping; keyed by user mapping OID in a process-local HTAB */
typedef struct ReplicaSet
{
	Oid			umid;			/* hash key, must be first */
	int			nconns;
	ReplicaConn *conns;			/* array[nconns]: one cached conn per replica,
								 * reused across statements */
	bool		xact_open;		/* remote REPEATABLE READ READ ONLY open on all */
	uint32		server_hashvalue;	/* GetSysCacheHashValue1(FOREIGNSERVEROID) */
	uint32		mapping_hashvalue;	/* GetSysCacheHashValue1(USERMAPPINGOID) */
	bool		invalidated;	/* an ALTER touched this server/mapping;
								 * conns need to be rebuilt when it's safe */
} ReplicaSet;

/*
 * Per-scan-node execution state.  A scan of a foreign table is an Append of one
 * ForeignScan per replica: each such node owns exactly one replica's connection
 * (rconn) and streams its ctid slice in chunked-rows mode, and an async-capable
 * Append drives all N children's sockets concurrently via the ForeignAsync*
 * callbacks (the same streaming state also feeds the synchronous
 * IterateForeignScan fallback).  A pushed-down count(*) instead uses a single
 * combine node (is_agg) that fans a partial query to every replica -- see the
 * agg_* fields below.
 *
 * fdw_private (plan -> exec) is the 8-tuple
 *   {sql_template, retrieved_attrs, remote_pred, is_count_agg, foreigntableid,
 *    my_index, nreplicas, mode}
 * where remote_pred is "" not NIL when there is no pushed predicate,
 * is_count_agg marks the combine node (my_index is -1 there, unused), and mode
 * is a RepFdwFanoutMode.  In VALUE_SPLIT remote_pred is this child's own
 * value-chunk predicate; in SERVE_LOCAL the node runs remote_pred against the
 * local table and there is no fan-out.
 */
typedef struct RepFdwScanState
{
	ReplicaSet *rset;
	RepFdwOptions *opts;
	Oid			foreigntableid;
	RepFdwFanoutMode mode;		/* execution mode (see RepFdwFanoutMode) */
	char	   *sql_template;	/* "SELECT ... FROM ...", no WHERE yet */
	List	   *retrieved_attrs;	/* ascending attnums fetched from replicas */
	char	   *remote_pred;	/* pushed-down WHERE predicate, no leading
								 * "WHERE" and no ctid bound; NULL if none */
	int			fetch_size;

	AttInMetadata *attinmeta;
	MemoryContext batch_cxt;	/* holds queued PGresult chunks */
	MemoryContext row_cxt;		/* reset per output row */

	/* per-replica scan state: this node owns one replica's slice */
	int			my_index;		/* this node's replica/slice index */
	int			nreplicas;		/* total replicas = Append child count */
	ReplicaConn *rconn;			/* this node's checked-out connection */
	bool		my_active;		/* false when my_index >= P (no slice) */
	bool		my_started;		/* streaming query has been sent */

	/*
	 * Aggregate-combine node (scanrelid==0): this single node fans a
	 * partial-aggregate query to every replica and combines the partials.
	 * Distinct from the per-replica scan path above.
	 */
	bool		is_agg;			/* this is the aggregate combine node */
	ReplicaConn **agg_conns;	/* array[agg_nconns] of checked-out conns */
	char	  **agg_sqls;		/* array[agg_nconns] per-replica partial SQL */
	RepFdwCtidBound *agg_bounds;	/* array[agg_nconns] ctid bind values */
	int			agg_nconns;		/* P: participating replicas */
	bool		agg_done;		/* the single combined row has been emitted */

	/*
	 * Serve-local node (mode == REPFDW_MODE_SERVE_LOCAL): the query is run once
	 * against the co-located local table via SPI and its rows materialized here
	 * (held in batch_cxt); no replica connection is used.
	 */
	HeapTuple  *local_tuples;	/* array[local_ntuples], in batch_cxt */
	int64		local_ntuples;
	int64		local_cur;		/* cursor into local_tuples */
} RepFdwScanState;

/* in option.c */
extern void RepFdwGetOptions(Oid foreigntableid, RepFdwOptions **opts);
extern List *RepFdwParseReplicas(const char *replicas_str);

/* in connection.c */
extern ReplicaSet *RepFdwGetConnections(UserMapping *user, RepFdwOptions *opts);
extern void RepFdwBeginRemoteXact(ReplicaSet *rset);
extern ReplicaConn *RepFdwCheckoutConn(ReplicaSet *rset, int index,
									   const char *servername);
extern void RepFdwReturnConn(ReplicaConn *rconn);
extern int	RepFdwQueuedRowCount(ReplicaConn *rconn);
extern void RepFdwStartOneQuery(ReplicaConn *rconn, const char *sql,
								const RepFdwCtidBound *bound, int fetch_size);
extern void RepFdwDrainConn(ReplicaConn *rconn, int fetch_size);
extern void RepFdwPumpOne(ReplicaConn *rconn, int fetch_size);
extern void RepFdwCancelDrainOne(ReplicaConn *rconn);
pg_noreturn extern void RepFdwReportError(PGresult *res, ReplicaConn *rconn,
										   const char *sql);

/* in slice.c */
extern BlockNumber RepFdwGetNBlocks(const char *schema, const char *table);
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

#endif							/* PG_REPLICA_FANOUT_FDW_H */
