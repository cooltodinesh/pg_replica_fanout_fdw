/*-------------------------------------------------------------------------
 *
 * pg_replica_fanout_fdw.h
 *		  Foreign-data wrapper that fans a sliced scan across N streaming
 *		  replicas and merges rows on the coordinator.  Read-only, no
 *		  remote qual/sort/aggregate pushdown.
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
	int			index;			/* replica index = slice index when active */
	char	   *host;
	int			port;
	RepConnState state;
	List	   *rowqueue;		/* queued PGresult chunks (PGRES_TUPLES_CHUNK) */
	int			cur_row;		/* cursor within linitial(rowqueue) */
	bool		paused;			/* backpressure: temporarily not polled */
} ReplicaConn;

struct RepFdwScanState;

/* cached per user-mapping; keyed by user mapping OID in a process-local HTAB */
typedef struct ReplicaSet
{
	Oid			umid;			/* hash key, must be first */
	int			nconns;
	ReplicaConn *conns;			/* array[nconns] */
	bool		xact_open;		/* remote REPEATABLE READ READ ONLY open on all */
	struct RepFdwScanState *active_scan;	/* non-NULL => a scan owns these
											 * conns; only one live scan per
											 * server is supported */
	uint32		server_hashvalue;	/* GetSysCacheHashValue1(FOREIGNSERVEROID) */
	uint32		mapping_hashvalue;	/* GetSysCacheHashValue1(USERMAPPINGOID) */
	bool		invalidated;	/* an ALTER touched this server/mapping;
								 * conns need to be rebuilt when it's safe */
} ReplicaSet;

/* fdw_private (plan -> exec): {sql_template, retrieved_attrs} */
typedef struct RepFdwScanState
{
	ReplicaSet *rset;
	RepFdwOptions *opts;
	Oid			foreigntableid;
	char	   *sql_template;	/* "SELECT ... FROM ...", no WHERE yet */
	List	   *retrieved_attrs;	/* ascending attnums fetched from replicas */
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
} RepFdwScanState;

/* in option.c */
extern void RepFdwGetOptions(Oid foreigntableid, RepFdwOptions **opts);
extern List *RepFdwParseReplicas(const char *replicas_str);

/* in connection.c */
extern ReplicaSet *RepFdwGetConnections(UserMapping *user, RepFdwOptions *opts);
extern void RepFdwBeginRemoteXact(ReplicaSet *rset);
extern void RepFdwStartQueries(ReplicaSet *rset, int nactive, char **sqls,
								RepFdwCtidBound *bounds, int fetch_size);
extern void RepStreamPump(struct RepFdwScanState *fsstate);
extern int	RepFdwQueuedRowCount(ReplicaConn *rconn);
extern void RepFdwCancelAndDrain(ReplicaSet *rset, int nactive);
extern PGresult *RepFdwExecSync(ReplicaConn *rconn, const char *sql);
pg_noreturn extern void RepFdwReportError(PGresult *res, ReplicaConn *rconn,
										   const char *sql);

/* in slice.c */
extern BlockNumber RepFdwGetNBlocks(ReplicaSet *rset, const char *schema,
									 const char *table);
extern int	RepFdwComputeSlices(BlockNumber nblocks, int nconns,
								 int min_blocks_per_slice,
								 RepFdwCtidBound **bounds_out);

/* in deparse.c */
extern char *RepFdwDeparseTemplate(Oid foreigntableid, List *retrieved_attrs,
									const char *schema, const char *table);
extern char *RepFdwDeparseCountTemplate(const char *schema, const char *table);
extern char *RepFdwBuildBoundedSql(const char *base_sql,
									const RepFdwCtidBound *bound);

/* in merge.c */
extern TupleTableSlot *RepFdwNextTuple(RepFdwScanState *fsstate,
										ForeignScanState *node);
extern TupleTableSlot *RepFdwNextCountTuple(RepFdwScanState *fsstate,
											 ForeignScanState *node);

#endif							/* PG_REPLICA_FANOUT_FDW_H */
