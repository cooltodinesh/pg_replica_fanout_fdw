/*-------------------------------------------------------------------------
 *
 * pg_replica_fanout_fdw.c
 *		  Handler and FDW callbacks (plan/exec glue) for pg_replica_fanout_fdw:
 *		  a sliced raw scan fanned out across N streaming replicas,
 *		  merged on the coordinator.  No remote qual pushdown, no ORDER
 *		  BY/aggregate pushdown, no writes.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "access/tupdesc.h"
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "pg_replica_fanout_fdw.h"

PG_MODULE_MAGIC_EXT(
					.name = "pg_replica_fanout_fdw",
					.version = PG_VERSION
);

PG_FUNCTION_INFO_V1(pg_replica_fanout_fdw_handler);

static void repfdwGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
									 Oid foreigntableid);
static void repfdwGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
								   Oid foreigntableid);
static ForeignScan *repfdwGetForeignPlan(PlannerInfo *root,
										  RelOptInfo *baserel,
										  Oid foreigntableid,
										  ForeignPath *best_path,
										  List *tlist, List *scan_clauses,
										  Plan *outer_plan);
static void repfdwBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *repfdwIterateForeignScan(ForeignScanState *node);
static void repfdwReScanForeignScan(ForeignScanState *node);
static void repfdwEndForeignScan(ForeignScanState *node);
static void repfdwExplainForeignScan(ForeignScanState *node, ExplainState *es);
static bool repfdwIsForeignScanParallelSafe(PlannerInfo *root,
											 RelOptInfo *rel,
											 RangeTblEntry *rte);

Datum
pg_replica_fanout_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	routine->GetForeignRelSize = repfdwGetForeignRelSize;
	routine->GetForeignPaths = repfdwGetForeignPaths;
	routine->GetForeignPlan = repfdwGetForeignPlan;
	routine->BeginForeignScan = repfdwBeginForeignScan;
	routine->IterateForeignScan = repfdwIterateForeignScan;
	routine->ReScanForeignScan = repfdwReScanForeignScan;
	routine->EndForeignScan = repfdwEndForeignScan;
	routine->ExplainForeignScan = repfdwExplainForeignScan;
	routine->IsForeignScanParallelSafe = repfdwIsForeignScanParallelSafe;

	PG_RETURN_POINTER(routine);
}

/*
 * repfdwGetForeignRelSize
 *		Parse options, estimate size from local stats, and record which
 *		columns will need to be fetched.  All baserestrictinfo stays local;
 *		remote qual pushdown is not supported.
 */
static void
repfdwGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
						 Oid foreigntableid)
{
	RepFdwPlanState *fpinfo = palloc0_object(RepFdwPlanState);
	ListCell   *lc;

	RepFdwGetOptions(foreigntableid, &fpinfo->opts);
	fpinfo->foreigntableid = foreigntableid;

	fpinfo->attrs_used = NULL;
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);
	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}

	baserel->fdw_private = fpinfo;

	/* Rely on ordinary pg_class stats, like a plain table would. */
	set_baserel_size_estimates(root, baserel);
}

/*
 * repfdwGetForeignPaths
 *		A single path: bill it at roughly (scan cost / N replicas), which is
 *		the whole point of fanning the scan out.
 */
static void
repfdwGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
					   Oid foreigntableid)
{
	RepFdwPlanState *fpinfo = (RepFdwPlanState *) baserel->fdw_private;
	int			nreplicas = Max(1, list_length(fpinfo->opts->replicas));
	Cost		startup_cost = 0;
	Cost		total_cost;

	total_cost = startup_cost
		+ (seq_page_cost * baserel->pages) / nreplicas
		+ cpu_tuple_cost * clamp_row_est(baserel->rows);

	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 NULL,	/* default pathtarget */
									 baserel->rows,
									 0,
									 startup_cost,
									 total_cost,
									 NIL,	/* no pathkeys */
									 baserel->lateral_relids,
									 NULL,	/* no outer plan */
									 NIL,	/* no fdw_restrictinfo */
									 NIL));	/* no fdw_private needed yet */
}

/*
 * repfdwGetForeignPlan
 *		Compute retrieved_attrs from the plan-time attrs_used bitmap and
 *		deparse the ctid-templated remote SELECT.  All scan_clauses are
 *		kept as local quals; remote qual pushdown is not supported.
 */
static ForeignScan *
repfdwGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
					  Oid foreigntableid, ForeignPath *best_path,
					  List *tlist, List *scan_clauses, Plan *outer_plan)
{
	RepFdwPlanState *fpinfo = (RepFdwPlanState *) baserel->fdw_private;
	Index		scan_relid = baserel->relid;
	List	   *retrieved_attrs = NIL;
	int			attno = -1;
	char	   *sql_template;
	List	   *fdw_private;

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	if (bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
					  fpinfo->attrs_used))
	{
		/*
		 * A whole-row reference (e.g. "t.*" or "t::text") was pulled in as
		 * attno 0 by pull_varattnos(), which does not expand it -- we have
		 * to fetch every live column ourselves.
		 */
		Relation	rel = table_open(foreigntableid, NoLock);
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			i;

		for (i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (!attr->attisdropped)
				retrieved_attrs = lappend_int(retrieved_attrs, attr->attnum);
		}
		table_close(rel, NoLock);
	}
	else
	{
		while ((attno = bms_next_member(fpinfo->attrs_used, attno)) >= 0)
		{
			int			realattno = attno + FirstLowInvalidHeapAttributeNumber;

			if (realattno > 0)
				retrieved_attrs = lappend_int(retrieved_attrs, realattno);
		}
	}

	sql_template = RepFdwDeparseTemplate(foreigntableid, retrieved_attrs,
										 fpinfo->opts->schema_name,
										 fpinfo->opts->table_name);

	fdw_private = list_make2(makeString(sql_template), retrieved_attrs);

	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no fdw_exprs */
							fdw_private,
							NIL,	/* no custom fdw_scan_tlist */
							NIL,	/* no remote quals */
							outer_plan);
}

/*
 * repfdwBeginForeignScan
 *		Get/connect the cached replica set, open the remote read-only
 *		transaction, discover the table's block count, compute slices, and
 *		kick off one streaming query per participating replica.
 */
static void
repfdwBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	Relation	rel = node->ss.ss_currentRelation;
	Oid			foreigntableid = RelationGetRelid(rel);
	List	   *fdw_private = fsplan->fdw_private;
	RepFdwScanState *fsstate;
	RepFdwOptions *opts;
	ForeignServer *server;
	UserMapping *user;
	RepFdwCtidBound *bounds;
	int			i;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	RepFdwGetOptions(foreigntableid, &opts);
	server = GetForeignServer(GetForeignTable(foreigntableid)->serverid);
	user = GetUserMapping(GetUserId(), server->serverid);

	fsstate = palloc0_object(RepFdwScanState);
	fsstate->opts = opts;
	fsstate->foreigntableid = foreigntableid;
	fsstate->sql_template = strVal(linitial(fdw_private));
	fsstate->retrieved_attrs = (List *) lsecond(fdw_private);
	fsstate->fetch_size = opts->fetch_size;

	fsstate->rset = RepFdwGetConnections(user, opts);
	RepFdwBeginRemoteXact(fsstate->rset);

	fsstate->nblocks = RepFdwGetNBlocks(fsstate->rset, opts->schema_name,
										opts->table_name);
	fsstate->nslices = RepFdwComputeSlices(fsstate->nblocks,
										   fsstate->rset->nconns,
										   opts->min_blocks_per_slice,
										   &bounds);
	fsstate->replica_bounds = bounds;

	fsstate->replica_sqls = (char **) palloc(sizeof(char *) * fsstate->nslices);
	for (i = 0; i < fsstate->nslices; i++)
		fsstate->replica_sqls[i] = RepFdwBuildBoundedSql(fsstate->sql_template,
														 &bounds[i]);

	RepFdwStartQueries(fsstate->rset, fsstate->nslices, fsstate->replica_sqls,
					   fsstate->replica_bounds, fsstate->fetch_size);

	fsstate->attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(rel));
	fsstate->rr_cursor = 0;
	fsstate->batch_cxt = AllocSetContextCreate(CurrentMemoryContext,
											   "pg_replica_fanout_fdw batch",
											   ALLOCSET_DEFAULT_SIZES);
	fsstate->row_cxt = AllocSetContextCreate(CurrentMemoryContext,
											 "pg_replica_fanout_fdw row",
											 ALLOCSET_SMALL_SIZES);

	node->fdw_state = fsstate;
}

static TupleTableSlot *
repfdwIterateForeignScan(ForeignScanState *node)
{
	RepFdwScanState *fsstate = (RepFdwScanState *) node->fdw_state;

	return RepFdwNextTuple(fsstate, node);
}

/*
 * repfdwReScanForeignScan
 *		Cancel/drain any in-flight query and resend the same per-replica SQL
 *		(the remote snapshot is unchanged, so this is exactly repeatable;
 *		nblocks/slicing does not need to be recomputed).
 */
static void
repfdwReScanForeignScan(ForeignScanState *node)
{
	RepFdwScanState *fsstate = (RepFdwScanState *) node->fdw_state;

	if (fsstate == NULL)
		return;

	RepFdwCancelAndDrain(fsstate->rset, fsstate->nslices);
	RepFdwStartQueries(fsstate->rset, fsstate->nslices, fsstate->replica_sqls,
					   fsstate->replica_bounds, fsstate->fetch_size);
	fsstate->rr_cursor = 0;
}

/*
 * repfdwEndForeignScan
 *		Drain/cancel outstanding results so the cached connections are left
 *		idle; the connections themselves and the remote transaction are
 *		left for the xact callback to close at local commit/abort.
 */
static void
repfdwEndForeignScan(ForeignScanState *node)
{
	RepFdwScanState *fsstate = (RepFdwScanState *) node->fdw_state;

	if (fsstate == NULL)
		return;

	RepFdwCancelAndDrain(fsstate->rset, fsstate->nslices);

	if (fsstate->batch_cxt)
		MemoryContextDelete(fsstate->batch_cxt);
	if (fsstate->row_cxt)
		MemoryContextDelete(fsstate->row_cxt);
}

/*
 * repfdwExplainForeignScan
 *		Minimal EXPLAIN output: replica count and the remote SQL template.
 *		Per-replica concrete ranges/row counts are not shown.
 */
static void
repfdwExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	char	   *sql_template = strVal(linitial(fsplan->fdw_private));
	RepFdwScanState *fsstate = (RepFdwScanState *) node->fdw_state;
	int			nreplicas;

	if (fsstate != NULL)
		nreplicas = fsstate->nslices;
	else
	{
		RepFdwOptions *opts;

		RepFdwGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &opts);
		nreplicas = list_length(opts->replicas);
	}

	ExplainPropertyInteger("Replicas", NULL, nreplicas, es);
	ExplainPropertyText("Remote SQL Template", sql_template, es);
}

static bool
repfdwIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
								RangeTblEntry *rte)
{
	return false;
}
