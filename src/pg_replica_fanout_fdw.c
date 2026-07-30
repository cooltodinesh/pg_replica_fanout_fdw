/*-------------------------------------------------------------------------
 *
 * pg_replica_fanout_fdw.c
 *		  Handler and FDW callbacks (plan/exec glue) for pg_replica_fanout_fdw:
 *		  a sliced raw scan fanned out across N streaming replicas,
 *		  merged on the coordinator.  Shippable (immutable) WHERE quals and
 *		  unqualified count(*) are pushed down; no ORDER BY/other aggregate
 *		  pushdown, no writes.
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
#include "optimizer/tlist.h"
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
static void repfdwGetForeignUpperPaths(PlannerInfo *root,
										UpperRelationKind stage,
										RelOptInfo *input_rel,
										RelOptInfo *output_rel,
										void *extra);

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
	routine->GetForeignUpperPaths = repfdwGetForeignUpperPaths;

	PG_RETURN_POINTER(routine);
}

/*
 * repfdwGetForeignRelSize
 *		Parse options, estimate size from local stats, and record which
 *		columns will need to be fetched.  Classify each baserestrictinfo
 *		entry as shippable (remote_conds) or not (local_conds) -- see
 *		RepFdwIsForeignQual for the shippability rule.
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

		if (!rinfo->pseudoconstant &&
			RepFdwIsForeignQual(root, baserel, rinfo->clause))
			fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
		else
		{
			fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
			pull_varattnos((Node *) rinfo->clause, baserel->relid,
						   &fpinfo->attrs_used);
		}
	}

	baserel->fdw_private = fpinfo;

	/* Rely on ordinary pg_class stats, like a plain table would. */
	set_baserel_size_estimates(root, baserel);
}

/*
 * repfdwGetForeignPaths
 *		A single path, costed as (page cost / N replicas) + per-row cost --
 *		intended to make the planner prefer this FDW over a plain seqscan
 *		roughly in proportion to the fan-out.  In practice baserel->pages is
 *		0 for any foreign table that has never been ANALYZEd (there's no
 *		AnalyzeForeignTable callback here to populate pg_class.relpages), so
 *		the page-cost term -- and hence this whole division by nreplicas --
 *		is usually a no-op; only the flat per-row term ends up mattering.
 *		This is a real limitation of the cost model, not just a comment
 *		nicety: don't rely on EXPLAIN cost numbers to judge the fan-out
 *		benefit. Fixing it (e.g. a crude remote pg_relation_size-based
 *		estimate) would need a planning-time round-trip and is out of scope
 *		here.
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
 * repfdwGetForeignUpperPaths
 *		Claim UPPERREL_GROUP_AGG for a single, narrow shape: count(*) over
 *		one of our foreign baserels, with an entirely shippable WHERE (or
 *		none) and no GROUP BY/HAVING/DISTINCT.  Each replica will compute
 *		its own partial count over its ctid slice (and the shippable
 *		predicate, if any) and the coordinator sums them -- see
 *		RepFdwNextCountTuple in merge.c -- so there is no Agg node above the
 *		Foreign Scan in the finished plan.
 *
 *		Anything not recognized here must fall back to a normal scan plus a
 *		local Agg: we bail out (add no path) rather than risk a wrong
 *		answer.
 */
static void
repfdwGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
						   RelOptInfo *input_rel, RelOptInfo *output_rel,
						   void *extra)
{
	RepFdwPlanState *ifpinfo;
	RepFdwPlanState *fpinfo;
	PathTarget *grouping_target;
	Aggref	   *aggref;
	Cost		startup_cost;
	Cost		total_cost;
	ForeignPath *path;

	/* 1. Only the plain full-aggregation upper stage; ignore the rest. */
	if (stage != UPPERREL_GROUP_AGG)
		return;

	/* 2. Don't add a second path if something already claimed this rel. */
	if (output_rel->fdw_private != NULL)
		return;

	/*
	 * 3. input_rel must be one of our own foreign baserels, and only one --
	 * no joins.  (GetForeignUpperPaths is only invoked at all when
	 * output_rel->fdwroutine, inherited from input_rel->fdwroutine, is
	 * ours, but that alone doesn't rule out a join of several of our
	 * tables.)
	 */
	if (input_rel->reloptkind != RELOPT_BASEREL ||
		input_rel->fdw_private == NULL ||
		bms_membership(input_rel->relids) != BMS_SINGLETON)
		return;

	ifpinfo = (RepFdwPlanState *) input_rel->fdw_private;

	/* 4. No GROUP BY / grouping sets. */
	if (root->parse->groupClause != NIL || root->parse->groupingSets != NIL)
		return;

	/* 5. No HAVING. */
	if (root->parse->havingQual != NULL)
		return;

	/*
	 * 6. Every qual on the base rel must be shippable.  A non-shippable
	 * (local-only) qual would have to run against the merged full-table
	 * scan before counting, which this plan shape has no way to do -- fall
	 * back to scan + local Agg in that case.  A shippable qual is fine: it
	 * gets deparsed into the per-slice count(*) below, same as a plain
	 * scan's remote predicate.
	 */
	if (ifpinfo->local_conds != NIL)
		return;

	/*
	 * 7. The grouping target must be exactly one expression, and it must
	 * be a bare count(*): aggstar, no DISTINCT/ORDER BY/FILTER, simple
	 * (non-partial) aggregation.  The core planner has already set
	 * output_rel->reltarget to the query's grouping target by the time it
	 * calls us (see create_grouping_paths/make_grouping_rel).
	 */
	grouping_target = output_rel->reltarget;
	if (list_length(grouping_target->exprs) != 1)
		return;

	if (!IsA(linitial(grouping_target->exprs), Aggref))
		return;

	aggref = castNode(Aggref, linitial(grouping_target->exprs));
	if (!aggref->aggstar ||
		aggref->aggdistinct != NIL ||
		aggref->aggorder != NIL ||
		aggref->aggfilter != NULL ||
		aggref->aggsplit != AGGSPLIT_SIMPLE)
		return;

	/* Eligible: stash the plan-time marker and add the foreign path. */
	fpinfo = palloc0_object(RepFdwPlanState);
	fpinfo->opts = ifpinfo->opts;
	fpinfo->foreigntableid = ifpinfo->foreigntableid;
	fpinfo->is_count_agg = true;
	fpinfo->remote_conds = ifpinfo->remote_conds;
	output_rel->fdw_private = fpinfo;

	/*
	 * Cost must reliably beat "scan + local Agg", but that fallback isn't
	 * necessarily expensive: a foreign table with no ANALYZE stats gets a
	 * default rows estimate of 1 (see repfdwGetForeignPaths), so the local
	 * fallback's own cost is already close to the minimum cost the planner
	 * can express (e.g. a 0.00..0.01 scan feeding a 0.01..0.02 Agg) -- a
	 * flat per-replica constant like cpu_tuple_cost * nreplicas can easily
	 * come out *higher* than that and lose the comparison outright. Since
	 * we can't estimate the real remote cost any better without a
	 * planning-time round trip (out of scope here, same as
	 * repfdwGetForeignPaths), just cost this at zero: we are certain it
	 * does less work than shipping every row back and aggregating locally,
	 * so always preferring it is correct, not just a tie-break.
	 */
	startup_cost = 0;
	total_cost = 0;

	path = create_foreign_upper_path(root, output_rel,
									 grouping_target,
									 1,		/* rows: exactly one output row */
									 0,		/* disabled_nodes */
									 startup_cost,
									 total_cost,
									 NIL,	/* no pathkeys */
									 NULL,	/* no fdw_outerpath */
									 NIL,	/* no fdw_restrictinfo */
									 NIL);	/* fdw_private carried on
											 * output_rel instead */

	add_path(output_rel, (Path *) path);
}

/*
 * repfdwGetForeignPlan
 *		Compute retrieved_attrs from the plan-time attrs_used bitmap,
 *		deparse the ctid-templated remote SELECT, and split scan_clauses
 *		into a shippable remote predicate and the quals that must stay
 *		local.  fdw_private is always the same 5-tuple -- see
 *		RepFdwScanState's comment in the header.
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
	char	   *remote_pred;
	List	   *fdw_private;

	/*
	 * The count(*) pushdown plan shape: an upper (GROUP_AGG) rel with
	 * relid==0, no columns to fetch, and a fixed remote template (plus any
	 * shippable WHERE qual carried over from the input baserel).
	 * fdw_scan_tlist carries the single emitted int8 count column, built
	 * from the same PathTarget (the query's count(*) Aggref) that the core
	 * planner already put in baserel->reltarget.  Because scan_relid==0,
	 * setrefs.c's set_foreignscan_references() treats fdw_scan_tlist as the
	 * scan's raw output tupdesc and rewrites the plan's own targetlist
	 * entries that structurally match it into plain Var(INDEX_VAR, resno)
	 * references -- so the Aggref never survives into the executed plan
	 * tree, and there's no Agg node to complain that it found one outside
	 * of an Agg.  is_count_agg/foreigntableid are also duplicated into
	 * fdw_private because scan_relid==0 means BeginForeignScan/
	 * ExplainForeignScan can't get them from ss_currentRelation, which will
	 * be NULL.
	 */
	if (fpinfo->is_count_agg)
	{
		char	   *count_sql = RepFdwDeparseCountTemplate(fpinfo->opts->schema_name,
														   fpinfo->opts->table_name);
		List	   *fdw_scan_tlist = make_tlist_from_pathtarget(baserel->reltarget);
		List	   *remote_exprs = NIL;
		ListCell   *lc;

		foreach(lc, fpinfo->remote_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			remote_exprs = lappend(remote_exprs, rinfo->clause);
		}
		remote_pred = RepFdwDeparseQuals(fpinfo->foreigntableid, remote_exprs);

		fdw_private = list_make5(makeString(count_sql),
								 NIL,
								 makeString(remote_pred ? remote_pred : ""),
								 makeBoolean(true),
								 makeInteger(fpinfo->foreigntableid));

		return make_foreignscan(tlist,
								NIL,	/* no local exprs */
								0,		/* scan_relid */
								NIL,	/* no fdw_exprs */
								fdw_private,
								fdw_scan_tlist,
								NIL,	/* no recheck quals */
								NULL);	/* no outer plan */
	}

	{
		List	   *remote_exprs = NIL;
		List	   *local_exprs = NIL;
		ListCell   *lc;

		foreach(lc, scan_clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			if (list_member_ptr(fpinfo->remote_conds, rinfo))
				remote_exprs = lappend(remote_exprs, rinfo);
			else
				local_exprs = lappend(local_exprs, rinfo);
		}

		remote_pred = RepFdwDeparseQuals(foreigntableid,
										 extract_actual_clauses(remote_exprs, false));
		scan_clauses = extract_actual_clauses(local_exprs, false);
	}

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

	fdw_private = list_make5(makeString(sql_template),
							 retrieved_attrs,
							 makeString(remote_pred ? remote_pred : ""),
							 makeBoolean(false),
							 makeInteger((int) foreigntableid));

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
	List	   *fdw_private = fsplan->fdw_private;
	bool		is_count_agg;
	Oid			foreigntableid;
	char	   *remote_pred;
	RepFdwScanState *fsstate;
	RepFdwOptions *opts;
	ForeignServer *server;
	UserMapping *user;
	RepFdwCtidBound *bounds;
	int			i;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * fdw_private is always {sql_template, retrieved_attrs, remote_pred,
	 * is_count_agg, foreigntableid} -- see the RepFdwScanState comment.
	 */
	remote_pred = strVal(lthird(fdw_private));
	if (remote_pred[0] == '\0')
		remote_pred = NULL;
	is_count_agg = boolVal(lfourth(fdw_private));
	foreigntableid = (Oid) intVal(list_nth(fdw_private, 4));

	RepFdwGetOptions(foreigntableid, &opts);
	server = GetForeignServer(GetForeignTable(foreigntableid)->serverid);
	user = GetUserMapping(GetUserId(), server->serverid);

	fsstate = palloc0_object(RepFdwScanState);
	fsstate->opts = opts;
	fsstate->foreigntableid = foreigntableid;
	fsstate->sql_template = strVal(linitial(fdw_private));
	fsstate->retrieved_attrs = (List *) lsecond(fdw_private);
	fsstate->remote_pred = remote_pred;
	fsstate->fetch_size = opts->fetch_size;
	fsstate->is_count_agg = is_count_agg;

	fsstate->rset = RepFdwGetConnections(user, opts);

	if (fsstate->rset->active_scan != NULL &&
		fsstate->rset->active_scan != fsstate)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_replica_fanout_fdw cannot run two concurrent scans on foreign server \"%s\"",
						server->servername),
				 errdetail("Each replica connection streams one query at a time, so only one foreign scan per server can be live at once."),
				 errhint("Rewrite so a single scan of this server is active at a time (e.g. materialize one side with a CTE, or place the tables on separate servers).")));
	fsstate->rset->active_scan = fsstate;

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
														 fsstate->remote_pred,
														 &bounds[i]);

	RepFdwStartQueries(fsstate->rset, fsstate->nslices, fsstate->replica_sqls,
					   fsstate->replica_bounds, fsstate->fetch_size);

	/*
	 * The count(*) merge path (RepFdwNextCountTuple) parses one int8 cell
	 * directly and never needs attinmeta; only the plain-scan merge path
	 * (RepFdwNextTuple) does.
	 */
	fsstate->attinmeta = is_count_agg ? NULL :
		TupleDescGetAttInMetadata(RelationGetDescr(node->ss.ss_currentRelation));
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

	if (fsstate->is_count_agg)
		return RepFdwNextCountTuple(fsstate, node);

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

	/* active-socket membership resets when the queries are resent */
	if (fsstate->stream_wes != NULL)
	{
		FreeWaitEventSet(fsstate->stream_wes);
		fsstate->stream_wes = NULL;
	}
	fsstate->wes_dirty = false;

	RepFdwStartQueries(fsstate->rset, fsstate->nslices, fsstate->replica_sqls,
					   fsstate->replica_bounds, fsstate->fetch_size);
	fsstate->rr_cursor = 0;

	/*
	 * The count(*) combine path emits exactly one row and then latches eof;
	 * a rescan (e.g. this scan on the inner side of a nestloop) must clear it
	 * so the resent queries produce the count again.  The plain-scan path does
	 * not read eof, so this is harmless there.
	 */
	fsstate->eof = false;
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

	if (fsstate->rset != NULL && fsstate->rset->active_scan == fsstate)
		fsstate->rset->active_scan = NULL;

	if (fsstate->stream_wes != NULL)
		FreeWaitEventSet(fsstate->stream_wes);

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
	List	   *fdw_private = fsplan->fdw_private;
	char	   *sql_template = strVal(linitial(fdw_private));
	char	   *remote_pred = strVal(lthird(fdw_private));
	Oid			foreigntableid = (Oid) intVal(list_nth(fdw_private, 4));
	RepFdwScanState *fsstate = (RepFdwScanState *) node->fdw_state;
	int			nreplicas;

	if (fsstate != NULL)
		nreplicas = fsstate->nslices;
	else
	{
		RepFdwOptions *opts;

		/* EXPLAIN-only (no execution): no per-slice count to report yet. */
		RepFdwGetOptions(foreigntableid, &opts);
		nreplicas = list_length(opts->replicas);
	}

	ExplainPropertyInteger("Replicas", NULL, nreplicas, es);

	/*
	 * Fold the pushed predicate into the same "Remote SQL Template" line
	 * (rather than a new label), mirroring postgres_fdw's single "Remote
	 * SQL" line -- existing users don't need new vocabulary.  The internal
	 * ctid $1/$2 slice bound is omitted here, same as before.
	 */
	if (remote_pred[0] != '\0')
		ExplainPropertyText("Remote SQL Template",
							psprintf("%s WHERE %s", sql_template, remote_pred),
							es);
	else
		ExplainPropertyText("Remote SQL Template", sql_template, es);
}

static bool
repfdwIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
								RangeTblEntry *rte)
{
	return false;
}
