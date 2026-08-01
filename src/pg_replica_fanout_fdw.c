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
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#include "executor/execAsync.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "utils/builtins.h"
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
static bool repfdwIsForeignPathAsyncCapable(ForeignPath *path);
static void repfdwForeignAsyncRequest(AsyncRequest *areq);
static void repfdwForeignAsyncConfigureWait(AsyncRequest *areq);
static void repfdwForeignAsyncNotify(AsyncRequest *areq);

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

	/* v2 (M0b): async execution -- the Append drives all replicas at once. */
	routine->IsForeignPathAsyncCapable = repfdwIsForeignPathAsyncCapable;
	routine->ForeignAsyncRequest = repfdwForeignAsyncRequest;
	routine->ForeignAsyncConfigureWait = repfdwForeignAsyncConfigureWait;
	routine->ForeignAsyncNotify = repfdwForeignAsyncNotify;

	PG_RETURN_POINTER(routine);
}

/*
 * repfdwGetForeignRelSize
 *		Parse options, estimate size from local stats, and record which
 *		columns will need to be fetched. Classify each baserestrictinfo
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

	/*
	 * Size estimate.  A foreign table's own pg_class stats are never populated
	 * (ANALYZE is a no-op without AnalyzeForeignTable), so on their own they
	 * give only default estimates.  But under the same-cluster assumption the
	 * remote table is a byte-identical physical replica of a table present
	 * locally in this database -- so we borrow that local copy's size via
	 * estimate_rel_size (the same logic core uses for a plain table: current
	 * block count plus reltuples).  This makes baserel->rows accurate, which
	 * mainly helps the planner pick good join methods/order and aggregation
	 * strategies for queries that combine this foreign table with other data.
	 * If the local table isn't found (or isn't a plain table), fall back to the
	 * default estimate.
	 */
	{
		Oid			nspoid = get_namespace_oid(fpinfo->opts->schema_name, true);
		Oid			localrelid = OidIsValid(nspoid) ?
			get_relname_relid(fpinfo->opts->table_name, nspoid) : InvalidOid;
		Relation	localrel = OidIsValid(localrelid) ?
			try_table_open(localrelid, AccessShareLock) : NULL;

		if (localrel != NULL)
		{
			if (localrel->rd_rel->relkind == RELKIND_RELATION)
			{
				BlockNumber pages;
				double		tuples;
				double		allvisfrac;

				estimate_rel_size(localrel, NULL, &pages, &tuples, &allvisfrac);
				baserel->pages = pages;
				baserel->tuples = tuples;
				baserel->allvisfrac = allvisfrac;
			}
			table_close(localrel, AccessShareLock);
		}
	}

	set_baserel_size_estimates(root, baserel);
}

/*
 * repfdwGetForeignPaths (v2 -- M0)
 *		Instead of one fan-out ForeignScan, emit an Append of N per-replica
 *		ForeignScan paths -- one child per replica, each carrying its replica
 *		index in the path's fdw_private.  The core planner then stacks native
 *		Agg/Sort/Join nodes on top of the Append, and (M0b) an async-capable
 *		Append drives all N children concurrently.  create_append_path
 *		explicitly supports a RELOPT_BASEREL parent, and async is gated only on
 *		the FDW routine + per-path capability, never on real partitioning --
 *		see the M0 findings in notes/v2-append-architecture.md.
 *
 *		Cost note (unchanged from v1): baserel->pages is 0 without ANALYZE, so
 *		the per-replica page-cost division is usually a no-op; don't read the
 *		fan-out benefit off EXPLAIN cost numbers.
 */
static void
repfdwGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
					   Oid foreigntableid)
{
	RepFdwPlanState *fpinfo = (RepFdwPlanState *) baserel->fdw_private;
	int			nreplicas = Max(1, list_length(fpinfo->opts->replicas));
	double		per_child_rows = clamp_row_est(baserel->rows / nreplicas);
	List	   *subpaths = NIL;
	AppendPathInput input;
	int			i;

	for (i = 0; i < nreplicas; i++)
	{
		Cost		startup_cost = 0;
		Cost		total_cost = startup_cost
			+ (seq_page_cost * baserel->pages) / nreplicas
			+ cpu_tuple_cost * per_child_rows;
		ForeignPath *child;

		child = create_foreignscan_path(root, baserel,
										NULL,	/* default pathtarget */
										per_child_rows,
										0,		/* disabled_nodes */
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										baserel->lateral_relids,
										NULL,	/* no outer plan */
										NIL,	/* no fdw_restrictinfo */
										list_make1(makeInteger(i)));
		subpaths = lappend(subpaths, child);
	}

	MemSet(&input, 0, sizeof(input));
	input.subpaths = subpaths;

	add_path(baserel, (Path *)
			 create_append_path(root, baserel, input,
								 NIL,	/* no pathkeys */
								 baserel->lateral_relids,
								 0,		/* parallel_workers */
								 false, /* parallel_aware */
								 clamp_row_est(baserel->rows)));
}

/*
 * repfdwGetForeignUpperPaths
 *		Disabled in v2 (returns immediately): aggregation is handled by native
 *		Agg nodes over the Append of per-replica scans, not by an internal
 *		combine (see repfdwGetForeignPaths and notes/v2-append-architecture.md).
 *		The v1 count(*) upper-path pushdown below is retained, unreached, as the
 *		starting point for M3 remote partial-aggregate pushdown (a per-child
 *		Partial Aggregate + a local Finalize Aggregate).
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

	/*
	 * M3: remote aggregate pushdown for the decomposable class.  Because the
	 * FDW upper-path API yields a single ForeignPath for the *final* grouped
	 * result (and our FDW-built Append over one baserel never triggers core's
	 * partitionwise/AGGSPLIT Finalize machinery), a pushed aggregate is a
	 * scanrelid==0 foreign node that fans a partial-aggregate query to every
	 * replica and combines the partials itself -- see repfdwBeginForeignScan's
	 * agg branch and notes/v2-append-architecture.md.  Currently only a bare
	 * count(*) is recognized; sum/min/max/avg and GROUP BY are follow-ons.
	 */

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

		/*
		 * Same 7-tuple layout as a plain scan child, with is_count_agg=true
		 * marking the aggregate combine node.  my_index is -1 (the agg node
		 * fans to all replicas, not one), nreplicas drives the fan-out.
		 */
		fdw_private = list_make5(makeString(count_sql),
								 NIL,
								 makeString(remote_pred ? remote_pred : ""),
								 makeBoolean(true),
								 makeInteger(fpinfo->foreigntableid));
		fdw_private = lappend(fdw_private, makeInteger(-1));
		fdw_private = lappend(fdw_private,
							  makeInteger(list_length(fpinfo->opts->replicas)));

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

	/*
	 * v2 (M0): this is one Append child.  Its replica/slice index rides in the
	 * child ForeignPath's fdw_private (set in repfdwGetForeignPaths); append it
	 * plus the total replica count to the plan-time fdw_private so
	 * BeginForeignScan knows which replica it owns and how to slice.
	 */
	fdw_private = list_make5(makeString(sql_template),
							 retrieved_attrs,
							 makeString(remote_pred ? remote_pred : ""),
							 makeBoolean(false),
							 makeInteger((int) foreigntableid));
	fdw_private = lappend(fdw_private,
						  makeInteger(intVal(linitial(best_path->fdw_private))));
	fdw_private = lappend(fdw_private,
						  makeInteger(list_length(fpinfo->opts->replicas)));

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
 * repfdw_run_slice_query
 *		Kick off this Append child's slice query on its own replica connection
 *		in chunked-rows streaming mode (non-blocking send).  Discovers nblocks
 *		from this node's replica, computes the P disjoint slices, and -- if
 *		this node's index is within P -- sends "SELECT ... WHERE <ctid slice>".
 *		A node whose index is >= P (fewer blocks than replicas) is idle and
 *		returns no rows.  The rows are drained later, concurrently across all
 *		children, by the async Append (or by RepFdwPumpOne in the sync path).
 */
static void
repfdw_run_slice_query(RepFdwScanState *fsstate)
{
	ReplicaConn *rconn = fsstate->rconn;
	RepFdwCtidBound *bounds;
	BlockNumber nblocks;
	int			P;
	char	   *sql;

	nblocks = RepFdwGetNBlocks(rconn, fsstate->opts->schema_name,
							   fsstate->opts->table_name);
	P = RepFdwComputeSlices(nblocks, fsstate->nreplicas,
							fsstate->opts->min_blocks_per_slice, &bounds);

	if (fsstate->my_index >= P)
	{
		fsstate->my_active = false;
		fsstate->my_started = false;
		return;
	}

	sql = RepFdwBuildBoundedSql(fsstate->sql_template, fsstate->remote_pred,
							   &bounds[fsstate->my_index]);
	RepFdwStartOneQuery(rconn, sql, &bounds[fsstate->my_index],
						fsstate->fetch_size);
	fsstate->my_active = true;
	fsstate->my_started = true;
}

/*
 * repfdw_agg_start
 *		Set up the aggregate combine node (scanrelid==0): discover nblocks,
 *		compute the P slices, check out one connection per participating
 *		replica, and *send* each replica's partial-aggregate query now.  Sending
 *		all P up front makes the replicas run concurrently; the partials are
 *		gathered later in repfdw_agg_combine (so total time ~= the slowest
 *		replica, not the sum).
 */
static void
repfdw_agg_send(RepFdwScanState *fsstate)
{
	int			i;

	for (i = 0; i < fsstate->agg_nconns; i++)
	{
		/* Idempotent for a fresh conn; drains a prior run on ReScan. */
		RepFdwCancelDrainOne(fsstate->agg_conns[i]);
		RepFdwStartOneQuery(fsstate->agg_conns[i], fsstate->agg_sqls[i],
							&fsstate->agg_bounds[i], fsstate->fetch_size);
	}
	fsstate->agg_done = false;
}

static void
repfdw_agg_start(RepFdwScanState *fsstate, const char *servername)
{
	RepFdwOptions *opts = fsstate->opts;
	ReplicaConn *c0;
	BlockNumber nblocks;
	int			P;
	int			i;

	/* Replica 0's connection tells us nblocks; it is also slice 0. */
	c0 = RepFdwCheckoutConn(fsstate->rset, 0, servername);
	nblocks = RepFdwGetNBlocks(c0, opts->schema_name, opts->table_name);
	P = RepFdwComputeSlices(nblocks, fsstate->nreplicas,
							opts->min_blocks_per_slice, &fsstate->agg_bounds);

	fsstate->agg_nconns = P;
	fsstate->agg_conns = (ReplicaConn **) palloc(sizeof(ReplicaConn *) * P);
	fsstate->agg_sqls = (char **) palloc(sizeof(char *) * P);

	for (i = 0; i < P; i++)
	{
		fsstate->agg_conns[i] = (i == 0) ? c0 :
			RepFdwCheckoutConn(fsstate->rset, i, servername);
		fsstate->agg_sqls[i] = RepFdwBuildBoundedSql(fsstate->sql_template,
													 fsstate->remote_pred,
													 &fsstate->agg_bounds[i]);
	}

	repfdw_agg_send(fsstate);
}

/*
 * repfdw_agg_combine
 *		Gather the one partial row from each replica and combine into the single
 *		output row.  For count(*) the partial is an int8 per slice and the
 *		combine is a sum.  (The decomposable class -- sum/min/max/avg -- will
 *		generalize this with a per-column combine spec.)
 */
static void
repfdw_agg_combine(RepFdwScanState *fsstate, TupleTableSlot *slot)
{
	int64		total = 0;
	int			i;

	for (i = 0; i < fsstate->agg_nconns; i++)
	{
		ReplicaConn *ci = fsstate->agg_conns[i];
		PGresult   *res;

		/* Finish draining this replica's partial (it has been running since Begin). */
		while (ci->state == REP_STREAMING)
		{
			MemoryContext oldcxt = MemoryContextSwitchTo(fsstate->batch_cxt);

			RepFdwPumpOne(ci, fsstate->fetch_size);
			MemoryContextSwitchTo(oldcxt);
		}

		if (ci->rowqueue == NIL)
			elog(ERROR,
				 "pg_replica_fanout_fdw: replica \"%s:%d\" returned no partial aggregate",
				 ci->host, ci->port);

		res = (PGresult *) linitial(ci->rowqueue);
		if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
			elog(ERROR,
				 "pg_replica_fanout_fdw: unexpected partial aggregate from replica \"%s:%d\"",
				 ci->host, ci->port);

		total += pg_strtoint64(PQgetvalue(res, 0, 0));

		PQclear(res);
		ci->rowqueue = list_delete_first(ci->rowqueue);
		ci->cur_row = 0;
	}

	ExecClearTuple(slot);
	slot->tts_values[0] = Int64GetDatum(total);
	slot->tts_isnull[0] = false;
	ExecStoreVirtualTuple(slot);
}

/*
 * repfdw_store_next_row
 *		If a row is buffered for this child, materialize it into slot and
 *		return true; otherwise return false (caller must drain more input or
 *		conclude EOF).  Shared by the sync IterateForeignScan and the async
 *		callbacks.
 */
static bool
repfdw_store_next_row(RepFdwScanState *fsstate, TupleTableSlot *slot)
{
	ReplicaConn *rconn = fsstate->rconn;
	PGresult   *res;
	HeapTuple	tuple;
	char	  **values;
	int			natts;
	int			j;
	ListCell   *lc;
	MemoryContext oldcxt;

	if (rconn->rowqueue == NIL)
		return false;

	res = (PGresult *) linitial(rconn->rowqueue);
	natts = fsstate->attinmeta->tupdesc->natts;

	MemoryContextReset(fsstate->row_cxt);
	oldcxt = MemoryContextSwitchTo(fsstate->row_cxt);

	values = (char **) palloc0(sizeof(char *) * natts);
	j = 0;
	foreach(lc, fsstate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc);

		if (!PQgetisnull(res, rconn->cur_row, j))
			values[attnum - 1] = PQgetvalue(res, rconn->cur_row, j);
		j++;
	}
	tuple = BuildTupleFromCStrings(fsstate->attinmeta, values);

	MemoryContextSwitchTo(oldcxt);

	rconn->cur_row++;
	if (rconn->cur_row >= PQntuples(res))
	{
		rconn->rowqueue = list_delete_first(rconn->rowqueue);
		PQclear(res);
		rconn->cur_row = 0;

		if (rconn->paused &&
			RepFdwQueuedRowCount(rconn) <= fsstate->fetch_size)
			rconn->paused = false;
	}

	ExecStoreHeapTuple(tuple, slot, false);
	return true;
}

/*
 * repfdwBeginForeignScan (v2 -- M0)
 *		One Append child = one replica.  Connect the cached replica set, open
 *		the shared remote read-only transaction (idempotent across siblings),
 *		and run this node's slice query synchronously.  Async streaming and
 *		per-node connection ownership are later milestones; for now sibling
 *		children share the cached ReplicaSet, each driving conns[my_index].
 */
static void
repfdwBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	List	   *fdw_private = fsplan->fdw_private;
	Oid			foreigntableid;
	char	   *remote_pred;
	bool		is_count_agg;
	RepFdwScanState *fsstate;
	RepFdwOptions *opts;
	ForeignServer *server;
	UserMapping *user;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * fdw_private 7-tuple:
	 *   {sql_template, retrieved_attrs, remote_pred, is_count_agg,
	 *    foreigntableid, my_index, nreplicas}
	 * is_count_agg=true is the aggregate combine node (scanrelid==0, fans to
	 * all replicas); false is a per-replica Append child (owns my_index).
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
	fsstate->is_agg = is_count_agg;
	fsstate->my_index = intVal(list_nth(fdw_private, 5));
	fsstate->nreplicas = intVal(list_nth(fdw_private, 6));

	fsstate->rset = RepFdwGetConnections(user, opts);
	RepFdwBeginRemoteXact(fsstate->rset);

	fsstate->batch_cxt = AllocSetContextCreate(CurrentMemoryContext,
											   "pg_replica_fanout_fdw batch",
											   ALLOCSET_DEFAULT_SIZES);

	if (fsstate->is_agg)
	{
		/*
		 * Aggregate combine node: no scan relation (scanrelid==0, so
		 * ss_currentRelation is NULL and there is no attinmeta), fan the
		 * partial query to every replica now.
		 */
		node->fdw_state = fsstate;
		repfdw_agg_start(fsstate, server->servername);
		return;
	}

	/*
	 * Per-replica Append child.  Claim its replica's connection; a second
	 * concurrent scan of the same server (e.g. a self-join) collides here and
	 * gets a clean error (see RepFdwCheckoutConn).
	 */
	fsstate->rconn = RepFdwCheckoutConn(fsstate->rset, fsstate->my_index,
										server->servername);
	fsstate->attinmeta =
		TupleDescGetAttInMetadata(RelationGetDescr(node->ss.ss_currentRelation));
	fsstate->row_cxt = AllocSetContextCreate(CurrentMemoryContext,
											 "pg_replica_fanout_fdw row",
											 ALLOCSET_SMALL_SIZES);

	node->fdw_state = fsstate;

	repfdw_run_slice_query(fsstate);
}

/*
 * repfdwIterateForeignScan (v2 -- M0b, synchronous fallback)
 *		Return the next row from this child's stream, blocking on its socket as
 *		needed (RepFdwPumpOne).  Used when the scan runs outside an async Append
 *		(e.g. EvalPlanQual); under an async Append the ForeignAsync* callbacks
 *		drive tuple production instead.
 */
static TupleTableSlot *
repfdwIterateForeignScan(ForeignScanState *node)
{
	RepFdwScanState *fsstate = (RepFdwScanState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ReplicaConn *rconn;

	/* Aggregate combine node: gather all replicas' partials, emit one row. */
	if (fsstate->is_agg)
	{
		if (fsstate->agg_done)
		{
			ExecClearTuple(slot);
			return slot;
		}
		repfdw_agg_combine(fsstate, slot);
		fsstate->agg_done = true;
		return slot;
	}

	if (!fsstate->my_active)
	{
		ExecClearTuple(slot);
		return slot;
	}

	rconn = fsstate->rconn;

	for (;;)
	{
		if (repfdw_store_next_row(fsstate, slot))
			return slot;

		if (rconn->state != REP_STREAMING)
		{
			/* stream finished and nothing left buffered: EOF */
			ExecClearTuple(slot);
			return slot;
		}

		/*
		 * Drain into batch_cxt: the queued PGresult list cells must outlive
		 * ExecScan's per-tuple context, which is reset between tuples.
		 */
		{
			MemoryContext oldcxt = MemoryContextSwitchTo(fsstate->batch_cxt);

			RepFdwPumpOne(rconn, fsstate->fetch_size);
			MemoryContextSwitchTo(oldcxt);
		}
	}
}

/*
 * repfdwReScanForeignScan (v2 -- M0b)
 *		Cancel/drain any in-flight stream and resend this child's slice query
 *		(the remote snapshot is unchanged, so this is exactly repeatable).
 */
static void
repfdwReScanForeignScan(ForeignScanState *node)
{
	RepFdwScanState *fsstate = (RepFdwScanState *) node->fdw_state;

	if (fsstate == NULL)
		return;

	if (fsstate->is_agg)
	{
		/* Resend the partials on the already-checked-out connections. */
		repfdw_agg_send(fsstate);
		return;
	}

	if (fsstate->my_started)
		RepFdwCancelDrainOne(fsstate->rconn);

	repfdw_run_slice_query(fsstate);
}

/*
 * repfdwEndForeignScan (v2 -- M0b)
 *		Cancel/drain this child's in-flight stream so its connection returns to
 *		idle-in-transaction.  The connection and remote transaction are left for
 *		the xact callback to close at local commit/abort.
 */
static void
repfdwEndForeignScan(ForeignScanState *node)
{
	RepFdwScanState *fsstate = (RepFdwScanState *) node->fdw_state;

	if (fsstate == NULL)
		return;

	/* Release checked-out connection(s), draining any in-flight query. */
	if (fsstate->is_agg)
	{
		int			i;

		for (i = 0; i < fsstate->agg_nconns; i++)
			RepFdwReturnConn(fsstate->agg_conns[i]);
	}
	else if (fsstate->rconn != NULL)
		RepFdwReturnConn(fsstate->rconn);

	if (fsstate->batch_cxt)
		MemoryContextDelete(fsstate->batch_cxt);
	if (fsstate->row_cxt)
		MemoryContextDelete(fsstate->row_cxt);
}

/*
 * repfdwIsForeignPathAsyncCapable
 *		Every per-replica child (a baserel scan) is async-capable: that is the
 *		whole point of the v2 Append (all replicas stream at once).  The
 *		aggregate upper path (scanrelid==0, RELOPT_UPPER_REL) fans out and
 *		combines synchronously, so it is not async.
 */
static bool
repfdwIsForeignPathAsyncCapable(ForeignPath *path)
{
	return ((Path *) path)->parent->reloptkind == RELOPT_BASEREL;
}

/*
 * repfdw_async_produce
 *		Core of the async callbacks.  If a row is buffered for this child, run
 *		the node's own ExecProcNode so ExecScan applies the ForeignScan's quals
 *		and projection (returning a properly formed result slot) -- exactly as
 *		postgres_fdw does; handing back a raw scan slot skips projection and
 *		corrupts the tuple the parent reads.  Otherwise signal EOF if the
 *		stream is finished, or mark the request pending so the Append waits on
 *		our socket.  Unlike postgres_fdw we never multiplex a connection across
 *		requests -- each child owns conns[my_index] -- so there is no
 *		pending-request juggling.
 */
static void
repfdw_async_produce(AsyncRequest *areq)
{
	ForeignScanState *node = (ForeignScanState *) areq->requestee;
	RepFdwScanState *fsstate = (RepFdwScanState *) node->fdw_state;
	ReplicaConn *rconn;

	if (!fsstate->my_active)
	{
		ExecAsyncRequestDone(areq, NULL);
		return;
	}

	rconn = fsstate->rconn;

	/*
	 * A row is buffered: let the node's normal ExecProcNode path pull it
	 * (IterateForeignScan returns it without blocking, since it is already
	 * queued) and project it.
	 */
	if (rconn->rowqueue != NIL)
	{
		TupleTableSlot *result = node->ss.ps.ExecProcNodeReal((PlanState *) node);

		if (!TupIsNull(result))
		{
			ExecAsyncRequestDone(areq, result);
			return;
		}
	}

	if (rconn->state != REP_STREAMING)
	{
		/* stream finished, nothing buffered: EOF */
		ExecAsyncRequestDone(areq, NULL);
		return;
	}

	/* No row yet; ask the Append to wait for our socket. */
	ExecAsyncRequestPending(areq);
}

/*
 * repfdwForeignAsyncRequest
 *		The Append wants a tuple from this child.
 */
static void
repfdwForeignAsyncRequest(AsyncRequest *areq)
{
	repfdw_async_produce(areq);
}

/*
 * repfdwForeignAsyncConfigureWait
 *		Register this child's socket in the Append's WaitEventSet so it is woken
 *		when data arrives.
 */
static void
repfdwForeignAsyncConfigureWait(AsyncRequest *areq)
{
	ForeignScanState *node = (ForeignScanState *) areq->requestee;
	RepFdwScanState *fsstate = (RepFdwScanState *) node->fdw_state;
	AppendState *requestor = (AppendState *) areq->requestor;
	WaitEventSet *set = requestor->as_eventset;
	ReplicaConn *rconn = fsstate->rconn;

	Assert(areq->callback_pending);

	AddWaitEventToSet(set, WL_SOCKET_READABLE, PQsocket(rconn->conn),
					  NULL, areq);
}

/*
 * repfdwForeignAsyncNotify
 *		Our socket signalled readable: drain available input and try to produce
 *		a tuple (or conclude EOF, or go pending again).
 */
static void
repfdwForeignAsyncNotify(AsyncRequest *areq)
{
	ForeignScanState *node = (ForeignScanState *) areq->requestee;
	RepFdwScanState *fsstate = (RepFdwScanState *) node->fdw_state;
	ReplicaConn *rconn = fsstate->rconn;

	Assert(!areq->callback_pending);

	if (fsstate->my_active && rconn->state == REP_STREAMING)
	{
		/* Queue chunks in batch_cxt (see IterateForeignScan). */
		MemoryContext oldcxt = MemoryContextSwitchTo(fsstate->batch_cxt);

		RepFdwDrainConn(rconn, fsstate->fetch_size);
		MemoryContextSwitchTo(oldcxt);
	}

	repfdw_async_produce(areq);
}

/*
 * repfdwExplainForeignScan (v2 -- M0)
 *		Per-child EXPLAIN: which replica this scan targets (of how many) and its
 *		remote SQL template.  N such Foreign Scans appear under the Append.
 */
static void
repfdwExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	List	   *fdw_private = fsplan->fdw_private;
	char	   *sql_template = strVal(linitial(fdw_private));
	char	   *remote_pred = strVal(lthird(fdw_private));
	bool		is_count_agg = boolVal(lfourth(fdw_private));
	int			my_index = intVal(list_nth(fdw_private, 5));
	int			nreplicas = intVal(list_nth(fdw_private, 6));

	/* Per-child scan shows its replica index; the agg node fans to all. */
	if (!is_count_agg)
		ExplainPropertyInteger("Replica", NULL, my_index, es);
	ExplainPropertyInteger("Replicas", NULL, nreplicas, es);

	/*
	 * Fold the pushed predicate into the same "Remote SQL Template" line
	 * (rather than a new label), mirroring postgres_fdw's single "Remote SQL"
	 * line.  The internal ctid $1/$2 slice bound is omitted here.
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
