/*-------------------------------------------------------------------------
 *
 * slice.c
 *		  nblocks discovery and ctid block-range math for pg_replica_fanout_fdw.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "storage/bufmgr.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pg_replica_fanout_fdw.h"

/*
 * RepFdwGetNBlocks
 *		Return the table's size in blocks, read from the co-located *local* copy
 *		rather than from a replica.  The coordinator is itself an instance of the
 *		cluster, so this table is always present locally as a byte-identical
 *		physical replica of the remote heaps -- they share the primary's exact
 *		block layout.  Reading the size locally (a cheap in-process smgr lookup)
 *		avoids a network round trip that would otherwise hit a possibly-remote
 *		replica just to size, and gives every Append child one authoritative
 *		block count, so their ctid slice boundaries provably line up (a single
 *		divisor, no gaps or overlaps).  The one caveat is unchanged in kind:
 *		replay lag between this local copy and a given replica's heap (the
 *		LSN-consistency caveat), now anchored to the local instance.
 *
 *		The local table is a hard requirement of this design, not a costing
 *		nicety: nblocks defines the ctid ranges, so a missing or non-ordinary
 *		local table is an error, never a silent fallback.
 */
BlockNumber
RepFdwGetNBlocks(const char *schema, const char *table)
{
	Oid			nspoid = get_namespace_oid(schema, true);
	Oid			relid = OidIsValid(nspoid) ?
		get_relname_relid(table, nspoid) : InvalidOid;
	Relation	rel;
	BlockNumber nblocks;

	if (!OidIsValid(relid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("local table \"%s.%s\" not found",
						schema, table),
				 errdetail("The coordinator must be an instance of the same "
						   "cluster as the replicas, with this table present "
						   "locally; its heap block count drives the ctid slice "
						   "bounds.")));

	rel = table_open(relid, AccessShareLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("local relation \"%s.%s\" is not an ordinary table",
						schema, table)));

	nblocks = RelationGetNumberOfBlocks(rel);
	table_close(rel, AccessShareLock);
	return nblocks;
}

/*
 * RepFdwComputeSlices
 *		Compute the number of participating replicas P and the ctid bound
 *		for each of their slices: P = Min(nconns, Max(1,
 *		nblocks/min_blocks_per_slice)), contiguous non-overlapping block
 *		ranges, first/last slice open-ended.
 *		Returns P and sets *bounds_out to a palloc'd array[P] of bounds
 *		(bind-parameter values, not SQL text -- see RepFdwBuildBoundedSql).
 */
int
RepFdwComputeSlices(BlockNumber nblocks, int nconns, int min_blocks_per_slice,
					RepFdwCtidBound **bounds_out)
{
	int			P;
	RepFdwCtidBound *bounds;
	int			i;

	if (nblocks == 0)
		P = 1;
	else
	{
		BlockNumber by_size = nblocks / Max(1, min_blocks_per_slice);

		P = Min(nconns, Max(1, (int) by_size));
	}

	bounds = (RepFdwCtidBound *) palloc0(sizeof(RepFdwCtidBound) * P);

	for (i = 0; i < P; i++)
	{
		uint64		lo = (uint64) nblocks * i / P;
		uint64		hi = (uint64) nblocks * (i + 1) / P;

		if (i > 0)
			bounds[i].lo = psprintf("(" UINT64_FORMAT ",0)", lo);
		if (i < P - 1)
			bounds[i].hi = psprintf("(" UINT64_FORMAT ",0)", hi);
	}

	*bounds_out = bounds;
	return P;
}
