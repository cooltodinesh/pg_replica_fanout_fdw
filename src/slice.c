/*-------------------------------------------------------------------------
 *
 * slice.c
 *		  nblocks discovery and ctid block-range math for pg_replica_fanout_fdw.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include "pg_replica_fanout_fdw.h"

/*
 * RepFdwGetNBlocks
 *		Discover the table's size, in blocks, on the given replica connection
 *		(inside its already-open REPEATABLE READ txn, so it's a stable
 *		snapshot read).  Each Append child asks its own replica; physical
 *		replicas share the primary's heap layout, so all agree on the block
 *		count -- modulo replay lag between replicas (the LSN-consistency caveat).
 */
BlockNumber
RepFdwGetNBlocks(ReplicaConn *rconn, const char *schema, const char *table)
{
	char	   *qualified = quote_qualified_identifier(schema, table);
	char	   *literal = quote_literal_cstr(qualified);
	StringInfoData sql;
	PGresult   *res;
	int64		bytes;
	BlockNumber nblocks;

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT pg_relation_size(%s::regclass)", literal);

	res = RepFdwExecSync(rconn, sql.data);

	if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
	{
		PQclear(res);
		ereport(ERROR,
				(errmsg("could not determine size of \"%s\" on replica \"%s:%d\"",
						qualified, rconn->host, rconn->port)));
	}

	bytes = strtoll(PQgetvalue(res, 0, 0), NULL, 10);
	PQclear(res);

	nblocks = (BlockNumber) (bytes / BLCKSZ);
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
