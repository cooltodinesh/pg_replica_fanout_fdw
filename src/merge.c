/*-------------------------------------------------------------------------
 *
 * merge.c
 *		  Raw round-robin merge and tuple materialization for
 *		  pg_replica_fanout_fdw.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "utils/builtins.h"

#include "pg_replica_fanout_fdw.h"

/*
 * RepFdwNextTuple
 *		Return the next merged row, round-robin across the active replica
 *		connections, pumping the stream loop as needed.  Returns an empty
 *		slot once every active replica has finished and drained.
 */
TupleTableSlot *
RepFdwNextTuple(RepFdwScanState *fsstate, ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	int			nslices = fsstate->nslices;

	MemoryContextReset(fsstate->row_cxt);

	for (;;)
	{
		bool		all_done = true;
		int			start = fsstate->rr_cursor;
		int			tries;

		for (tries = 0; tries < nslices; tries++)
		{
			int			idx = (start + tries) % nslices;
			ReplicaConn *rconn = &fsstate->rset->conns[idx];

			if (rconn->state != REP_DONE || rconn->rowqueue != NIL)
				all_done = false;

			if (rconn->rowqueue != NIL)
			{
				PGresult   *res = (PGresult *) linitial(rconn->rowqueue);
				HeapTuple	tuple;
				char	  **values;
				int			natts = fsstate->attinmeta->tupdesc->natts;
				int			j;
				ListCell   *lc;
				MemoryContext oldcxt;

				fsstate->rr_cursor = (idx + 1) % nslices;

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
				}

				if (rconn->paused &&
					RepFdwQueuedRowCount(rconn) <= fsstate->fetch_size)
				{
					rconn->paused = false;
					fsstate->wes_dirty = true;
				}

				ExecStoreHeapTuple(tuple, slot, false);
				return slot;
			}
		}

		if (all_done)
		{
			ExecClearTuple(slot);
			return slot;
		}

		/* Nobody had a row ready right now: pump the streams and retry. */
		{
			MemoryContext oldcxt = MemoryContextSwitchTo(fsstate->batch_cxt);

			RepStreamPump(fsstate);
			MemoryContextSwitchTo(oldcxt);
		}
	}
}

/*
 * RepFdwNextCountTuple
 *		Combine step for the count(*) pushdown plan shape (see
 *		notes/phase-b-count.md).  Pumps every replica to REP_DONE, sums the
 *		single int8 partial count each one returns for its ctid slice, and
 *		emits exactly one row.  There is no per-replica interleaving to do
 *		here (unlike RepFdwNextTuple) since nothing is returned until every
 *		replica has finished.
 */
TupleTableSlot *
RepFdwNextCountTuple(RepFdwScanState *fsstate, ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	int			nslices = fsstate->nslices;
	int64		total = 0;
	int			i;

	if (fsstate->eof)
	{
		ExecClearTuple(slot);
		return slot;
	}

	for (;;)
	{
		bool		all_done = true;

		for (i = 0; i < nslices; i++)
		{
			if (fsstate->rset->conns[i].state != REP_DONE)
			{
				all_done = false;
				break;
			}
		}

		if (all_done)
			break;

		{
			MemoryContext oldcxt = MemoryContextSwitchTo(fsstate->batch_cxt);

			RepStreamPump(fsstate);
			MemoryContextSwitchTo(oldcxt);
		}
	}

	for (i = 0; i < nslices; i++)
	{
		ReplicaConn *rconn = &fsstate->rset->conns[i];
		PGresult   *res;

		if (rconn->rowqueue == NIL)
			elog(ERROR,
				 "pg_replica_fanout_fdw: replica \"%s:%d\" returned no result for pushed-down count(*)",
				 rconn->host, rconn->port);

		res = (PGresult *) linitial(rconn->rowqueue);

		if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
			elog(ERROR,
				 "pg_replica_fanout_fdw: replica \"%s:%d\" returned an unexpected result for pushed-down count(*)",
				 rconn->host, rconn->port);

		total += pg_strtoint64(PQgetvalue(res, 0, 0));

		PQclear(res);
		rconn->rowqueue = list_delete_first(rconn->rowqueue);
		rconn->cur_row = 0;
	}

	ExecClearTuple(slot);
	slot->tts_values[0] = Int64GetDatum(total);
	slot->tts_isnull[0] = false;
	ExecStoreVirtualTuple(slot);

	fsstate->eof = true;
	return slot;
}
