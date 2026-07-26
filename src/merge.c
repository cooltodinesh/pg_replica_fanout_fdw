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
					rconn->paused = false;

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

			RepStreamPump(fsstate->rset, nslices, fsstate->fetch_size);
			MemoryContextSwitchTo(oldcxt);
		}
	}
}
