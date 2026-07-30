-- count_pushdown.sql: unqualified count(*) is pushed down as an internal
-- combine -- each replica computes its own partial count over its ctid
-- slice, and the FDW itself SUMs the partials into a single emitted row.
-- There is no Agg node above the Foreign Scan in the finished plan.
--
-- Checks below are deliberately *discriminating*:
--   * A regressed combine that emitted one row per replica (instead of
--     summing) would show "actual rows=3", not 1.
--   * A stale/1-slice connection cache would show "Replicas: 1", not 3.
--   * Every negative-guard query below is NOT an unqualified count(*), so
--     none of them may be pushed down; each must fall back to a correct
--     local Aggregate over a plain (uncombined) Foreign Scan.
\i test/loopback-setup.sql

-- Multi-block table so that, with min_blocks_per_slice=1, nslices tracks
-- nconns (P = Min(nconns, nblocks) and nblocks >> 3 here) -- same sizing as
-- test/sql/invalidation.sql's inv_t.
CREATE TABLE count_t (id int, pad text);
INSERT INTO count_t SELECT g, repeat('q', 900) FROM generate_series(1, 400) g;

DROP SERVER IF EXISTS count_srv CASCADE;
CREATE SERVER count_srv FOREIGN DATA WRAPPER pg_replica_fanout_fdw
  OPTIONS (replicas 'localhost:5432,localhost:5432,localhost:5432',
           consistency 'none');
CREATE USER MAPPING FOR CURRENT_USER SERVER count_srv;
CREATE FOREIGN TABLE cft (id int, pad text)
  SERVER count_srv OPTIONS (table_name 'count_t', min_blocks_per_slice '1');

-- 1. Pushdown happens, and the answer is correct.
SELECT count(*) FROM cft;

-- No Aggregate/Partial Aggregate node above the Foreign Scan, and the
-- deparsed remote template is the count(*) shape.
EXPLAIN (COSTS OFF) SELECT count(*) FROM cft;

-- 2. Slices actually ran: Replicas: 3 (not a stale 1-slice cache), and
-- exactly one row came back (not one row per replica -- that would be a
-- broken combine).  Also an independent re-proof that the 3 slices are
-- disjoint and complete: SUM of partials = true row count.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT count(*) FROM cft;

-- 3. Negative guards -- pushdown must NOT trigger; local Agg must still
-- compute the correct answer.

-- count(DISTINCT id): aggdistinct != NIL (also aggstar is false here)
EXPLAIN (COSTS OFF) SELECT count(DISTINCT id) FROM cft;
SELECT count(DISTINCT id) FROM cft;

-- a WHERE clause with a non-shippable conjunct: local_conds != NIL, even
-- though the other conjunct (id > 100) is independently shippable and does
-- get folded into the remote template's WHERE.  (A WHERE that is *entirely*
-- shippable, e.g. just "id > 100", legitimately pushes count(*) down with
-- no Agg node -- see test/sql/qual_pushdown.sql's WHERE + count(*) case.)
EXPLAIN (COSTS OFF) SELECT count(*) FROM cft WHERE id > 100 AND random() < 2;
SELECT count(*) FROM cft WHERE id > 100 AND random() < 2;

-- GROUP BY: groupClause != NIL
EXPLAIN (COSTS OFF) SELECT id, count(*) FROM cft GROUP BY id;
SELECT count(*) FROM (SELECT id, count(*) FROM cft GROUP BY id) s;

-- sum(), not count(*): aggstar is false
EXPLAIN (COSTS OFF) SELECT sum(id) FROM cft;
SELECT sum(id) FROM cft;

-- HAVING: havingQual != NULL
EXPLAIN (COSTS OFF) SELECT count(*) FROM cft HAVING count(*) > 0;
SELECT count(*) FROM cft HAVING count(*) > 0;

-- 4. Reuse: a pushed count(*) followed by a plain scan in the same session
-- must both succeed -- the connection cache / active_scan guard has to
-- cycle correctly across the two plan shapes (scan_relid==0 vs. the normal
-- per-baserel scan).
SELECT count(*) FROM cft;
SELECT id FROM cft ORDER BY id LIMIT 3;

-- 5. Rescan: the pushed-down count(*) on the inner side of a nestloop is
-- re-executed once per outer row.  The combine latches eof after emitting
-- its single row, so ReScan must clear it -- otherwise the 2nd and 3rd
-- rescans return no row and outer rows silently drop.  enable_material=off
-- keeps the planner from caching the inner (which would mask the bug).
SET enable_material = off;
SELECT v.x, c.n
  FROM (VALUES (1), (2), (3)) v(x)
  CROSS JOIN LATERAL (SELECT count(*) AS n FROM cft) c
  ORDER BY v.x;
RESET enable_material;
