-- qual_pushdown.sql: shippable top-level AND conjuncts of a WHERE clause are
-- pushed into each replica's per-slice query (each Append child's remote SQL);
-- everything else stays a local Filter on the Async Foreign Scan.
--
-- The governing rule: N replicas each evaluate the pushed predicate
-- independently against their own ctid slice, so the fan-out answer equals
-- the local answer only if the predicate is IMMUTABLE.  This is stricter
-- than postgres_fdw, which ships STABLE functions (e.g. now()) freely -- we
-- must not, since replicas' clocks and snapshots can differ.  Checks below
-- are deliberately *discriminating*, in the same style as
-- test/sql/aggregation.sql.
\i test/loopback-setup.sql

-- Multi-block table so that, with min_blocks_per_slice=1, nslices tracks
-- nconns (P = Min(nconns, nblocks) and nblocks >> 3 here) -- same sizing as
-- test/sql/count_pushdown.sql's count_t.  One extra NULL-pad row (id=0,
-- deliberately NOT > 100) is added for the IS NULL / IS NOT NULL checks
-- without disturbing the "id > 100" counts used elsewhere below.
CREATE TABLE qual_t (id int, pad text, ts timestamptz);
INSERT INTO qual_t
  SELECT g, repeat('q', 900), '2020-01-01'::timestamptz + (g || ' seconds')::interval
  FROM generate_series(1, 400) g;
INSERT INTO qual_t VALUES (0, NULL, '2020-01-01'::timestamptz);

DROP SERVER IF EXISTS qual_srv CASCADE;
CREATE SERVER qual_srv FOREIGN DATA WRAPPER pg_replica_fanout_fdw
  OPTIONS (replicas 'localhost:5432,localhost:5432,localhost:5432',
           consistency 'none');
CREATE USER MAPPING FOR CURRENT_USER SERVER qual_srv;
CREATE FOREIGN TABLE qft (id int, pad text, ts timestamptz)
  SERVER qual_srv OPTIONS (table_name 'qual_t', min_blocks_per_slice '1');

-- 1. A shippable qual on a plain scan is folded into the remote template's
-- WHERE, with no local Filter at all.
EXPLAIN (COSTS OFF) SELECT id FROM qft WHERE id > 100;
SELECT count(*) FROM qft WHERE id > 100;

-- 2. Mixed conjuncts: "id > 100" is independently shippable and is folded
-- into the remote template's WHERE; "random() < 0.5" is volatile and stays
-- a local Filter.  This proves the per-conjunct split -- one WHERE clause
-- ends up half remote, half local.  (random() makes the row count
-- nondeterministic, so only the plan shape is checked here.)
EXPLAIN (COSTS OFF) SELECT id FROM qft WHERE id > 100 AND random() < 0.5;

-- 3. STABLE is rejected (the crux of this suite): now() can read a
-- different value on each replica's connection, so "ts < now()" must stay
-- a local Filter and the remote template must have NO WHERE at all -- the
-- deliberate divergence from postgres_fdw, which would ship this.
EXPLAIN (COSTS OFF) SELECT count(*) FROM qft WHERE ts < now();
SELECT count(*) FROM qft WHERE ts < now();

-- 4. Volatile is rejected: random() must never be shipped, even wrapped in
-- a cast/arithmetic.  Nondeterministic row count, so only the plan shape is
-- checked.
EXPLAIN (COSTS OFF) SELECT id FROM qft WHERE id > (random() * 1000)::int;

-- 5. IN / IS NULL / OR / NOT all push down, and results are correct.
EXPLAIN (COSTS OFF) SELECT count(*) FROM qft WHERE id IN (1, 2, 3);
SELECT count(*) FROM qft WHERE id IN (1, 2, 3);

EXPLAIN (COSTS OFF) SELECT count(*) FROM qft WHERE pad IS NULL;
SELECT count(*) FROM qft WHERE pad IS NULL;
SELECT count(*) FROM qft WHERE pad IS NOT NULL;

EXPLAIN (COSTS OFF) SELECT count(*) FROM qft WHERE id = 5 OR id = 10;
SELECT count(*) FROM qft WHERE id = 5 OR id = 10;

EXPLAIN (COSTS OFF) SELECT count(*) FROM qft WHERE NOT (id > 100);
SELECT count(*) FROM qft WHERE NOT (id > 100);

-- 6. A column whose "column_name" option differs from the local name: a
-- qual on the local name must appear under the REMOTE name in the pushed
-- predicate (and in the column list).
CREATE FOREIGN TABLE qft2 (my_id int OPTIONS (column_name 'id'), pad text)
  SERVER qual_srv OPTIONS (table_name 'qual_t', min_blocks_per_slice '1');

EXPLAIN (COSTS OFF) SELECT my_id FROM qft2 WHERE my_id > 100;
SELECT count(*) FROM qft2 WHERE my_id > 100;

-- 7. count(*) pushdown composes with qual pushdown.  A shippable WHERE on
-- the count(*) plan shape still produces a single Foreign Scan with no Agg
-- node above it -- the per-slice query is "SELECT count(*) FROM ... WHERE
-- (<pred>) AND ctid ...", and RepFdwNextCountTuple sums the partials
-- unchanged.
EXPLAIN (COSTS OFF) SELECT count(*) FROM qft WHERE id > 100;
SELECT count(*) FROM qft WHERE id > 100;

-- BUFFERS OFF: ANALYZE enables buffer accounting by default on PG18+, and
-- the "Planning: Buffers:" line is catalog-cache-warmth dependent, i.e.
-- flaky (same reason as test/sql/count_pushdown.sql / invalidation.sql).
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
  SELECT count(*) FROM qft WHERE id > 100;
