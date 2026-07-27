# pg_replica_fanout_fdw

A PostgreSQL foreign-data wrapper that fans a **sliced table scan** across N
streaming replicas and merges the rows on the coordinator, so that a large
table scan gets roughly 1/N the I/O per node instead of hitting one server.

It supports a raw fan-out scan plus pushdown of unqualified `count(*)`
(see "Aggregate pushdown" below); everything else (WHERE, ORDER BY,
LIMIT, and every other aggregate) is still evaluated locally on the
coordinator.

Requires **PostgreSQL 19+** (uses `PQsetChunkedRowsMode`/
`PGRES_TUPLES_CHUNK`, the `libpqsrv_*` connection helpers, and the
resource-owner form of `CreateWaitEventSet`).

## What it does

```sql
CREATE EXTENSION pg_replica_fanout_fdw;

CREATE SERVER my_replicas FOREIGN DATA WRAPPER pg_replica_fanout_fdw
  OPTIONS (replicas 'replica1:5432,replica2:5432,replica3:5432');

CREATE USER MAPPING FOR CURRENT_USER SERVER my_replicas;

CREATE FOREIGN TABLE big_ft (id int, val text)
  SERVER my_replicas OPTIONS (table_name 'big');

SELECT * FROM big_ft;   -- each replica scans a disjoint ctid block range
```

Each replica must hold a byte-identical copy of the table (a physical
streaming standby of the same primary). The coordinator itself never reads
the table's local heap blocks — it only dispatches per-slice queries over
libpq and round-robin-merges the results. To have the coordinator/primary
contribute I/O too, just list it in `replicas` like any other node.

## Options

| Option | Object | Default | Notes |
|---|---|---|---|
| `replicas` | SERVER | *(required)* | `'host1[:port],host2,...'`; order = replica/slice index |
| `dbname` | SERVER | coordinator's current database | standbys share the primary's catalogs |
| `consistency` | SERVER | `'none'` | only `'none'` is currently supported |
| `fetch_size` | SERVER/table | 1000 | rows per streamed chunk |
| `connect_timeout` | SERVER | 5 (seconds) | |
| `application_name` | SERVER | `pg_replica_fanout_fdw` | |
| `user`, `password` | USER MAPPING | *(none)* | one credential set for all replicas |
| `table_name` | FOREIGN TABLE | the foreign table's own name | |
| `schema_name` | FOREIGN TABLE | the foreign table's own schema | |
| `min_blocks_per_slice` | FOREIGN TABLE | 128 | anti-over-slicing guard; lower it (e.g. `1`) to force multi-way splits in tests on small tables |
| `column_name` | COLUMN | the column's own name | remote column name, if it differs from the local one |

## Aggregate pushdown

- **`count(*)` is pushed down.** An unqualified `SELECT count(*) FROM
  big_ft` (no `WHERE`, `GROUP BY`, `HAVING`, or `DISTINCT`) is answered by
  having each replica compute `count(*)` over its own ctid slice and
  summing the partials on the coordinator — only N small integers cross
  the network, not every row. `EXPLAIN` shows a single `Foreign Scan`
  with no `Aggregate` node above it. Anything else (`count(DISTINCT
  ...)`, a `WHERE` clause, `GROUP BY`, `sum`/`avg`/`min`/`max`, `HAVING`)
  falls back to fetching every row and aggregating locally.

## Known limitations

- **Read-only.** No INSERT/UPDATE/DELETE, no join pushdown.
- **No pushdown beyond unqualified `count(*)`.** WHERE clauses, `ORDER
  BY`, `LIMIT`, and every other aggregate are always evaluated locally on
  the coordinator after every row has been shipped back.
- **`consistency='none'` only.** No LSN alignment; each replica reads
  under its own `REPEATABLE READ` snapshot with no cross-replica skew
  bound. Any connect or scan failure is a plain `ERROR` — there is no
  degraded/redistribute mode.
- **Slice bounds are sized from replica 0 only.** `nblocks` (and hence
  every slice's ctid range) is discovered by asking replica 0 for
  `pg_relation_size()`, once, at the start of the scan. If a *different*
  replica is lagging and hasn't replayed out to that block count yet, the
  middle slices it's assigned (bounded on both sides) can silently return
  fewer rows than expected for blocks it hasn't caught up to. The
  open-ended first and last slices (`ctid < hi` / `ctid >= lo`, no other
  bound) aren't affected the same way since they cover whatever the
  replica actually has. There's no cross-replica alignment to fix this in
  `consistency='none'` — that needs LSN-based coordination.
- **One live scan per replica connection.** Each cached connection
  streams a single chunked query at a time (`PQsendQuery` +
  `PQsetChunkedRowsMode`), so it can't serve two concurrently-active
  foreign scans. A self-join of the same foreign table, or a plan with
  the FDW on both sides of a merge join, will error rather than corrupt
  results. A nested-loop **rescan** of the same scan node is fine — it
  re-runs the inner scan sequentially. Lifting this needs per-scan
  cursors or per-replica connection pooling.
- Only plain heap tables are supported on the replica side.

## Verifying the scan-side I/O split (manual check)

Automated `make installcheck` deliberately avoids `BUFFERS`/timing
assertions — they're stats- and storage-dependent and flaky in CI. To
confirm slicing is actually reducing per-replica I/O, run one slice's
query directly against a replica and check its buffer count:

```sql
-- on the coordinator: find the real block count and a slice's ctid range
SELECT pg_relation_size('big'::regclass) / current_setting('block_size')::int;
--  -> total_blocks

-- on one replica, P = number of participating replicas (see EXPLAIN
-- output of the foreign scan on the coordinator for the exact template
-- and replica count):
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, val FROM big
WHERE ctid >= '(LO,0)'::tid AND ctid < '(HI,0)'::tid;
```

Confirm:
- the plan node is **`Tid Range Scan`** (not a plain `Seq Scan`);
- `Buffers: shared read+hit` is approximately `total_blocks / P`.

This proves the block-range split even on a single VM. A genuine
wall-clock speedup requires independent storage per replica (separate
disks/volumes, or real separate nodes) — a single shared-disk VM can't
demonstrate that, since every replica ends up contending for the same
underlying I/O path.

## Testing

`make installcheck` runs six suites against a **loopback harness**: all
"replicas" point at the same local instance (`replicas
'localhost:5432,localhost:5432,localhost:5432'`). Disjoint ctid slices
still union to exactly the whole table, so slicing/fan-out/merge/rescan
correctness is fully exercised on one instance, even though it can't
demonstrate a real I/O speedup (see above).

- `basic` — extension/server/mapping/table setup, a smoke `SELECT`, and
  validator error checks (missing `replicas`, `consistency='lsn'`,
  unknown option name).
- `slicing` — the core correctness proof: full row-set equality
  (`EXCEPT` both ways), count and `md5(string_agg(...))` checksum vs. the
  base table on a 100k-row table (real 3-way fan-out), plus empty /
  single-block / fewer-blocks-than-replicas / single-replica-server edge
  cases, plus a correlated-subquery rescan test.
- `types_nulls` — `int4/int8/text/numeric/bool/timestamptz` with
  interspersed `NULL`s round-trip exactly through the text protocol.
- `errors` — a server with an unreachable replica errors cleanly, names
  the replica, and respects `connect_timeout` instead of hanging.
- `invalidation` — `ALTER SERVER`/`ALTER USER MAPPING` are picked up by
  the next query in the same session.
- `count_pushdown` — unqualified `count(*)` is pushed down (no `Agg`
  node, correct sum of per-replica partials); every other aggregate
  shape (`count(DISTINCT ...)`, a `WHERE` clause, `GROUP BY`, `sum`,
  `HAVING`) falls back to a correct local `Aggregate` over a plain scan.

```
make PG_CONFIG=/path/to/pg_config
make install PG_CONFIG=/path/to/pg_config
make installcheck PG_CONFIG=/path/to/pg_config
```

## Repo layout

```
Makefile                          PGXS build (MODULE_big, REGRESS)
pg_replica_fanout_fdw.control
sql/pg_replica_fanout_fdw--0.1.0.sql     handler()/validator() + CREATE FOREIGN DATA WRAPPER
src/
  pg_replica_fanout_fdw.h                shared structs/decls
  pg_replica_fanout_fdw.c                handler + FDW plan/exec callbacks
  option.c                        validator, option parsing, replicas-list parser
  connection.c                    conn cache, concurrent connect, streaming loop, xact callbacks
  deparse.c                       SELECT template with ctid placeholder
  slice.c                         nblocks discovery + block-range math
  merge.c                         round-robin merge + tuple materialization
test/
  loopback-setup.sql              shared \i'd setup for the loopback suites
  sql/, expected/                 pg_regress suites (see Testing above)
README.md
```
