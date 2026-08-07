# pg_replica_fanout_fdw

A PostgreSQL foreign-data wrapper that fans a **sliced table scan** across N
streaming replicas and merges the rows on the coordinator, so that a large
table scan gets roughly 1/N the I/O per node instead of hitting one server.

It supports a raw fan-out scan, pushdown of shippable `WHERE` quals (see
"Qual pushdown" below), and pushdown of `count(*)` including a shippable
`WHERE` (see "Aggregate pushdown" below); everything else (`ORDER BY`,
`LIMIT`, and every other aggregate) is still evaluated locally on the
coordinator. Not every scan fans out: a query an index can serve is answered
directly from the coordinator's local copy, and a large `IN`-list is split
across replicas by value instead of by block — the mode is chosen per query
from what the local planner would do (see "Execution plan selection").

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
streaming standby of the same primary), and the **coordinator is itself one
instance of that cluster** with the table present locally. It uses that local
copy two ways: to size the ctid slices (one authoritative block count), and —
for a query an index can serve — to answer the scan directly without any
fan-out (see "Execution plan selection"). For a big fan-out scan it dispatches
per-slice queries over libpq and merges the results. To have the coordinator
contribute fan-out I/O too, just list it in `replicas` like any other node.

## Requirements

- **PostgreSQL 19+**, built from source (this uses `PQsetChunkedRowsMode`/
  `PGRES_TUPLES_CHUNK`, the `libpqsrv_*` helpers, and the resource-owner
  `CreateWaitEventSet`). Build the extension against the **same** server's
  `pg_config` — a version mismatch will fail to load with a magic-block error.
- **libpq** (client library + headers) available to that `pg_config`; the module
  links `-lpq` and opens libpq connections to the replicas at runtime.
- A **physical streaming-replication cluster**: one primary and N hot standbys
  built from it (`pg_basebackup`), all byte-identical. The **coordinator** (where
  you `CREATE EXTENSION` and run queries) must **itself be an instance of that
  cluster** — primary or standby — with the target table present locally. That
  local copy is not optional: its block count sizes the ctid slices (see
  "Known limitations"). List the replicas the FDW should fan out to in the
  `replicas` option; include the coordinator's own address there too if you want
  it to carry a slice.

## Building and installing

From the extension directory (`pg_replica_fanout_fdw/`):

```sh
# Point PG_CONFIG at the target server's pg_config (omit if it's already on PATH).
make        PG_CONFIG=/path/to/pg-install/bin/pg_config
make install PG_CONFIG=/path/to/pg-install/bin/pg_config   # may need sudo if the install tree is root-owned
```

This builds `pg_replica_fanout_fdw.so` and installs it plus the `.control` and
`.sql` files into the server's `$libdir`/extension dirs. No
`shared_preload_libraries` entry is needed — the FDW handler is loaded on demand
by `CREATE EXTENSION` / first use.

Then, in the coordinator database:

```sql
CREATE EXTENSION pg_replica_fanout_fdw;
```

To run the regression suite against a running server (loopback harness — no real
standbys required), see "Testing" below.

## Setting up the replica cluster (if you don't already have one)

The FDW consumes an existing streaming-replication cluster; it does not create
one. A minimal local setup, all on one host with distinct ports:

```sh
BIN=/path/to/pg-install/bin

# 1. Primary
$BIN/initdb -D /data/primary
echo "wal_level=replica"            >> /data/primary/postgresql.conf
echo "max_wal_senders=10"           >> /data/primary/postgresql.conf
echo "listen_addresses='*'"         >> /data/primary/postgresql.conf   # or specific IPs
# allow replication + client connections in /data/primary/pg_hba.conf, then:
$BIN/pg_ctl -D /data/primary -o "-p 5432" -l /data/primary/log start

# 2. Each standby (repeat per replica, distinct -D and port)
$BIN/pg_basebackup -h <primary-host> -p 5432 -D /data/standby1 -R -X stream
$BIN/pg_ctl -D /data/standby1 -o "-p 5433" -l /data/standby1/log start   # hot_standby=on is the default
```

`pg_basebackup -R` writes the `primary_conninfo` so each standby streams
automatically. Confirm every standby is caught up before querying
(`SELECT pg_last_wal_replay_lsn();` on each vs. `pg_current_wal_lsn()` on the
primary). Load the table you want to scan on the primary; it replicates to every
standby (and is present on the coordinator, being a cluster instance).

## Creating the FDW objects and verifying

On the coordinator, against the database that has the local table:

```sql
CREATE EXTENSION IF NOT EXISTS pg_replica_fanout_fdw;

CREATE SERVER my_replicas FOREIGN DATA WRAPPER pg_replica_fanout_fdw
  OPTIONS (replicas 'standby1:5433,standby2:5434,standby3:5435');

CREATE USER MAPPING FOR CURRENT_USER SERVER my_replicas
  OPTIONS (user 'repl_reader', password '...');   -- one credential set for all replicas

CREATE FOREIGN TABLE big_ft (id int, val int, pad text)
  SERVER my_replicas OPTIONS (table_name 'big');   -- schema_name defaults to the FT's schema

-- Verify: the plan should be an Append of per-replica Async Foreign Scans,
-- each with a disjoint ctid range ($1/$2) in its Remote SQL.
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM big_ft;
SELECT count(*) FROM big_ft;   -- must match SELECT count(*) FROM big
```

If a replica is unreachable the scan errors and names it (respecting
`connect_timeout`) rather than returning a partial result. See "Verifying the
scan-side I/O split" for confirming the block-range split actually reduces
per-replica I/O.

## Options

| Option | Object | Default | Notes |
|---|---|---|---|
| `replicas` | SERVER | *(required)* | `'host1[:port],host2,...'`; order = replica/slice index |
| `consistency` | SERVER | `'none'` | only `'none'` is currently supported |
| `fetch_size` | SERVER/table | 1000 | rows per streamed chunk |
| `connect_timeout` | SERVER | 5 (seconds) | |
| `application_name` | SERVER | `pg_replica_fanout_fdw` | |
| `user`, `password` | USER MAPPING | *(none)* | one credential set for all replicas |
| `table_name` | FOREIGN TABLE | the foreign table's own name | |
| `schema_name` | FOREIGN TABLE | the foreign table's own schema | |
| `min_blocks_per_slice` | FOREIGN TABLE | 128 | anti-over-slicing guard; lower it (e.g. `1`) to force multi-way splits in tests on small tables |
| `column_name` | COLUMN | the column's own name | remote column name, if it differs from the local one |

## Qual pushdown

- **Shippable `WHERE` quals are sent to the replicas.** Each top-level
  `AND` conjunct of a `WHERE` clause is pushed independently: `Var`s of the
  scanned table, `Const`s, comparison operators, `AND`/`OR`/`NOT`,
  `IS [NOT] NULL`, and `IN`/`= ANY`/`= ALL` are all shippable, as long as
  the whole conjunct is **immutable**. Every replica evaluates the pushed
  predicate independently against its own ctid slice, so an expression
  that could evaluate *differently* per replica — a `STABLE` function like
  `now()` or `current_user`, or anything `VOLATILE` — is never pushed, even
  though `STABLE` alone would be safe to ship in an ordinary single-node
  query. Any conjunct that isn't shippable stays a local `Filter` above the
  `Foreign Scan`; `EXPLAIN`'s "Remote SQL Template" line shows exactly what
  was shipped.

## Aggregate pushdown

- **`count(*)` is pushed down, including a shippable `WHERE`.** A
  `SELECT count(*) FROM big_ft` with no `GROUP BY`, `HAVING`, or
  `DISTINCT`, and whose `WHERE` (if any) is entirely shippable per "Qual
  pushdown" above, is answered by having each replica compute `count(*)`
  over its own ctid slice (and the pushed predicate, if any) and summing
  the partials on the coordinator — only N small integers cross the
  network, not every row. `EXPLAIN` shows a single `Foreign Scan` with no
  `Aggregate` node above it. Anything else (`count(DISTINCT ...)`, a
  `WHERE` with a non-shippable conjunct, `GROUP BY`,
  `sum`/`avg`/`min`/`max`, `HAVING`) falls back to fetching every row and
  aggregating locally.

## Execution plan selection

A scan of a foreign table runs in one of three **modes**, chosen once during
planning from the query's shape and from what the *local* planner would do with
the co-located copy. The coordinator plans (but does not execute) an equivalent
scan of the local table and inspects the access path it picks — the local
instance has the same indexes and full column statistics as the replicas, so its
choice is the faithful signal for "would fanning this out actually help?" A
foreign table's own costing can never surface this, because the planner never
builds an index path on a foreign table.

| Local plan for the query | Mode | What runs |
|---|---|---|
| No shippable `WHERE`, or a **sequential / TID-range scan** | **ctid-slice fan-out** (default) | each replica scans a disjoint heap **block range** (its ctid slice) and streams rows back; the coordinator merges them |
| **Index / bitmap / index-only scan** on a shippable `WHERE` | **serve-local** | the whole query is answered from the **coordinator's own local copy** via one index scan — no fan-out, no replica connection, nothing over the network |
| Index-served, and the whole shippable qual is one large **`IN (...)`** | **value-split fan-out** | the value list is sorted, de-duplicated and dealt in contiguous chunks across `P = min(replicas, distinct values)` replicas; each runs a real index scan over `col = ANY(its chunk)`, with no ctid bound |

Why each:

- **Big block scans** are where fan-out pays: there is a lot of I/O to divide,
  and each replica reads only ~1/N of the heap. This is the extension's original
  purpose.
- **A selective index lookup is already fast on one node**, so fanning it out N
  ways would only duplicate the index descent on every replica. Serving it from
  the coordinator's own copy avoids the network entirely.
- **A large `IN`-list** on an indexed column is the case where fan-out *does*
  help an index scan: splitting the values parallelizes index probing that a
  single node does serially, while sorted contiguous chunks keep each replica's
  scan local to a tight run of index leaf pages. De-duplication is required for
  correctness — a value in two chunks would be returned by two replicas.

`EXPLAIN` names the non-default modes: serve-local shows `Fanout Mode:
serve-local` and a `Local SQL` line (and no per-replica children); value-split
shows `Fanout Mode: value-split fan-out` with each child's own `col = ANY(chunk)`
template. The default ctid-slice fan-out prints no mode line (an `Append` of
per-replica `Async Foreign Scan`s).

The three modes partition along different dimensions but stay sound the same
way: the ctid bound is emitted **only** in ctid-slice mode — the keyed modes
(serve-local, value-split) never carry one, since a value/key partition plus a
block-range bound would drop rows. Not yet mode-aware: `count(*)` pushdown always
uses the fan-out combine node regardless of an index, and range / `ORDER BY` /
`GROUP BY`-by-key splitting (which would need a value histogram) isn't
implemented.

## Known limitations

- **Read-only.** No INSERT/UPDATE/DELETE, no join pushdown.
- **No pushdown beyond shippable `WHERE` quals and `count(*)`.** `ORDER
  BY`, `LIMIT`, parameterized quals (values coming from a join's outer
  side), and every other aggregate are always evaluated locally on the
  coordinator after every row has been shipped back.
- **`consistency='none'` only.** No LSN alignment; each replica reads
  under its own `REPEATABLE READ` snapshot with no cross-replica skew
  bound. Any connect or scan failure is a plain `ERROR` — there is no
  degraded/redistribute mode.
- **Slice bounds are sized from the coordinator's local copy.** `nblocks`
  (and hence every slice's ctid range) is read once from the co-located local
  table (`RelationGetNumberOfBlocks`), so every slice shares one authoritative
  divisor and the boundaries line up exactly. If a *different* replica is
  lagging and hasn't replayed out to that block count yet, the middle slices
  it's assigned (bounded on both sides) can silently return fewer rows than
  expected for blocks it hasn't caught up to. The open-ended first and last
  slices (`ctid < hi` / `ctid >= lo`, no other bound) aren't affected the same
  way since they cover whatever the replica actually has. There's no
  cross-replica alignment to fix this in `consistency='none'` — that needs
  LSN-based coordination.
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

`make installcheck` runs eight suites against a **loopback harness**: all
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
- `aggregation` — `count(*)` (with or without a shippable `WHERE`) is
  pushed down (no `Agg` node, correct sum of per-replica partials); every
  other aggregate shape (`count(DISTINCT ...)`, a non-shippable `WHERE`
  conjunct, `GROUP BY`, `sum`, `HAVING`) falls back to a correct local
  `Aggregate` over a plain scan.
- `qual_pushdown` — shippable `WHERE` conjuncts (comparisons, `IN`,
  `IS NULL`, `OR`, `NOT`, a `column_name`-mapped column) are folded into
  the remote template and produce correct results; `STABLE` (e.g.
  `now()`) and `VOLATILE` (e.g. `random()`) conjuncts are rejected and
  stay a local `Filter`, including in a mixed AND with a shippable
  conjunct.
- `exec_modes` — the execution mode is chosen from the local plan: a
  selective indexed qual is served locally (single `Foreign Scan`,
  `Fanout Mode: serve-local`, no fan-out) with the right rows and a stable
  set under rescan; a full scan and a non-selective unindexed qual fan out
  by ctid slice; a large indexed `IN`-list splits into disjoint key-sorted
  per-replica chunks (correct, complete, duplicate-free), while a small
  `IN` stays local.

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
  deparse.c                       SELECT template, qual shippability/deparse, ctid placeholder, IN-list chunking
  slice.c                         local nblocks + block-range (ctid slice) math
test/
  loopback-setup.sql              shared \i'd setup for the loopback suites
  sql/, expected/                 pg_regress suites (see Testing above)
README.md
```
