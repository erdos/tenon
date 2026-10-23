# Experiment: SQLite → Turso Database

Swapped tenon's storage from SQLite (`org.xerial/sqlite-jdbc` 3.53.4.0) to
Turso Database, the Rust rewrite of SQLite (`tech.turso/turso` 0.8.3 JDBC
driver, embedded, local file), and measured the impact.

**Verdict: not worth it today.** After porting and tuning, Turso passes the
whole test suite but is **~2–10x slower on every workload**: writes ~6x,
concurrent writes ~10x, point reads ~10x, listings ~2–3x. Its write cost also
grows with table size, while SQLite's stays flat.

## How to run

```
clojure -M:bench -m tenon.bench          # SQLite
clojure -M:turso:bench -m tenon.bench    # Turso
clojure -M:turso:test -m cognitect.test-runner -d test -d http/test
```

The `:turso` alias adds the driver and sets `-Dtenon.backend=turso`. Without
it, nothing changes for SQLite.

## Results

4 vCPU cloud sandbox, JDK 21, durable commits (synchronous FULL) on both.
Throughput in ops/s, two runs each (SQLite write numbers vary ~30% between
runs on this disk). "Turso naive" is the first working port. "Turso tuned"
also spells out the `workflow_state` view (see below).

| Workload | SQLite | Turso naive | Turso tuned | Tuned vs SQLite |
|---|---|---|---|---|
| New workflow, 1 thread (insert + finish) | 1039 / 710 | 142 | 170 / 127 | ~6x slower |
| New workflow, 16 threads | 1520 / 1313 | 127 | 145 / 135 | ~10x slower |
| Reuse a finished result | 1692 / 1100 | 37 | 282 / 292 | ~5x slower |
| Nested tree of 364 workflows (p50 ms) | 390 / 363 | 3461 | 3105 / 3706 | ~9x slower |
| get-invocation, random id | 53k / 47k | 144 | 5.0k / 4.6k | ~10x slower |
| List page of 100 | 810 / 707 | 19 | 349 / 383 | ~2x slower |
| List, state = ERROR | 459 / 342 | 10 | 160 / 174 | ~2.3x slower |
| List by wf-def | 1250 / 878 | 20 | 310 / 346 | ~3x slower |
| full-timeline of 364-node tree | 177 / 179 | 39 | 39 / 36 | ~4.7x slower |
| DB file + WAL after the run | 2.9 MB | 5.3 MB | 3.9–4.9 MB | larger |

Test suite: 198 tests / 2288 assertions pass on both. Wall time 24 s on SQLite,
42 s on Turso.

## Why Turso is slower

1. **Views are not flattened.** Every query through `workflow_state`
   scanned the whole `workflow` table (`SCAN workflow`), where SQLite does six
   primary-key lookups. That made the idempotency check on every call O(n):
   15 ms vs 0.28 ms at 11k workflows. Spelling the view out as joins for Turso
   (`for-backend` in `sqlite.clj`) fixed most of the read gap ("tuned").
2. **Trigger-driven writes are expensive and grow with the table.** With fsync
   off (pure CPU), `insert-workflow!` costs 2.6 ms at 2k events and 6.8 ms at
   20k on Turso, vs a flat ~0.05–0.1 ms on SQLite. `finish!` and
   `record-reuse!` behave the same way. The query plans use primary keys, so
   the cost is inside Turso's trigger execution. tenon's schema moves its
   pointers with triggers, so every write pays it.
3. **Commits cost ~2x more.** A plain durable one-row insert: 0.67 ms on Turso
   vs 0.28 ms on SQLite. With fsync off both are ~0.025 ms.
4. **No prepared-statement reuse.** The driver never resets a statement (see
   below), so each execution prepares a new one. Small cost (0.14 ms vs
   0.06 ms for the get-workflow query), but it adds up.
5. **Concurrency doesn't help.** 16 writer threads reach about the same
   throughput as 1 thread (~140 ops/s). SQLite gains ~1.5x from group commit.

## What the port needed

Unsupported in Turso 0.8.3, worked around:

- `WITHOUT ROWID` tables are experimental (CLI flag only): `idempotence`
  became a rowid table with a unique BLOB key.
- `INSTEAD OF` triggers are not supported: `new_workflow` became an
  always-empty table. A `BEFORE INSERT` trigger drops a row whose key is taken
  (`RAISE(IGNORE)`). An `AFTER INSERT` trigger does the work, then deletes the
  row. This stays a single atomic statement.
- `INDEXED BY event_call` (an index on an expression, with a WHERE clause)
  fails to plan ("no query solution"): the hint is dropped for Turso.
- The JDBC URL takes no parameters, so `busy_timeout` and `cache_size` are set
  with PRAGMAs on each new connection. `mmap_size` doesn't exist.

Bugs in the JDBC driver (0.8.3), worked around in `pool.clj`:

- `PreparedStatement.execute()` + `getResultSet()` **loses the first row**:
  `execute()` steps the cursor itself. next.jdbc uses `execute()`, so plain
  next.jdbc silently returns wrong results. The workaround uses
  `executeQuery()` for SQL that returns rows, and `executeUpdate()` otherwise.
- A prepared statement **cannot be run twice**: it is never reset, so the
  second query returns no rows and the second update does nothing, silently.
  Statement caching is off for Turso.
- `ResultSetMetaData.getTableName` throws, so next.jdbc's default
  (table-qualified) row builder fails. tenon uses unqualified builders; four
  tests were changed to do the same.

Two of these bugs return wrong data without raising an error. That is a
correctness risk on top of the speed gap.

## What works fine

WAL, `BEGIN IMMEDIATE`, `STRICT` tables, partial indexes, `INDEXED BY` on
plain partial indexes, recursive CTEs, `json_each`, `unixepoch('subsec')`,
`RETURNING`, `RAISE`, `AFTER` triggers, and several pools on one file (the
concurrent-init test passes).
