# Proposal: PostgreSQL storage backend

Status: draft · October 2026

## Summary

Add a PostgreSQL implementation of the `tenon.workflow.db/Storage` and `Timeouts` protocols, shipped as a separate module (`tenon/postgres`). Most teams that run Clojure services in production already have Postgres. Many cannot use a local SQLite file at all: they run several app instances, deploy to containers with no persistent disk, or need the database backed up and monitored like their other data.

The engine (`tenon.workflow`), the timeout logic (`tenon.workflow.timeout`) and the web UI (`http/`) talk to storage only through the protocols, so they need small changes only. The work is mostly:

1. Decouple `init` and the test suite from SQLite.
2. Write a Postgres schema and protocol implementation that keeps the same guarantees under real concurrency. SQLite gets those guarantees for free from its single writer.
3. Run the whole test suite against both backends in CI.

Estimated effort: about 3 to 4 weeks for one developer, done in phases that can each be merged on their own.

## Goals

- Every protocol method behaves as documented in `tenon.workflow.db`, verified by the existing contract tests and by the behavioral tests in `db_test.clj` and `workflow_test.clj`.
- Several processes on several hosts can share one database: deduplication, compare-and-set state changes, leases and sweeps stay correct.
- Users bring their own `javax.sql.DataSource` (HikariCP or whatever pool they already run). Tenon does not own or tune their pool.
- The core library stays free of the Postgres driver. The SQLite path keeps working unchanged: `(init "app.db")` stays valid.
- Performance is in the same range as SQLite for one process and scales with more processes.

## Non-goals (for the first version)

- Recording workflows inside the caller's own transaction. Tenon deliberately writes outside it: see `run-invocation-persists-despite-enclosing-transaction-rollback-test`.
- Other databases (MySQL, CockroachDB, YugabyteDB). The design avoids things that would rule them out where it's cheap, but they are not tested.
- Migrating existing SQLite data into Postgres. This could be a later export/import tool.
- New engine features (automatic retries, queues, timers). Those are separate proposals. Some of them get easier with Postgres; see *Follow-ups*.

## Current state: where SQLite leaks out

| Place | Coupling | Change needed |
|---|---|---|
| `tenon.workflow/init` | Always calls `sqlite/datasource` on a path string | Accept a storage instance as well as a path |
| `tenon.workflow/close!` | Calls `.close` on `:tenon/db` | Close only storage tenon created (see *Ownership*) |
| `tenon.workflow.sqlite` | Protocols are extended to the `Pool` type | None: Postgres gets its own type |
| `test/tenon/workflow/test_util.clj` | `temp-db-fixture` creates a SQLite file; `history` and `expire!` run raw SQLite SQL | A fixture per backend; move the raw SQL behind a test-only protocol |
| `db_test.clj`, `workflow_test.clj`, `http_test.clj` | About 40 uses of raw `jdbc/execute!` or `*db-path*` | Route through the test helpers above |
| `http/` | None (it only calls `tenon.workflow`) | None |

Engine code that the protocols already shield from storage, and that needs no change: deduplication with the race fallback (`insert-workflow!` returns nil → `find-by-wf-def-and-params`), compare-and-set finish, restart and time-out, the heartbeat (`extend-lease!`) and the sweep (`list-expired`).

## Guarantees SQLite gives for free, and how Postgres keeps them

This is the core of the proposal. The SQLite storage relies on SQLite running **one writer at a time**: every write statement sees the result of all earlier ones, and no two run interleaved. Postgres runs writers concurrently under `READ COMMITTED`, so every one of these has to be handled explicitly.

### 1. Idempotent insert

**SQLite:** an `INSERT` into the `new_workflow` view fires an `INSTEAD OF` trigger. It checks the 64-bit hash `key` plus the full `params` and does `RAISE(IGNORE)` if the workflow exists. This is safe only because nothing else writes in between. The `workflow_key` index is not unique, since 64-bit hashes can collide.

**Postgres:** a real unique constraint, with `INSERT … ON CONFLICT DO NOTHING RETURNING id`. Under concurrency, the second insert waits for the first transaction, then sees the conflict and returns no row. That is exactly the "nil means somebody else inserted it" contract the engine already handles.

`params` can be arbitrarily large, and a btree index entry is limited to about 2.7 KB, so the constraint is on a digest instead of the text:

```sql
params_digest bytea NOT NULL,          -- SHA-256 of params, computed in the JVM
UNIQUE (wf_def_id, params_digest)
```

SHA-256 collisions are not a practical concern, so the constraint alone decides uniqueness. `find-by-wf-def-and-params` still compares `params` as a safety net, as SQLite does. The digest is computed in Clojure (`java.security.MessageDigest`), so no extension is needed and the value is the same in tests and production.

### 2. Compare-and-set state changes

**SQLite:** `finish!`, `time-out!` and `restart!` are `INSERT INTO event … SELECT … WHERE w.state_id = i.id …`. A trigger then moves the workflow row's pointers. The `WHERE` is a correct check-and-set only because no other writer can change the row between the read and the write.

**Postgres:** in `READ COMMITTED`, two concurrent `INSERT … SELECT` statements can both read the old state and both insert. That would allow, for example, a late `DONE` racing a sweep's `TIMEOUT` (`late-finish-after-timeout-keeps-timeout-test`), or two restarts (`concurrent-restarts-of-one-workflow-restart-once-test`) both succeeding.

The fix is to make the **`UPDATE` of the workflow row the check**, and append the event from its `RETURNING`, in one statement. Postgres takes a row lock for the `UPDATE`. A concurrent `UPDATE` waits, then re-checks its `WHERE` against the new row version and matches nothing. That is a true compare-and-set without explicit locks or `SERIALIZABLE`:

```sql
-- finish! / time-out!   params: state, invocation-id, any-time?, data
WITH s AS (SELECT nextval('event_id_seq') AS id),
w AS (
  UPDATE workflow w
     SET state = $state, state_id = s.id, expires_at = NULL
    FROM s
   WHERE w.id = (SELECT workflow_id FROM event WHERE id = $invocation_id)
     AND w.invocation_id = $invocation_id
     AND w.state = 0                                   -- STARTED
     AND ($any_time OR w.expires_at <= {{now}})
  RETURNING w.id, w.state_id, w.invocation_id)
INSERT INTO event (id, workflow_id, state, occurred_at, data, invocation_id)
SELECT state_id, id, $state, {{now}}, $data, invocation_id FROM w
RETURNING id;
```

`restart!` follows the same shape. It sets `invocation_id = state_id = s.id` and the new lease, requires `state <> 0`, and appends a `STARTED` event. `nextval` is taken once in the `s` CTE: calling it twice in `SET` would hand out two different ids.

### 3. Ordering of ids

**SQLite:** a new invocation id is `max(id) + 1` taken under the write lock, so ids are handed out in commit order. That gives a total order that the listing's seek pagination (`:before`), `restart!` ("a new, larger invocation_id") and the timeline's `ORDER BY id` rely on.

**Postgres:** ids come from a sequence. `max(id) + 1` would collide under concurrent writers. A sequence is strictly increasing, but only in the order `nextval` is called, not in commit order. Consequences:

- **Restart gets a larger id:** still true.
- **Timeline order:** still correct for everything that is causally ordered. A workflow's own events (each change starts only after the previous one committed), and a child's events relative to its parent's (a child starts after the parent's `STARTED` committed, and finishes before the parent does). Only events of unrelated sibling workflows running in parallel can appear in an order that differs from commit order by milliseconds. That is harmless for display.
- **Pagination:** a row whose id is below the cursor can commit after a page was read, so someone paging backwards through the listing will not see it. Reloading the first page shows it. This only affects the UI's live view. **Proposal:** accept it and document it. A total commit order would need a single global lock (`pg_advisory_xact_lock` around every write), which would throw away Postgres's main advantage.

Gaps in the sequence (rolled back transactions, cached values) are fine: nothing relies on ids being dense.

### 4. Linked lists → indexes

**SQLite:** to avoid secondary indexes on `event`, events form linked lists. `prev_id` threads a workflow's states and its reuses, `prev_call_id` threads the calls a workflow made, and the heads live on the `workflow` row (`state_id`, `last_reuse_id`, `last_call_id`). Each new event reads the current head and moves it, which is safe only with one writer.

**Postgres:** keeping the lists would mean locking the head row on every append. That includes the **parent's** workflow row on every nested call, and a parent that calls children in parallel (e.g. `pmap`) would line them up behind one lock. It would also need a lock order (ascending ids) to avoid deadlocks between a reuse and its caller. **Proposal:** drop the lists in the Postgres schema and use ordinary indexes instead. Postgres handles concurrent index inserts well. Appending a `REUSED` event then touches no workflow row at all.

- `event (workflow_id, id)` replaces the state and reuse lists.
- `event (parent_invocation_id, id) WHERE wf_def_id IS NOT NULL` replaces the call list.

The workflow row keeps only `invocation_id`, `state`, `state_id` and `expires_at`, all of which change only under the compare-and-set `UPDATE` above (plus `extend-lease!`).

### 5. The storage clock

**SQLite:** `{{now}}` is `unixepoch('subsec') * 1000`, fixed for the duration of a statement.

**Postgres:** `(extract(epoch FROM statement_timestamp()) * 1000)::bigint`, which is also fixed per statement. `now()` would be fixed per *transaction*, and `clock_timestamp()` changes within a statement. Either would break the "every use within one statement yields the same value" rule. All processes share the primary's clock, as they share SQLite's file today.

### 6. Group commit

**SQLite:** `tenon.workflow.sqlite.pool/write!` batches concurrent writes into one transaction to pay for one fsync per batch.

**Postgres:** not needed. Postgres already lets many writers commit concurrently and groups their WAL flushes (`commit_delay`/`commit_siblings` can tune this). Every write here is a single autocommit statement, run on a connection borrowed from the user's `DataSource`.

### 7. Concurrent schema creation

**SQLite:** `BEGIN IMMEDIATE` so that one of several processes starting at once creates the schema.

**Postgres:** run the DDL in one transaction holding `pg_advisory_xact_lock(<constant>)`. Postgres DDL is transactional, so the other processes wait and then find the schema complete.

## Schema

All objects live in a configurable schema (`tenon` by default), so tenon's tables never clash with the application's.

```sql
CREATE SCHEMA IF NOT EXISTS tenon;

CREATE TABLE tenon.schema_version (version int NOT NULL);   -- one row; see Migrations

CREATE TABLE tenon.wf_def (
  id        int     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name      text    NOT NULL UNIQUE,
  top_level boolean NOT NULL DEFAULT false
);

CREATE SEQUENCE tenon.event_id_seq;                          -- invocation and event ids

CREATE TABLE tenon.workflow (
  id                   bigint   PRIMARY KEY,                 -- its first invocation id
  wf_def_id            int      NOT NULL,
  params_digest        bytea    NOT NULL,
  parent_invocation_id bigint,                               -- caller of its first invocation
  invocation_id        bigint   NOT NULL,                    -- current invocation
  state                smallint NOT NULL,                    -- 0 STARTED 1 DONE 2 ERROR 3 TIMEOUT
  state_id             bigint   NOT NULL,                    -- current state's event
  expires_at           bigint,                               -- lease end (epoch ms) while STARTED
  params               text     NOT NULL,
  UNIQUE (wf_def_id, params_digest)
);
CREATE INDEX workflow_not_done ON tenon.workflow (state, id) WHERE state <> 1;

CREATE TABLE tenon.event (
  id                   bigint   PRIMARY KEY,
  workflow_id          bigint   NOT NULL,
  state                smallint NOT NULL CHECK (state BETWEEN 0 AND 4),   -- 4 REUSED
  occurred_at          bigint   NOT NULL,                    -- epoch ms
  data                 text,                                 -- metadata or result (EDN)
  invocation_id        bigint,                               -- finished or reused invocation
  expires_at           bigint,                               -- STARTED: lease it started with
  parent_invocation_id bigint,                               -- calls: caller's invocation
  wf_def_id            int,                                  -- set on calls only
  CHECK ((invocation_id IS NULL) = (state = 0)),
  CHECK (expires_at IS NULL OR state = 0)
);
CREATE INDEX event_workflow   ON tenon.event (workflow_id, id);
CREATE INDEX event_by_caller  ON tenon.event (parent_invocation_id, id) WHERE wf_def_id IS NOT NULL;
CREATE INDEX event_top_calls  ON tenon.event (id) WHERE wf_def_id IS NOT NULL AND parent_invocation_id IS NULL;
CREATE INDEX event_def_calls  ON tenon.event (wf_def_id, (parent_invocation_id IS NULL), id)
                               WHERE wf_def_id IS NOT NULL;
```

Notes:

- **Timestamps stay `bigint` epoch milliseconds**, matching the protocol contract and SQLite. `timestamptz` would be friendlier for people querying the tables by hand, but every read would need converting. A read-only view with `timestamptz` columns can be added for operators.
- **Payloads stay EDN `text`**, not `jsonb`: they are EDN, and Postgres compresses large values automatically (TOAST).
- **No foreign keys** at first. Every write path is a single statement that cannot leave orphans, and foreign keys would add a lookup and a lock to every insert. They can be added later if they turn out to be wanted.
- **`wf_def` ids are cached in the JVM** (a `ConcurrentHashMap` from name to id and top-level flag). The first call of a name per process inserts it (`ON CONFLICT DO NOTHING`, then `SELECT`). The `top_level` flag is set once, with a conditional `UPDATE`, the first time a name is seen at the top level. This keeps every insert off the hot `wf_def` row, which an `ON CONFLICT DO UPDATE` would lock on every call.

## Method by method

| Method | Postgres implementation |
|---|---|
| `init-db!` | Advisory-locked DDL and migrations (see above). Returns the storage. |
| `insert-workflow!` | Resolve `wf_def_id` (cache). `WITH s AS (SELECT nextval(…)), w AS (INSERT INTO workflow … ON CONFLICT (wf_def_id, params_digest) DO NOTHING RETURNING …) INSERT INTO event … SELECT … FROM w RETURNING id`. Empty result → `nil`. |
| `finish!`, `time-out!` | The compare-and-set statement in section 2. |
| `restart!` | Compare-and-set `UPDATE … WHERE invocation_id = $id AND state <> 0`, new id from `s`, then append `STARTED`. |
| `record-reuse!` | Plain `INSERT INTO event … SELECT … FROM event i JOIN workflow w … WHERE i.id = $id AND i.state = 0`. No workflow row is touched. |
| `get-workflow` | Primary key lookups: `event` (by any invocation id) → `workflow` → its current events. `expires_at <= {{now}}` gives `:workflow/expired`. |
| `find-by-wf-def-and-params` | Lookup on the unique `(wf_def_id, params_digest)`, then compare `params`. |
| `top-level-workflows` | The same three query shapes as SQLite (by not-done state, by `wf_def`, unfiltered), using the partial indexes above and `ORDER BY id DESC LIMIT n` with an `id < $before` cursor. Planner hints are not available in Postgres: the plans have to be checked with `EXPLAIN` on a large data set (see *Performance*). |
| `top-level-wf-defs` | `SELECT name FROM wf_def WHERE top_level OR NOT $1 ORDER BY name`. |
| `full-timeline` | A recursive CTE: root workflow → its invocations (`event_workflow`, state 0) → calls under those (`event_by_caller`) → workflows first started by those calls → … Then every event of the tree's workflows (`event_workflow`), plus the root's `REUSED` events made from outside the tree, each tagged with its depth, `ORDER BY id`. Same rows and rules as the SQLite version, but it follows indexes instead of the linked lists. |
| `extend-lease!` | `UPDATE workflow SET expires_at = {{now}} + $t WHERE id IN (SELECT workflow_id FROM event WHERE id = ANY($ids)) AND invocation_id = ANY($ids) AND state = 0 AND expires_at IS NOT NULL RETURNING invocation_id`. `$ids` is a `bigint[]` (`Connection.createArrayOf`), so the statement is the same for any batch size. |
| `list-expired` | `… WHERE state = 0 AND state <> 1 AND expires_at <= {{now}} ORDER BY id` on `workflow_not_done`. |

Every statement is a single autocommit statement. Every connection is borrowed from the user's `DataSource` and returned straight away. No connection is held while a workflow runs or while a caller waits.

## Public API

```clojure
;; deps.edn (artifact coordinates to be decided)
{:deps {io.github.erdos/tenon          {:mvn/version "…"}
        io.github.erdos/tenon-postgres {:mvn/version "…"}
        org.postgresql/postgresql      {:mvn/version "…"}}}   ; the user picks the driver version

(require '[tenon.workflow :as wf]
         '[tenon.workflow.postgres :as pg])

;; with a DataSource the application already has (HikariCP, next.jdbc, …)
(def engine (wf/init (pg/storage my-datasource {:schema "tenon"})))

;; or a quick start from a JDBC URL - tenon creates and owns a small pool
(def engine (wf/init (pg/storage "jdbc:postgresql://db/app?user=…" {})))
```

Changes in `tenon.workflow`:

- `init` accepts either a string (SQLite path, as today) or anything that satisfies `db/Storage`. Detection is `(string? x)`, so no existing call changes.
- **Ownership.** `close!` closes the storage only if tenon created it. `pg/storage` given a `DataSource` returns a storage whose `close` does nothing to the user's pool. Given a URL, it closes the pool it created. The SQLite storage keeps closing its pool, as today.

Module layout, mirroring `http/`:

```
postgres/
  deps.edn                       ; tenon/tenon, next.jdbc; the driver as a test dependency only
  src/tenon/workflow/postgres.clj
  src/tenon/workflow/postgres/schema.clj   ; DDL + migrations
  test/tenon/workflow/postgres_test.clj
```

The core keeps `next.jdbc`. The Postgres module adds no dependency other than the driver, which the user chooses.

## Migrations

The SQLite schema has no version today, and has changed shape before (the append-only history rewrite). Postgres users will expect upgrades that don't lose data, so the module starts with versioning from day one:

- `tenon.schema_version` holds the current version.
- `init-db!` takes the advisory lock, reads the version, and runs the numbered migration steps after it, all in one transaction.
- A database *newer* than the library refuses to start, with a clear error. This protects against rolling back a deploy onto a migrated schema.
- An option (`:migrate? false`) lets teams that run their DDL through Flyway or Liquibase skip it. `pg/schema-sql` returns the DDL for them to copy.

The same mechanism should then be added to the SQLite backend with `PRAGMA user_version` (a separate, small change).

## Testing

The test suite is the main asset here: 211 tests, including targeted concurrency tests. The goal is for **all of them to run against both backends**.

1. **Backend-neutral fixtures.** `temp-db-fixture` becomes a fixture that builds a fresh storage for the backend picked by `TENON_TEST_BACKEND` (`sqlite` by default). For Postgres, each test gets a fresh schema (`CREATE SCHEMA test_<random>`, dropped afterwards), so tests can share one server and still run in isolation.
2. **Test-only inspection.** `test-util/history`, `expire!` and the other raw-SQL helpers move behind a small protocol (`tenon.workflow.test-storage/Inspect`) with one implementation per backend. Tests stop calling `jdbc/execute!` directly. This touches about 40 call sites.
3. **Contract tests.** `storage_contract.clj` already covers shapes. Add behavioral checks that are currently SQLite-only (compare-and-set, lease extension, reuse logging) so third-party storages get them too.
4. **Concurrency under Postgres.** The existing `concurrent-*` tests, plus new ones aimed at the risks above: two engines (separate pools) racing on insert, finish against time-out, two restarts, and parallel nested calls from one parent. Each runs many iterations in a loop.
5. **CI.** A GitHub Actions matrix: SQLite, plus Postgres in a service container, on the oldest and newest supported Postgres versions (14 and 18 at the time of writing). Locally, `./run test-pg` starts Postgres with Docker, or uses Testcontainers.

## Performance

Targets to check before release, on a laptop-class machine with Postgres on the same host:

- Overhead of one top-level call with no nesting (insert + finish): within 2× of SQLite with one process, and growing with more processes rather than staying flat.
- Listing and timeline queries: under 10 ms for a page of 50 on a database with 10 million events.
- `EXPLAIN (ANALYZE, BUFFERS)` of every listing shape checked into `postgres/bench/`, so plan regressions show up in review.

A small benchmark namespace (`bench/`), using the `fibonacci` fixture and a wide fan-out workflow, runs against both backends and prints calls per second.

## Rollout plan

| Phase | Content | Effort | Can merge on its own |
|---|---|---|---|
| 0 | `init` accepts a storage, ownership on `close!`, backend-neutral test fixtures and `Inspect` protocol (SQLite only) | 2–3 days | Yes: no behavior change |
| 1 | `postgres/` module: schema, `Storage` + `Timeouts`, contract tests green | 4–6 days | Yes, marked experimental |
| 2 | Full suite green on Postgres, new concurrency tests, CI matrix | 3–4 days | Yes |
| 3 | Benchmarks, `EXPLAIN` review, index tuning | 2–3 days | Yes |
| 4 | Migrations framework, docs, README section, release on Clojars | 2–3 days | Release |

Total: about 13–19 working days.

## Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A compare-and-set written as `INSERT … SELECT` instead of `UPDATE`-first slips in and allows double transitions | Medium | High: double finish, two restarts | Section 2 pattern for every state change; concurrency tests looping on a real server; code review checklist |
| Pagination misses late-committing rows | Certain, rare | Low: UI only | Documented; reload fixes it |
| Planner picks a bad plan for a listing shape at scale | Medium | Medium | `EXPLAIN` snapshots in the repo, benchmark at 10M events, partial indexes per shape |
| Many waiting callers poll the database (up to every 200 ms each) | Medium | Medium at scale | Fine at first; `LISTEN/NOTIFY` follow-up below |
| Users point tenon at a pool that is too small, and workflows stall on connections | Medium | Medium | Document sizing; tenon never holds a connection while running or waiting |
| Schema change later breaks existing installs | Low with migrations | High | Versioned migrations from the first release |

## Follow-ups this enables

- **Instant cross-process wakeups.** Today a caller waiting on a workflow run by another process polls with backoff. With Postgres, `finish!` can `NOTIFY tenon_finished, '<invocation-id>'`, and the engine's `with-backoff` already has a `wake` hook that a `LISTEN` connection can complete. This cuts both latency and polling load.
- **Work queue and automatic recovery.** `SELECT … FOR UPDATE SKIP LOCKED` on `workflow_not_done` is the standard way to let a pool of workers claim timed-out or scheduled workflows. It is the basis for automatic retries and for resuming work after a crash.
- **Retention.** Range-partitioning `event` by id, or a `prune!` that deletes finished trees older than N days, in batches.
- **Read replicas** for the UI's listing and timeline queries.

## Open questions

1. Separate artifact (`tenon-postgres`) or part of the main one with the driver optional? The proposal says separate, like `http`.
2. Keep `bigint` milliseconds, or move both backends to `timestamptz` / ISO strings in the protocol? The proposal keeps `bigint` for now.
3. Minimum Postgres version: 14 (the oldest still supported in late 2026, reaching end of life in November 2026) or 15?
4. Should the SQLite schema also drop the linked lists in favor of indexes, so both backends share one model? The proposal says no: the lists exist for SQLite performance, and SQLite has a single writer.
