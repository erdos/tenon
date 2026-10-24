# tenon storage benchmark: SQLite and PostgreSQL

- 4 CPUs, JVM 21.0.12.1, PostgreSQL 18.6 (embedded, same machine, over TCP), SQLite 3.53.4
- durable commits on both: SQLite WAL + synchronous=FULL, PostgreSQL fsync + synchronous_commit=on;
  PostgreSQL with async commit (not durable) for reference

## Writes (operations per second, latency per operation)

| | SQLite | PostgreSQL | PostgreSQL, async commit |
|---|---:|---:|---:|
| start + finish a workflow, 1 thread | 2,170/s, p50 462 us, p99 1.2 ms | 875/s, p50 1.1 ms, p99 2.2 ms | 2,765/s, p50 337 us, p99 751 us |
| start + finish a workflow, 16 threads | 7,660/s, p50 1.8 ms, p99 6.3 ms | 3,569/s, p50 3.4 ms, p99 25.0 ms | 7,808/s, p50 1.6 ms, p99 10.4 ms |
| start + finish a workflow, 64 threads | 14,538/s, p50 3.8 ms, p99 11.8 ms | 6,411/s, p50 8.8 ms, p99 25.2 ms | 10,884/s, p50 5.1 ms, p99 17.0 ms |
| a workflow calling 3, reusing 1 (10 writes), 16 threads | 1,804/s, p50 8.3 ms, p99 29.0 ms | 875/s, p50 16.2 ms, p99 43.7 ms | 1,730/s, p50 8.9 ms, p99 16.5 ms |

## The engine, end to end (calls per second)

| | SQLite | PostgreSQL |
|---|---:|---:|
| engine: a nested call (4 storage calls), 16 threads | 2,507/s, p50 5.7 ms, p99 17.6 ms | 1,956/s, p50 7.2 ms, p99 26.2 ms |
| engine: a call reusing a stored result (1 storage call), 16 threads | 11,945/s, p50 1.2 ms, p99 4.9 ms | 7,043/s, p50 1.9 ms, p99 7.8 ms |

## Reads, among 249,850 workflows (100,000 top-level)

| | SQLite | PostgreSQL |
|---|---:|---:|
| writing them, 16 threads (writes per second) | 13,432/s (38 s) | 8,725/s (59 s) |
| get-workflow, 1 thread | 74,363/s, p50 12 us, p99 36 us | 9,924/s, p50 92 us, p99 201 us |
| get-workflow, 16 threads | 168,164/s, p50 11 us, p99 43 us | 30,458/s, p50 133 us, p99 7.3 ms |
| find-by-wf-def-and-params, 16 threads | 141,768/s, p50 10 us, p99 2.3 ms | 49,733/s, p50 74 us, p99 6.1 ms |
| listing: first page (100) | 2,559/s, p50 354 us, p99 1.5 ms | 722/s, p50 1.2 ms, p99 4.9 ms |
| listing: a page in the middle (100) | 3,039/s, p50 312 us, p99 571 us | 787/s, p50 1.2 ms, p99 3.2 ms |
| listing: ERROR, top level (100) | 304/s, p50 3.2 ms, p99 4.7 ms | 151/s, p50 6.3 ms, p99 17.6 ms |
| listing: a nested wf_def, all levels (100) | 1,820/s, p50 492 us, p99 1.6 ms | 785/s, p50 1.2 ms, p99 3.2 ms |
| full-timeline of a workflow and its calls | 22,140/s, p50 41 us, p99 95 us | 3,987/s, p50 234 us, p99 536 us |
| extend-lease! of 500 invocations | 588/s, p50 1.6 ms, p99 2.6 ms | 292/s, p50 3.2 ms, p99 5.6 ms |
| size on disk (tables and indexes) | 43 MB, 182 B per workflow | 134 MB, 563 B per workflow (19 MB of it free, left by updates for later writes) |
| WAL written per workflow | - | 1,056 B |

## Reading the numbers

Measured with `./run bench 100000` in a cloud container: 4 vCPUs shared by the JVM and the PostgreSQL
server, one ext4 disk whose `fdatasync` takes ~190 us (`pg_test_fsync`). Both storages as the library
configures them: pools of 10 connections; PostgreSQL with 4 group-commit lanes. The server ran with
its defaults but for durability (the embedded server's test defaults turn it off) and
`shared_buffers=512MB`. Run to run, the write numbers vary by 10-25%.

- **Writes.** A durable commit costs one WAL flush on both, ~200-300 us here: one thread starting and
  finishing workflows is bound by it on both. On top of that PostgreSQL spends ~130-170 us per
  statement (a round trip, and the work of the statement) where SQLite spends ~40 us in-process:
  without the flush (async commit), PostgreSQL runs about as fast as durable SQLite.
- **Group commit** (`tenon.workflow.postgres.group-commit`): concurrent writes share a transaction, and
  so a flush. It pays where there are more writers than connections: at 64 threads it takes
  PostgreSQL from ~3,700/s to ~6,400/s, and its p99 from ~100 ms to ~25 ms. Where every writer has a
  connection of its own, it is level with none - PostgreSQL then shares flushes between concurrent
  commits by itself - and needs 4 connections for writing rather than one per writer. One lane would
  be slower than none: a lane runs its statements one after the other. The ceiling left is this
  machine's CPUs, shared with the clients.
- **The engine end to end.** A call of a workflow is one storage call (`call!`): it starts the
  workflow or logs the reuse of its outcome - before, a lookup, then an insert or a lookup and a
  reuse. A reuse of a stored result is ~1.9x what it was (7,000/s against 3,800/s), ~60% of SQLite's.
  A nested call, whose cost is its four durable writes rather than the lookups it lost, gained little.
- **Reads** are a round trip to a server against a function call: a point read is ~100 us against
  ~12 us, a page of the listing ~1.2 ms against ~0.35 ms, a timeline ~0.24 ms against ~0.04 ms. None
  scans a table: each is bounded by the rows it returns (see `postgres-test/no-table-scans-test`),
  apart from the listing of ERROR workflows, which - as in SQLite - looks at every failed workflow,
  and so costs more the more of them there are.
- **Space.** PostgreSQL rows carry a 23-byte header and fixed-width integers, SQLite's variable-length
  ones: 3x the size. Of PostgreSQL's, a part is the free space the updated workflow rows leave in their
  pages until VACUUM makes it reusable. A workflow writes ~1.1 kB of WAL.
