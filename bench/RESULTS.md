# tenon storage benchmark: SQLite and PostgreSQL

- 4 CPUs, JVM 21.0.12.1, PostgreSQL 18.6 (embedded, same machine, over TCP), SQLite 3.53.4
- durable commits on both: SQLite WAL + synchronous=FULL, PostgreSQL fsync + synchronous_commit=on;
  PostgreSQL with async commit (not durable) for reference

## Writes (operations per second, latency per operation)

| | SQLite | PostgreSQL | PostgreSQL, async commit |
|---|---:|---:|---:|
| start + finish a workflow, 1 thread | 2,228/s, p50 408 us, p99 1.2 ms | 868/s, p50 1.1 ms, p99 2.2 ms | 2,418/s, p50 345 us, p99 839 us |
| start + finish a workflow, 16 threads | 7,760/s, p50 1.7 ms, p99 9.0 ms | 4,311/s, p50 3.2 ms, p99 10.8 ms | 10,016/s, p50 1.1 ms, p99 8.3 ms |
| a workflow calling 3, reusing 1 (10 writes), 16 threads | 1,737/s, p50 8.4 ms, p99 16.8 ms | 1,004/s, p50 14.8 ms, p99 32.6 ms | 1,879/s, p50 6.7 ms, p99 24.0 ms |

## The engine, end to end (calls per second)

| | SQLite | PostgreSQL |
|---|---:|---:|
| engine: a nested call (6 storage calls), 16 threads | 2,516/s, p50 5.7 ms, p99 16.6 ms | 1,867/s, p50 6.8 ms, p99 33.7 ms |
| engine: a call reusing a stored result (2 storage calls), 16 threads | 8,556/s, p50 1.5 ms, p99 7.3 ms | 3,824/s, p50 3.4 ms, p99 11.0 ms |

## Reads, among 249,850 workflows (100,000 top-level)

| | SQLite | PostgreSQL |
|---|---:|---:|
| writing them, 16 threads (writes per second) | 13,127/s (39 s) | 8,472/s (61 s) |
| get-workflow, 1 thread | 74,746/s, p50 12 us, p99 35 us | 9,454/s, p50 96 us, p99 220 us |
| get-workflow, 16 threads | 154,024/s, p50 12 us, p99 520 us | 29,862/s, p50 288 us, p99 4.2 ms |
| find-by-wf-def-and-params, 16 threads | 205,467/s, p50 8 us, p99 1.4 ms | 48,911/s, p50 148 us, p99 2.1 ms |
| listing: first page (100) | 2,200/s, p50 371 us, p99 846 us | 774/s, p50 1.2 ms, p99 4.1 ms |
| listing: a page in the middle (100) | 2,706/s, p50 332 us, p99 690 us | 766/s, p50 1.2 ms, p99 3.2 ms |
| listing: ERROR, top level (100) | 361/s, p50 2.7 ms, p99 5.1 ms | 151/s, p50 6.4 ms, p99 13.5 ms |
| listing: a nested wf_def, all levels (100) | 2,135/s, p50 448 us, p99 804 us | 811/s, p50 1.2 ms, p99 2.9 ms |
| full-timeline of a workflow and its calls | 22,816/s, p50 40 us, p99 90 us | 3,829/s, p50 238 us, p99 544 us |
| extend-lease! of 500 invocations | 582/s, p50 1.6 ms, p99 4.2 ms | 266/s, p50 3.3 ms, p99 10.4 ms |
| size on disk (tables and indexes) | 43 MB, 182 B per workflow | 138 MB, 580 B per workflow (11 MB of it free, left by updates for later writes) |
| WAL written per workflow | - | 1,109 B |

## Reading the numbers

Measured with `./run bench 100000` in a cloud container: 4 vCPUs shared by the JVM and the PostgreSQL
server, one ext4 disk whose `fdatasync` takes ~190 us (`pg_test_fsync`). PostgreSQL ran with its
defaults but for durability (the embedded server's test defaults turn it off) and `shared_buffers=512MB`.
Run to run, the write numbers vary by about 10-20%.

- **Writes.** A durable commit costs one WAL flush on both, ~200-300 us here: one thread starting and
  finishing workflows is bound by it on both. On top of that PostgreSQL spends ~130-170 us per
  statement (a round trip, and the work of the statement) where SQLite spends ~40 us in-process:
  without the flush (async commit), PostgreSQL runs about as fast as durable SQLite. Under concurrency, SQLite's own group commit
  (one writer runs everyone's queued writes in one transaction, one flush) does better than PostgreSQL's
  flushing per commit: ~55-60% of its throughput. PostgreSQL's `commit_delay` did not change that here.
- **The engine end to end**: a nested call, where the function and the engine's bookkeeping take part of
  the time, runs at ~75% of SQLite's rate. A reuse of a stored result is two cheap statements, so the
  round trips weigh more: ~45%. (Before the call and reuse lists were replaced by indexes - each reuse of
  one popular workflow locked its row until the commit was flushed - it was ~15%.)
- **Reads** are a round trip to a server against a function call: a point read is ~100 us against
  ~12 us, a page of the listing ~1.2 ms against ~0.4 ms, a timeline ~0.24 ms against ~0.04 ms. None scans
  a table: each is
  bounded by the rows it returns (see `postgres-test/no-table-scans-test`), apart from the listing of
  ERROR workflows, which - as in SQLite - looks at every failed workflow, and so costs more the more
  of them there are.
- **Space.** PostgreSQL rows carry a 23-byte header and fixed-width integers, SQLite's variable-length
  ones: 3x the size. Of PostgreSQL's, a part is the free space the updated workflow rows leave in their
  pages until VACUUM makes it reusable. A workflow writes ~1.1 kB of WAL.
