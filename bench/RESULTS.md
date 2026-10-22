# SQLite vs Redis storage benchmarks

Redis numbers are for the second data model (see below); the first is in `bench/results/redis-v1/`.

Machine: 4 cores / 16 GB, disk fsync ≈ 0.3 ms. Load generator, Redis and SQLite share the 4 cores;
Redis is reached over localhost (PING p50 ≈ 60 µs). Data set: 50k top-level workflows (1% STARTED,
5% ERROR, rest DONE), nested children and reuses. Closed loop, 3 s per point after 1 s warm-up,
one run per point (noise is real: e.g. sqlite-disk get-workflow at 16 threads is an outlier).
Cells: ops/s, p50 µs / p99 µs. Raw data: `bench/results/*.edn`. Reproduce: `clojure -M:bench -m tenon.workflow.bench <sqlite:PATH | redis://host:port> out.edn`.

Configs: sqlite-disk (WAL, synchronous FULL: durable), sqlite-shm (tmpfs: no fsync cost),
redis-mem (no persistence), redis-aof-everysec, redis-aof-always (fsync per write: durable).

| scenario | thr | sqlite-disk | sqlite-shm | redis-mem | redis-aof-everysec | redis-aof-always |
|---|--:|---|---|---|---|---|
| insert-workflow! | 1 | 2593 · 328/999 | 13899 · 52/262 | 6109 · 148/304 | 4647 · 193/438 | 1703 · 545/1520 |
| insert-workflow! | 16 | 2319 · 374/254863 | 8548 · 103/31906 | 17054 · 878/2148 | 12324 · 1202/3193 | 5478 · 2527/9573 |
| insert + finish! (a run) | 1 | 1350 · 633/3429 | 8977 · 83/560 | 3599 · 251/593 | 3208 · 283/641 | 993 · 934/2634 |
| insert + finish! (a run) | 16 | 1203 · 762/523688 | 4726 · 190/62958 | 8219 · 1824/4514 | 7710 · 1908/4909 | 3829 · 3930/7988 |
| nested run: root+child+reuse | 16 | 444 · 2001/1095572 | 2155 · 429/158388 | 3767 · 3874/8600 | 3279 · 4567/10552 | 1467 · 10692/16094 |
| get-workflow | 1 | 31259 · 23/89 | 36214 · 22/77 | 9604 · 92/251 | 10100 · 89/215 | 9966 · 88/231 |
| get-workflow | 4 | 138000 · 22/92 | 140388 · 22/97 | 25982 · 146/315 | 25394 · 146/333 | 25584 · 147/340 |
| find-by-wf-def-and-params | 4 | 148322 · 21/93 | 145572 · 22/93 | 23226 · 148/428 | 23094 · 149/432 | 23751 · 147/406 |
| mixed 90% get / 10% run | 16 | 11831 · 39/18697 | 20835 · 46/11230 | 24571 · 552/1967 | 22944 · 601/2049 | 11807 · 1214/3852 |
| list page (limit 50) | 1 | 2070 · 418/900 | 2161 · 413/863 | 1153 · 809/1574 | 1070 · 854/2445 | 1051 · 887/2246 |
| list page (limit 50) | 4 | 6958 · 490/1019 | 6440 · 512/3708 | 2656 · 1394/3328 | 2467 · 1444/4432 | 2482 · 1420/4175 |
| list page, state=ERROR | 1 | 554 · 1691/3329 | 547 · 1720/3487 | 936 · 995/2420 | 937 · 1000/1872 | 719 · 1248/3949 |
| list all STARTED (~1%) | 1 | 200 · 4693/9215 | 188 · 4761/9493 | 89 · 10645/17148 | 80 · 11552/19339 | 78 · 12248/20303 |
| full-timeline (nested) | 1 | 2830 · 297/720 | 3625 · 240/594 | 6710 · 134/315 | 6337 · 139/394 | 6274 · 142/377 |
| extend-lease! (500 leased) | 1 | 78 · 14088/22426 | 261 · 3857/7105 | 242 · 3802/7554 | 178 · 5037/9494 | 143 · 6315/12955 |
| list-expired (500 running) | 1 | 2572 · 360/693 | 2427 · 395/659 | 11014 · 80/220 | 10934 · 80/215 | 11092 · 80/194 |

Footprint after loading (50k workflows, ~65k events): SQLite file 14.7 MB; Redis used_memory 66 MB (68 MB with the first model).

## Redis data model v1 (SQLite's tables ported) vs v2 (Redis-native), redis-mem, ops/s

Point reads, inserts and timelines are within noise (v2 +0–10%): they are bound by the network round trip
and by the number of index writes. Listings gain the most: list page +30% (1 thread) to +50% (4 threads),
state=ERROR page 150 → 936, list all STARTED 72 → 89. Cause: a row is one HMGET of the workflow's hash instead of
five lookups, and rare states touch only workflows that were reused (an index per state).
