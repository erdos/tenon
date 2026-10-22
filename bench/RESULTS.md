# SQLite vs Redis storage benchmarks

Machine: 4 cores / 16 GB, disk fsync ≈ 0.3 ms. Load generator, Redis and SQLite share the 4 cores;
Redis is reached over localhost (PING p50 ≈ 60 µs). Data set: 50k top-level workflows (1% STARTED,
5% ERROR, rest DONE), nested children and reuses. Closed loop, 3 s per point after 1 s warm-up,
one run per point (noise is real: e.g. sqlite-disk get-workflow at 16 threads is an outlier).
Cells: ops/s, p50 µs / p99 µs. Raw data: `bench/results/*.edn`. Reproduce: `clojure -M:bench -m tenon.workflow.bench <sqlite:PATH | redis://host:port> out.edn`.

Configs: sqlite-disk (WAL, synchronous FULL: durable), sqlite-shm (tmpfs: no fsync cost),
redis-mem (no persistence), redis-aof-everysec, redis-aof-always (fsync per write: durable).

| scenario | thr | sqlite-disk | sqlite-shm | redis-mem | redis aof-everysec | redis aof-always |
|---|--:|---|---|---|---|---|
| insert-workflow! | 1 | 2593 · 328/999 | 13899 · 52/262 | 5917 · 151/412 | 4635 · 193/534 | 1578 · 564/1868 |
| insert-workflow! | 16 | 2319 · 374/254863 | 8548 · 103/31906 | 14163 · 990/3272 | 8796 · 1574/4417 | 5580 · 2530/7792 |
| insert + finish! (a run) | 1 | 1350 · 633/3429 | 8977 · 83/560 | 3630 · 244/666 | 2933 · 297/902 | 931 · 997/2889 |
| insert + finish! (a run) | 16 | 1203 · 762/523688 | 4726 · 190/62958 | 7511 · 1745/5720 | 5862 · 2450/8214 | 3698 · 4085/8509 |
| nested run (4 writes) | 16 | 444 · 2001/1095572 | 2155 · 429/158388 | 4279 · 3474/8952 | 2795 · 5411/10056 | 1420 · 10361/26906 |
| get-workflow | 1 | 31259 · 23/89 | 36214 · 22/77 | 8974 · 99/260 | 9742 · 92/239 | 9769 · 91/217 |
| get-workflow | 4 | 138000 · 22/92 | 140388 · 22/97 | 23630 · 158/351 | 24210 · 157/335 | 22220 · 163/462 |
| find-by-wf-def-and-params | 4 | 148322 · 21/93 | 145572 · 22/93 | 22371 · 155/455 | 23058 · 158/413 | 22899 · 154/422 |
| mixed 90% get / 10% run | 16 | 11831 · 39/18697 | 20835 · 46/11230 | 23431 · 591/1908 | 19624 · 694/2668 | 11147 · 1277/3955 |
| list page (limit 50) | 1 | 2070 · 418/900 | 2161 · 413/863 | 863 · 1046/2530 | 887 · 1000/2999 | 884 · 1007/2846 |
| list page, state=ERROR | 1 | 554 · 1691/3329 | 547 · 1720/3487 | 150 · 6211/11576 | 137 · 6678/12200 | 151 · 6203/10591 |
| list all STARTED (500) | 1 | 200 · 4693/9215 | 188 · 4761/9493 | 72 · 13115/23858 | 61 · 14819/29422 | 66 · 14197/25878 |
| full-timeline (nested) | 1 | 2830 · 297/720 | 3625 · 240/594 | 6515 · 136/345 | 5729 · 154/409 | 6365 · 142/334 |
| extend-lease! (500 ids) | 1 | 78 · 14088/22426 | 261 · 3857/7105 | 184 · 4905/8949 | 143 · 6128/13230 | 125 · 7139/13064 |
| list-expired (500 running) | 1 | 2572 · 360/693 | 2427 · 395/659 | 11258 · 76/245 | 10104 · 84/329 | 11147 · 78/259 |

Footprint after loading the 50k workflows (~65k more events): SQLite file 14.7 MB; Redis used_memory 68 MB.
