(ns tenon.workflow.bench
  "Benchmarks of the Storage implementations through the same protocol calls.

     clojure -M:bench -m tenon.workflow.bench sqlite:/path/to/file.db out.edn
     clojure -M:bench -m tenon.workflow.bench redis://localhost:6379 out.edn

   Closed-loop load: each of n threads issues one operation after another for
   a fixed time, after a warm-up. Prints one line per scenario, and writes
   them to out.edn. The data set is built first (see populate!)."
  (:require [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [taoensso.carmine :as car]
            [tenon.workflow.db :as db]
            [tenon.workflow.redis :as redis]
            [tenon.workflow.sqlite :as sqlite])
  (:import [java.util.concurrent.atomic AtomicLong]))

(def ^:private n-workflows 50000)
(def ^:private warmup-ms 1000)
(def ^:private run-ms 3000)

(defn- open [target]
  (if (.startsWith ^String target "sqlite:")
    (db/init-db! (sqlite/datasource (subs target 7)))
    (db/init-db! (redis/datasource {:uri target :prefix "bench" :pool-size 64}))))

;;; Load generator

(defn- percentile [^longs sorted p]
  (let [n (alength sorted)]
    (if (zero? n) 0 (aget sorted (min (dec n) (long (* p n)))))))

(defn- load!
  "Runs (op thread-index) on `threads` threads for ms. Returns the number of
   ops and the sorted latencies (ns)."
  [threads ms op]
  (let [deadline (+ (System/nanoTime) (* ms 1000000))
        runs (mapv (fn [t]
                     (future
                       (let [lat (long-array 1000000)]
                         (loop [i 0]
                           (let [t0 (System/nanoTime)]
                             (if (and (< t0 deadline) (< i (alength lat)))
                               (do (op t)
                                   (aset lat i (- (System/nanoTime) t0))
                                   (recur (inc i)))
                               [lat i]))))))
                   (range threads))
        parts (mapv deref runs)
        total (reduce + (map second parts))
        all (long-array total)]
    (reduce (fn [pos [^longs lat n]] (System/arraycopy lat 0 all pos n) (+ pos n)) 0 parts)
    (java.util.Arrays/sort all)
    {:ops total :lat all}))

(defn- scenario
  "Times op under `threads` threads. Prints and returns the result."
  [results label threads op]
  (load! threads warmup-ms op)
  (let [t0 (System/nanoTime)
        {:keys [ops lat]} (load! threads run-ms op)
        secs (/ (- (System/nanoTime) t0) 1e9)
        r {:scenario label :threads threads
           :ops-per-s (Math/round (/ ops secs))
           :p50-us (/ (percentile lat 0.5) 1000.0)
           :p99-us (/ (percentile lat 0.99) 1000.0)}]
    (printf "%-34s %2d thr %9d ops/s   p50 %9.1f us   p99 %9.1f us%n" label threads (:ops-per-s r) (:p50-us r) (:p99-us r))
    (flush)
    (swap! results conj r)
    r))

;;; Data set

(def ^:private wf-defs (mapv #(str "bench/w" %) (range 5)))

(defn- populate!
  "n-workflows top-level workflows over 5 wf-defs: 1% left STARTED (leased
   for 10 minutes), 5% ERROR, the rest DONE; every 10th has a nested DONE
   child, every 7th is reused once. Returns the invocation ids of the
   top-level ones, in order."
  [ds]
  (let [ids (object-array n-workflows)
        next-i (AtomicLong. 0)
        workers (mapv (fn [_]
                        (future
                          (loop []
                            (let [i (.getAndIncrement next-i)]
                              (when (< i n-workflows)
                                (let [id (db/insert-workflow! ds (wf-defs (mod i 5)) (str "[" i "]") nil
                                                              "{:actor 1}" (when (zero? (mod i 100)) 600000))]
                                  (aset ids i id)
                                  (cond
                                    (zero? (mod i 100)) nil
                                    (= 1 (mod i 20)) (db/finish! ds id "ERROR" "{:message \"boom\"}")
                                    :else (db/finish! ds id "DONE" (str "{:result " i "}")))
                                  (when (zero? (mod i 10))
                                    (let [c (db/insert-workflow! ds "bench/child" (str "[" i "]") id nil nil)]
                                      (db/finish! ds c "DONE" "42")))
                                  (when (zero? (mod i 7))
                                    (db/record-reuse! ds id nil nil)))
                                (recur))))))
                      (range 16))]
    (run! deref workers)
    (vec ids)))

(defn- footprint [target]
  (if (.startsWith ^String target "sqlite:")
    (let [p (subs target 7)]
      {:disk-bytes (+ (.length (io/file p)) (.length (io/file (str p "-wal"))))})
    (let [info (car/wcar {:pool {} :spec {:uri target}} (car/info "memory"))
          used (some->> (re-find #"used_memory:(\d+)" info) second parse-long)]
      {:redis-used-memory-bytes used})))

;;; Scenarios

(defn- ping-us [target]
  (when-not (.startsWith ^String target "sqlite:")
    (let [c {:pool {} :spec {:uri target}}]
      (dotimes [_ 2000] (car/wcar c (car/ping)))
      (let [lat (long-array 5000)]
        (dotimes [i 5000]
          (let [t0 (System/nanoTime)] (car/wcar c (car/ping)) (aset lat i (- (System/nanoTime) t0))))
        (java.util.Arrays/sort lat)
        (/ (percentile lat 0.5) 1000.0)))))

(defn -main [target out]
  (let [ds (open target)
        results (atom [])
        counter (AtomicLong. (System/nanoTime))
        fresh (fn [] (str "[" (.incrementAndGet counter) "]"))
        before (footprint target)
        t0 (System/nanoTime)
        ids (populate! ds)
        populate-s (/ (- (System/nanoTime) t0) 1e9)
        after (footprint target)
        rnd (fn [] (nth ids (rand-int (count ids))))]
    (println "target" target "populated" (count ids) "workflows in" (format "%.1f s" populate-s) after)
    (when-let [p (ping-us target)] (println "redis PING p50 us" p))

    (doseq [n [1 4 16]]
      (scenario results "get-workflow" n (fn [_] (db/get-workflow ds (rnd)))))
    (doseq [n [1 4 16]]
      (scenario results "find-by-wf-def-and-params" n
                (fn [_] (let [i (rand-int n-workflows)] (db/find-by-wf-def-and-params ds (wf-defs (mod i 5)) (str "[" i "]"))))))
    (doseq [[label filters] [["list page (limit 50)" {:limit 50}]
                             ["list page, all calls" {:limit 50 :top-level-only? false}]
                             ["list page, wf-def" {:limit 50 :wf-def "bench/w2"}]
                             ["list page, state=DONE" {:limit 50 :state "DONE"}]
                             ["list page, state=ERROR" {:limit 50 :state "ERROR"}]
                             ["list all STARTED (~1%)" {:state "STARTED" :top-level-only? false}]
                             ["list deep page (before mid)" {:limit 50 :before (nth ids (quot n-workflows 2))}]]]
      (doseq [n [1 4]]
        (scenario results label n (fn [_] (db/top-level-workflows ds filters)))))
    (doseq [n [1 4]]
      (scenario results "full-timeline (nested)" n
                (fn [_] (db/full-timeline ds (nth ids (* 10 (rand-int (quot n-workflows 10))))))))
    (let [leased (vec (filter some? (map #(nth ids (* 100 %)) (range (quot n-workflows 100)))))
          batch (vec (take 500 leased))]
      (scenario results "extend-lease! (500 leased)" 1 (fn [_] (db/extend-lease! ds batch 600000)))
      (scenario results "list-expired (500 running)" 1 (fn [_] (db/list-expired ds))))
    ;; Writes last: they leave STARTED workflows and a longer log behind.
    (doseq [n [1 4 16]]
      (scenario results "insert-workflow!" n
                (fn [_] (db/insert-workflow! ds "bench/insert" (fresh) nil "{:actor 1}" 60000))))
    (doseq [n [1 4 16]]
      (scenario results "insert + finish! (a run)" n
                (fn [_] (let [id (db/insert-workflow! ds "bench/run" (fresh) nil "{:actor 1}" 60000)]
                          (db/finish! ds id "DONE" "{:result 1}")))))
    (doseq [n [1 4 16]]
      (scenario results "nested run: root+child+reuse" n
                (fn [_] (let [root (db/insert-workflow! ds "bench/root" (fresh) nil "{:actor 1}" 60000)
                              child (db/insert-workflow! ds "bench/kid" (fresh) root nil 60000)]
                          (db/finish! ds child "DONE" "1")
                          (db/record-reuse! ds child root nil)
                          (db/finish! ds root "DONE" "2")))))
    (doseq [n [1 4 16]]
      (scenario results "mixed 90% get / 10% run" n
                (fn [_] (if (< (rand) 0.9)
                          (db/get-workflow ds (rnd))
                          (let [id (db/insert-workflow! ds "bench/mixed" (fresh) nil nil 60000)]
                            (db/finish! ds id "DONE" "1"))))))
    (spit out (pr-str {:target target :populate-s populate-s :footprint-before before :footprint-after after
                       :ping-us (ping-us target) :results @results}))
    (.close ^java.io.Closeable ds)
    (shutdown-agents)))
