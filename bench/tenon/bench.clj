(ns tenon.bench
  "Storage benchmark: the same workload against whichever backend
   tenon.workflow.sqlite/backend selects. Run each backend in its own JVM:
     clojure -M:bench -m tenon.bench
     clojure -M:turso:bench -m tenon.bench"
  (:require [tenon.workflow :as engine]
            [tenon.workflow.sqlite :as sqlite])
  (:import [java.io File]
           [java.util.concurrent Executors TimeUnit]))

#workflow
(defn leaf [i]
  (when (zero? (mod i 100)) (throw (ex-info "every 100th fails" {:i i})))
  {:i i})

#workflow
(defn node [tree depth i]
  (if (zero? depth)
    i
    (reduce + (map #(node tree (dec depth) (+ (* 3 i) %)) (range 3)))))

(defn- percentile [^longs sorted p]
  (/ (aget sorted (min (dec (alength sorted)) (long (* p (alength sorted))))) 1e6))

(defn- measure
  "Calls (f i) for i in 0..n-1 on one thread, returning stats."
  [label n f]
  (let [lat (long-array n)
        t0 (System/nanoTime)]
    (dotimes [i n]
      (let [s (System/nanoTime)]
        (f i)
        (aset lat i (- (System/nanoTime) s))))
    (let [total (/ (- (System/nanoTime) t0) 1e9)]
      (java.util.Arrays/sort lat)
      {:label label :n n :seconds total :ops-per-s (/ n total)
       :p50-ms (percentile lat 0.5) :p99-ms (percentile lat 0.99)})))

(defn- measure-concurrent [label threads per-thread f]
  (let [pool (Executors/newFixedThreadPool threads)
        t0 (System/nanoTime)
        futs (mapv (fn [t] (.submit pool ^Callable (fn [] (dotimes [i per-thread] (f t i)))))
                   (range threads))]
    (run! #(.get ^java.util.concurrent.Future %) futs)
    (.shutdown pool) (.awaitTermination pool 1 TimeUnit/MINUTES)
    (let [total (/ (- (System/nanoTime) t0) 1e9) n (* threads per-thread)]
      {:label label :n n :seconds total :ops-per-s (/ n total)})))

(defn- call [f & args]
  (try (apply f args) (catch clojure.lang.ExceptionInfo _ :failed)))

(defn -main [& [scale]]
  (let [scale (Double/parseDouble (or scale "1"))
        n #(max 1 (long (* scale %)))
        path (str (System/getProperty "java.io.tmpdir") "/tenon-bench-" sqlite/backend "-" (System/nanoTime) ".db")
        eng (engine/init path)
        results (atom [])
        run! (fn [r] (println (format "%-34s n=%-6d %8.2f s %10.1f ops/s%s" (:label r) (:n r) (double (:seconds r)) (double (:ops-per-s r))
                                      (if (:p50-ms r) (format "  p50 %.3f ms  p99 %.3f ms" (:p50-ms r) (:p99-ms r)) "")))
               (swap! results conj r))]
    (println "backend:" sqlite/backend "db:" path)
    (alter-var-root #'engine/*workflow-engine* (constantly eng))
    ;; warm-up (JIT, statement caches), on args not used below
    (dotimes [i (n 500)] (call leaf (- -1 i)))
    (run! (measure "new workflow (1 thread)" (n 3000) #(call leaf %)))
    (run! (measure-concurrent "new workflow (16 threads)" 16 (n 500) (fn [t i] (call leaf (+ 1000000 (* t 100000) i)))))
    (run! (measure "reuse finished result" (n 3000) #(call leaf %)))
    (run! (measure "nested tree, 364 workflows each" (n 10) #(node % 5 0)))
    (let [roots (mapv #(:workflow/invocation-id (engine/get-invocation %))
                      (keep :workflow/invocation-id (engine/list-invocations {:wf-def "tenon.bench/node" :limit 10})))
          max-id (reduce max 1 (map :workflow/invocation-id (engine/list-invocations {:limit 1})))
          rnd (java.util.Random. 42)]
      (run! (measure "get-invocation (random id)" (n 5000) (fn [_] (engine/get-invocation (inc (.nextInt rnd (int max-id)))))))
      (run! (measure "list-invocations page of 100" (n 200) (fn [_] (engine/list-invocations {:limit 100}))))
      (run! (measure "list-invocations state=ERROR" (n 200) (fn [_] (engine/list-invocations {:state "ERROR" :limit 100}))))
      (run! (measure "list-invocations by wf-def" (n 200) (fn [_] (engine/list-invocations {:wf-def "tenon.bench/leaf" :limit 100}))))
      (run! (measure "full-timeline (364-node tree)" (n 50) (fn [i] (engine/full-timeline (nth roots (mod i (count roots))))))))
    (engine/close! eng)
    (let [size (reduce + (map #(.length (File. (str path %))) ["" "-wal"]))]
      (println (format "db file + wal: %.1f MB" (/ size 1048576.0))))
    (spit (str "bench-" sqlite/backend ".edn") (pr-str @results))
    (shutdown-agents)
    (System/exit 0)))
