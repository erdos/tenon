(ns tenon.workflow.bench
  "Benchmarks of the storages - SQLite and PostgreSQL - side by side: the
   same writes and reads against each, on the same machine and disk, and
   both durable: SQLite as tenon.workflow.sqlite configures it (WAL,
   synchronous=FULL, group commit), and an embedded PostgreSQL with fsync
   and synchronous_commit on - and for the writes, also with
   synchronous_commit off, for reference.

   Run with ./run bench [roots], roots (default 100000) being the number of
   top-level workflows the reads are measured among. Prints a Markdown
   report."
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [tenon.workflow :as engine]
            [tenon.workflow.db :as db]
            [tenon.workflow.fixtures :as fixtures]
            [tenon.workflow.postgres :as postgres]
            [tenon.workflow.sqlite :as sqlite])
  (:import [io.zonky.test.db.postgres.embedded EmbeddedPostgres]
           [java.util Arrays]))

;; Measuring

(defn- measure
  "Runs (f thread i) for i below n on each of threads threads, returning
   {:ops/s throughput, :p50-us and :p99 latency in microseconds}."
  [threads n f]
  (let [latencies (long-array (* threads n))
        start (System/nanoTime)
        workers (mapv (fn [t]
                        (future
                          (dotimes [i n]
                            (let [s (System/nanoTime)]
                              (f t i)
                              (aset latencies (+ (* t n) i) (- (System/nanoTime) s))))))
                      (range threads))]
    (run! deref workers)
    (let [secs (/ (- (System/nanoTime) start) 1e9)
          at (fn [q] (/ (aget latencies (min (dec (alength latencies)) (long (* q (alength latencies))))) 1000.0))]
      (Arrays/sort latencies)
      {:ops/s (/ (* threads n) secs) :p50-us (at 0.5) :p99 (at 0.99)})))

(defn- fmt-rate [x] (if (>= x 100) (format "%,.0f" (double x)) (format "%,.1f" (double x))))

(defn- fmt-us [us]
  (cond (>= us 1000) (format "%,.1f ms" (/ us 1000.0))
        :else (format "%,.0f us" (double us))))

;; The stores

(defn- postgres-server ^EmbeddedPostgres []
  (-> (EmbeddedPostgres/builder)
      ;; the embedded server's defaults are for tests: made durable here
      (.setServerConfig "fsync" "on")
      (.setServerConfig "synchronous_commit" "on")
      (.setServerConfig "full_page_writes" "on")
      (.setServerConfig "shared_buffers" "512MB")
      (.start)))

(defn- temp-sqlite-path []
  (let [f (java.io.File/createTempFile "tenon-bench" ".db")]
    (.delete f)
    (.getAbsolutePath f)))

(defn- stores
  "The stores to compare, as {:name :open (fn [] storage) :size (fn [storage] bytes)}."
  [^EmbeddedPostgres pg]
  (let [base (.getJdbcUrl pg "postgres" "postgres")
        schema-counter (atom 0)
        pg-store (fn [label options]
                   {:name label
                    :open (fn [] (let [schema (str "bench_" (swap! schema-counter inc))]
                                   (db/init-db! (postgres/storage (str base "&currentSchema=" schema options)
                                                                  {:pool-size 16}))))
                    :size (fn [s] (:size (jdbc/execute-one! s ["SELECT sum(pg_total_relation_size(c.oid))::bigint AS size
                                                                 FROM pg_class c
                                                                 JOIN pg_namespace n ON n.oid = c.relnamespace
                                                                WHERE n.nspname = current_schema() AND c.relkind = 'r'"])))
                    :free (fn [s]
                            (jdbc/execute! s ["CREATE EXTENSION IF NOT EXISTS pgstattuple"])
                            (:free (jdbc/execute-one! s ["SELECT sum((pgstattuple(c.oid)).free_space)::bigint AS free
                                                           FROM pg_class c
                                                           JOIN pg_namespace n ON n.oid = c.relnamespace
                                                          WHERE n.nspname = current_schema() AND c.relkind = 'r'"])))
                    :wal (fn [s] (:lsn (jdbc/execute-one! s ["SELECT pg_current_wal_lsn() - '0/0'::pg_lsn AS lsn"])))})]
    [{:name "SQLite"
      :open (fn [] (db/init-db! (sqlite/datasource (temp-sqlite-path))))
      :size (fn [s] (let [path (second (re-find #"jdbc:sqlite:([^?]+)" (.-url ^tenon.workflow.sqlite.pool.Pool s)))]
                      (reduce + (map #(.length (java.io.File. (str path %))) ["" "-wal"]))))}
     (pg-store "PostgreSQL" "")
     (pg-store "PostgreSQL, async commit" "&options=-c%20synchronous_commit%3Doff")]))

;; Writes

(def ^:private run-id (System/currentTimeMillis))

(defn- unique [t i] (str "[" run-id " " t " " i "]"))

(defn- lifecycle!
  "A workflow started and finished."
  [s t i]
  (db/finish! s (db/insert-workflow! s "bench/flat" (unique t i) nil "{:host \"a\"}" 15000) "DONE" "42"))

(defn- tree!
  "A workflow calling three others, reusing one of them, then finishing -
   10 writes."
  [s t i]
  (let [params (unique t i)
        root (db/insert-workflow! s "bench/root" params nil "{:host \"a\"}" 15000)
        kids (mapv #(let [c (db/insert-workflow! s (str "bench/child-" %) params root nil 15000)]
                      (db/finish! s c "DONE" "{:ok true}")
                      c)
                   (range 3))]
    (db/record-reuse! s (first kids) root nil)
    (db/finish! s root "DONE" "42")))

(defn- write-scenarios []
  [["start + finish a workflow, 1 thread" 1 1500 lifecycle!]
   ["start + finish a workflow, 16 threads" 16 600 lifecycle!]
   ["a workflow calling 3, reusing 1 (10 writes), 16 threads" 16 100 tree!]])

;; The engine

(def ^:private engine-counter (atom 0))

(defn- engine-scenarios []
  [["engine: a nested call (6 storage calls), 16 threads" 16 200
    (fn [_ _ _] (fixtures/nested-parent (+ (* run-id 1000) (swap! engine-counter inc))))]
   ["engine: a call reusing a stored result (2 storage calls), 16 threads" 16 500
    (fn [_ _ _] (fixtures/say-hi "again"))]])

;; Reads

(defn- populate!
  "roots top-level workflows written by 16 threads, each calling up to 3
   others, mostly DONE, ~1% ERROR, ~1% left running, some reused. Returns
   {:ops/s writes per second, :roots [...], :children [...]}."
  [s roots]
  (let [defs (mapv #(str "app.ns/wf-" %) (range 20))
        out (java.util.concurrent.ConcurrentLinkedQueue.)
        writes (java.util.concurrent.atomic.AtomicLong.)
        n (quot roots 16)
        start (System/nanoTime)]
    (run! deref
          (mapv (fn [t]
                  (future
                    (let [rnd (java.util.Random. t)
                          w! (fn [x] (.incrementAndGet writes) x)]
                      (dotimes [i n]
                        (let [k (str "[" t " " i "]")
                              root (w! (db/insert-workflow! s (defs (.nextInt rnd 5)) k nil "{:host \"a\"}" 15000))
                              kids (vec (for [j (range (.nextInt rnd 4))]
                                          (let [c (w! (db/insert-workflow! s (defs (+ 5 (.nextInt rnd 15)))
                                                                           (str "[" t " " i " " j "]") root nil 15000))]
                                            (w! (db/finish! s c (if (zero? (.nextInt rnd 50)) "ERROR" "DONE") "{:ok 1}"))
                                            c)))]
                          (when (and (seq kids) (zero? (.nextInt rnd 5)))
                            (w! (db/record-reuse! s (first kids) (when (.nextBoolean rnd) root) nil)))
                          (case (.nextInt rnd 100)
                            0 nil
                            1 (w! (db/finish! s root "ERROR" "{:message \"boom\"}"))
                            (w! (db/finish! s root "DONE" "42")))
                          (.add out [root kids]))))))
                (range 16)))
    (let [secs (/ (- (System/nanoTime) start) 1e9)
          pairs (vec out)]
      {:ops/s (/ (.get writes) secs)
       :secs secs
       :roots (mapv first pairs)
       :with-children (vec (keep #(when (seq (second %)) (first %)) pairs))
       :children (vec (mapcat second pairs))})))

(defn- read-scenarios [{:keys [roots with-children children]}]
  (let [pick (fn [coll t i] (nth coll (mod (+ (* t 7919) (* i 104729)) (count coll))))
        mid (nth (sort roots) (quot (count roots) 2))]
    [["get-workflow, 1 thread" 1 3000 (fn [s t i] (db/get-workflow s (pick children t i)))]
     ["get-workflow, 16 threads" 16 1000 (fn [s t i] (db/get-workflow s (pick children t i)))]
     ["find-by-wf-def-and-params, 16 threads" 16 1000
      (fn [s t i] (db/find-by-wf-def-and-params s "app.ns/wf-0" (str "[" (mod t 16) " " i "]")))]
     ["listing: first page (100)" 1 300 (fn [s _ _] (db/top-level-workflows s {:limit 100}))]
     ["listing: a page in the middle (100)" 1 300 (fn [s _ _] (db/top-level-workflows s {:limit 100 :before mid}))]
     ["listing: ERROR, top level (100)" 1 100 (fn [s _ _] (db/top-level-workflows s {:limit 100 :state "ERROR"}))]
     ["listing: a nested wf_def, all levels (100)" 1 300
      (fn [s _ _] (db/top-level-workflows s {:limit 100 :wf-def "app.ns/wf-12" :top-level-only? false}))]
     ["full-timeline of a workflow and its calls" 1 2000 (fn [s t i] (db/full-timeline s (pick with-children t i)))]
     ["extend-lease! of 500 invocations" 1 300
      (fn [s t i] (db/extend-lease! s (set (map #(pick roots t (+ i %)) (range 500))) 15000))]]))

;; The report

(defn- table [stores rows]
  (let [header (str "| | " (str/join " | " stores) " |")
        rule (str "|---|" (str/join "|" (repeat (count stores) "---:")) "|")]
    (str/join "\n" (concat [header rule] (for [[label & cells] rows] (str "| " label " | " (str/join " | " cells) " |"))))))

(defn- result-cell [{:keys [ops/s p50-us p99]}]
  (str (fmt-rate s) "/s, p50 " (fmt-us p50-us) ", p99 " (fmt-us p99)))

(defn -main [& [roots]]
  (let [roots (if roots (parse-long roots) 100000)
        pg (postgres-server)
        version (:version (jdbc/execute-one! (jdbc/get-datasource (.getJdbcUrl pg "postgres" "postgres"))
                                             ["SELECT current_setting('server_version') AS version"]))
        [sqlite-store pg-store pg-async-store] (stores pg)]
    (try
      (println "# tenon storage benchmark\n")
      (println (str "- " (.availableProcessors (Runtime/getRuntime)) " CPUs, JVM " (System/getProperty "java.version")
                    ", PostgreSQL " version " (embedded, same machine, over TCP), SQLite "
                    (:v (jdbc/execute-one! (jdbc/get-datasource "jdbc:sqlite::memory:") ["SELECT sqlite_version() AS v"]))))
      (println "- durable commits on both: SQLite WAL + synchronous=FULL, PostgreSQL fsync + synchronous_commit=on;")
      (println "  PostgreSQL with async commit (not durable) for reference\n")

      ;; writes, on fresh stores - each warmed up first
      (let [write-stores [sqlite-store pg-store pg-async-store]
            results (for [{:keys [open]} write-stores]
                      (with-open [s (open)]
                        (measure 4 50 (fn [t i] (lifecycle! s (+ 100 t) i))) ; warm-up
                        (mapv (fn [[_ threads n f]] (measure threads n (fn [t i] (f s t i)))) (write-scenarios))))
            results (vec results)]
        (println "## Writes (operations per second, latency per operation)\n")
        (println (table (map :name write-stores)
                        (map-indexed (fn [k [label]] (into [label] (map #(result-cell (nth % k)) results)))
                                     (write-scenarios))))
        (println))

      ;; the engine end to end
      (let [engine-stores [sqlite-store pg-store]
            results (vec (for [{:keys [open]} engine-stores]
                           (let [e (engine/init (open) :tenon/sweep-ms nil)]
                             (try
                               (binding [engine/*workflow-engine* e]
                                 (fixtures/say-hi "again")
                                 (mapv (fn [[_ threads n f]]
                                         (measure threads n (fn [t i] (binding [engine/*workflow-engine* e] (f nil t i)))))
                                       (engine-scenarios)))
                               (finally (engine/close! e))))))]
        (println "## The engine, end to end (calls per second)\n")
        (println (table (map :name engine-stores)
                        (map-indexed (fn [k [label]] (into [label] (map #(result-cell (nth % k)) results)))
                                     (engine-scenarios))))
        (println))

      ;; reads, among roots top-level workflows
      (let [results (vec (for [{:keys [open size free wal]} [sqlite-store pg-store]]
                           (with-open [s (open)]
                             (let [wal-before (when wal (wal s))
                                   data (populate! s roots)
                                   wal-after (when wal (wal s))
                                   _ (when wal (jdbc/execute! s ["ANALYZE"]))
                                   workflows (+ (count (:roots data)) (count (:children data)))]
                               (measure 4 200 (fn [t i] (db/get-workflow s (nth (:children data) (mod (+ t i) (count (:children data)))))))
                               {:populate data
                                :workflows workflows
                                :bytes (size s)
                                :free (when free (free s))
                                :wal-per-workflow (when wal (/ (double (- wal-after wal-before)) workflows))
                                :reads (mapv (fn [[_ threads n f]] (measure threads n (fn [t i] (f s t i))))
                                             (read-scenarios data))}))))]
        (println (format "## Reads, among %,d workflows (%,d top-level)\n" (:workflows (first results)) roots))
        (println (table ["SQLite" "PostgreSQL"]
                        (concat
                         [(into ["writing them, 16 threads (writes per second)"]
                                (map #(str (fmt-rate (get-in % [:populate :ops/s])) "/s ("
                                           (format "%.0f s" (get-in % [:populate :secs])) ")")
                                     results))]
                         (map-indexed (fn [k [label]] (into [label] (map #(result-cell (get-in % [:reads k])) results)))
                                      (read-scenarios (:populate (first results))))
                         [(into ["size on disk (tables and indexes)"]
                                (map #(str (format "%,.0f MB, %,.0f B per workflow"
                                                   (/ (:bytes %) 1048576.0) (/ (double (:bytes %)) (:workflows %)))
                                           (when (:free %)
                                             (format " (%,.0f MB of it free, left by updates for later writes)"
                                                     (/ (:free %) 1048576.0))))
                                     results))
                          ["WAL written per workflow" "-"
                           (format "%,.0f B" (:wal-per-workflow (second results)))]])))
        (println))
      (finally
        (.close pg)
        (shutdown-agents)))))
