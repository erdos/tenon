(ns tenon.workflow.postgres-test
  (:require [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [tenon.workflow.db :as db]
            [tenon.workflow.postgres :as postgres]
            [tenon.workflow.storage-contract :as contract]
            [tenon.workflow.test-util :as test-util]))

(defn- with-storage
  "Calls (f storage) with a storage on a fresh schema, closing it after."
  [f]
  (test-util/with-postgres-schema
    (fn [url]
      (with-open [s (postgres/storage url)]
        (f s)))))

(deftest postgres-storage-contract-test
  (with-storage #(contract/check-storage (db/init-db! %))))

(deftest postgres-listing-contract-test
  (with-storage contract/check-listing))

(deftest postgres-timeouts-contract-test
  (with-storage contract/check-timeouts))

(deftest schema-test
  (testing "the schema is the URL's currentSchema, else tenon, else the :schema option"
    (test-util/with-postgres-schema
      (fn [url]
        (with-open [s (db/init-db! (postgres/storage url))]
          (is (= ["event" "wf_def" "workflow"]
                 (mapv :tables/table_name
                       (jdbc/execute! s ["SELECT table_name FROM information_schema.tables
                                           WHERE table_schema = current_schema() ORDER BY table_name COLLATE \"C\""])))))
        (with-open [s (db/init-db! (postgres/storage url {:schema "test_other"}))]
          (try
            (let [id (db/insert-workflow! s "test.ns/a" "[]" nil nil nil)]
              (is (= 1 (:count (jdbc/execute-one! s ["SELECT count(*) FROM test_other.workflow WHERE id = ?" id])))))
            (finally
              (jdbc/execute! s ["DROP SCHEMA test_other CASCADE"])))))))
  (testing "a schema name is a lowercase identifier"
    (is (thrown? IllegalArgumentException (postgres/storage (test-util/postgres-url) {:schema "a; DROP TABLE x"})))
    (is (thrown? IllegalArgumentException (postgres/storage (test-util/postgres-url) {:schema "Tenon"})))))

(deftest a-datasource-of-the-caller-test
  ;; Given a DataSource, the storage uses it but leaves closing it to the
  ;; caller.
  (test-util/with-postgres-schema
    (fn [url]
      (let [ds (jdbc/get-datasource url)]
        (with-open [s (db/init-db! (postgres/storage ds {:schema (re-find #"test_\d+" url)}))]
          (let [id (db/insert-workflow! s "test.ns/ds" "[]" nil nil nil)]
            (is (true? (db/finish! s id "DONE" "1")))
            (is (= "DONE" (:workflow/state (db/get-workflow s id))))))))))

(deftest concurrent-init-of-a-new-database-test
  ;; Several processes starting up on a new database: one creates the
  ;; schema, the others find it done.
  (test-util/with-postgres-schema
    (fn [url]
      (let [storages (vec (repeatedly 4 #(postgres/storage url {:pool-size 2})))]
        (try
          (let [started (promise)
                inits (mapv #(future @started (db/init-db! %)) storages)]
            (deliver started true)
            (is (every? some? (mapv deref inits))))
          (is (= [] (db/top-level-workflows (first storages) {})))
          (testing "and init-db! of an existing schema changes nothing"
            (let [id (db/insert-workflow! (first storages) "test.ns/init" "[]" nil nil nil)]
              (db/init-db! (second storages))
              (is (= id (:workflow/invocation-id (db/get-workflow (second storages) id))))))
          (finally
            (run! #(.close ^java.io.Closeable %) storages)))))))

(deftest idempotence-key-collisions-test
  ;; The idempotence key is a 64-bit hash: workflows whose keys collide
  ;; are still told apart by their wf_def and params.
  (with-storage
    (fn [ds]
      (db/init-db! ds)
      (with-redefs [postgres/idempotence-key (fn ^long [^String _ ^String _] 42)]
        (let [a (db/insert-workflow! ds "test.ns/a" "[1]" nil nil nil)
              b (db/insert-workflow! ds "test.ns/a" "[2]" nil nil nil)
              c (db/insert-workflow! ds "test.ns/c" "[1]" nil nil nil)]
          (is (every? int? [a b c]) "colliding keys don't make them the same workflow")
          (is (= 3 (count (distinct [a b c]))))
          (is (nil? (db/insert-workflow! ds "test.ns/a" "[2]" nil nil nil)) "but the same one is still found")
          (is (= [a b c] (mapv #(:workflow/invocation-id (db/find-by-wf-def-and-params ds %1 %2))
                               ["test.ns/a" "test.ns/a" "test.ns/c"] ["[1]" "[2]" "[1]"])))
          (is (nil? (db/find-by-wf-def-and-params ds "test.ns/c" "[2]"))))))))

(deftest text-round-trips-test
  ;; Params, metadata and results are stored and read back as they are -
  ;; any Unicode, large or empty.
  (with-storage
    (fn [ds]
      (db/init-db! ds)
      (doseq [[i s] (map-indexed vector ["" "\"árvíztűrő tükörfúrógép\"" "\"😀 \\u0000 \\n\"" (pr-str (vec (range 5000)))])]
        (let [id (db/insert-workflow! ds "test.ns/text" (str "[" i " " s "]") nil s nil)]
          (db/finish! ds id "DONE" s)
          (is (= {:workflow/params (str "[" i " " s "]") :workflow/metadata s :workflow/result s}
                 (select-keys (db/get-workflow ds id) [:workflow/params :workflow/metadata :workflow/result]))))))))

;; Query plans

(defn- populate!
  "roots top-level workflows, each with up to three nested ones, mostly
   DONE, some STARTED or ERROR, some reused - written by 8 threads, so that
   the indexes on the running workflows churn as they would in use. Returns
   the roots' ids."
  [storage roots]
  (let [defs (mapv #(str "app.ns/wf-" %) (range 20))
        work (fn [t]
               (let [rnd (java.util.Random. t)]
                 (vec (for [i (range (quot roots 8))]
                        (let [k (+ (* t 1000000) i)
                              root (db/insert-workflow! storage (defs (.nextInt rnd 5)) (str "[" k "]") nil "{}" 15000)
                              kids (vec (for [j (range (.nextInt rnd 4))]
                                          (let [c (db/insert-workflow! storage (defs (+ 5 (.nextInt rnd 15)))
                                                                       (str "[" k " " j "]") root nil 15000)]
                                            (db/finish! storage c (if (zero? (.nextInt rnd 50)) "ERROR" "DONE") "1")
                                            c)))]
                          (when (and (seq kids) (zero? (.nextInt rnd 5)))
                            (db/record-reuse! storage (first kids) (when (.nextBoolean rnd) root) nil))
                          (case (.nextInt rnd 100)
                            0 nil
                            1 (db/finish! storage root "ERROR" "{}")
                            (db/finish! storage root "DONE" "42"))
                          root)))))]
    (into [] cat (mapv deref (mapv #(future (work %)) (range 8))))))

(defn- plans
  "The plans sql (with ? params) is run with, executed with params - under
   plan_cache_mode mode - in a transaction rolled back after: the
   statement's, and those of the statements run by the functions it calls.
   Returns their lines."
  [storage sql params mode]
  (with-open [c (jdbc/get-connection storage)]
    (.setAutoCommit c false)
    (try
      (let [n (atom 0)
            literal #(cond (nil? %) "NULL"
                           (string? %) (str "'" (.replace ^String % "'" "''") "'")
                           (instance? (Class/forName "[J") %) (str "'{" (apply str (interpose "," %)) "}'")
                           :else (str %))
            run (fn [sql]
                  (with-open [st (.createStatement c)]
                    (let [rows (when (.execute st sql)
                                 (with-open [r (.getResultSet st)]
                                   (loop [acc []] (if (.next r) (recur (conj acc (.getString r 1))) acc))))]
                      (concat (loop [w (.getWarnings st) acc []] (if w (recur (.getNextWarning w) (conj acc (.getMessage w))) acc))
                              rows))))]
        (run! run ["DEALLOCATE ALL" "LOAD 'auto_explain'" "SET auto_explain.log_min_duration = 0" "SET auto_explain.log_level = notice"
                   "SET auto_explain.log_nested_statements = on" "SET client_min_messages = notice"
                   (str "SET plan_cache_mode = " mode)
                   (str "PREPARE q AS " (clojure.string/replace sql #"\?" (fn [_] (str "$" (swap! n inc)))))])
        (mapcat clojure.string/split-lines
                (run (str "EXPLAIN EXECUTE q" (when (seq params) (str "(" (apply str (interpose ", " (map literal params))) ")"))))))
      (finally
        (.rollback c)))))

(deftest no-table-scans-test
  ;; Each statement - and each statement of the functions they call - in
  ;; every shape of the listing finds its rows from an index, and by primary
  ;; key lookups from there, whatever plan Postgres would cache: none scans
  ;; event or workflow, nor goes parallel to.
  (test-util/with-postgres-schema
    (fn [url]
      (with-open [s (db/init-db! (postgres/storage url {:pool-size 8}))]
        (let [roots (populate! s 10000)
              _ (jdbc/execute! s ["VACUUM ANALYZE event, workflow, wf_def"])
              sqls (.-sqls s)
              some-root (nth roots 1000)
              started (first (filter #(= "STARTED" (:workflow/state (db/get-workflow s %))) roots))
              mid (nth (sort roots) (quot (count roots) 2))
              statements (concat
                          [["insert" (:insert sqls) [12345 "app.ns/wf-7" "[new]" some-root nil 15000]]
                           ["insert an existing one" (:insert sqls) [(db/idempotence-key "app.ns/wf-0" "[0]") "app.ns/wf-0" "[0]" nil nil 1]]
                           ["finish" (:finish sqls) [1 started true "42"]]
                           ["time-out" (:finish sqls) [3 started false "42"]]
                           ["restart" (:restart sqls) [some-root nil 15000]]
                           ["reuse" (:reuse sqls) [nil started started some-root]]
                           ["get-workflow" (:get-workflow sqls) [some-root]]
                           ["find" (:find sqls) [(db/idempotence-key "app.ns/wf-0" "[0]")]]
                           ["list-expired" (:list-expired sqls) []]
                           ["extend-lease" (:extend-lease sqls) [60000 (long-array (take 500 roots))]]
                           ["full-timeline" (:timeline sqls) [some-root]]]
                          (for [state [nil "STARTED" "DONE" "ERROR" "TIMEOUT"]
                                wf-def [nil "app.ns/wf-3" "app.ns/wf-12"]
                                top-level-only? [true false]
                                before [nil mid]
                                :let [filters (cond-> {:limit 100 :top-level-only? top-level-only?}
                                                state (assoc :state state)
                                                wf-def (assoc :wf-def wf-def :wf-def-id (@#'postgres/wf-def-id s wf-def))
                                                before (assoc :before before))
                                      [sql params] (@#'postgres/calls-query (.-schema s) (java.util.concurrent.ConcurrentHashMap.) filters)]]
                            [(str "top-level-workflows " (dissoc filters :wf-def-id)) sql params]))]
          (doseq [[label sql params] statements
                  mode ["force_custom_plan" "force_generic_plan"]]
            (let [lines (plans s sql params mode)]
              (is (seq lines))
              (is (= [] (filterv #(re-find #"Seq Scan on (event|workflow)\b|Gather|Parallel" %) lines))
                  (str label ", " mode ":\n" (clojure.string/join "\n" lines))))))))))

;; Write amplification

(deftest workflow-rows-change-only-with-their-state-and-lease-test
  ;; Calls - a workflow's calls of others, its reuses - only append events:
  ;; no other workflow's row is updated, so none is locked either. A
  ;; workflow's own row changes with its state, and its lease, whose
  ;; renewals - the most frequent update - are HOT: they write no index
  ;; entry.
  (test-util/with-postgres-schema
    (fn [url]
      ;; one connection, so that it can flush the statistics of its own
      ;; writes - which takes PostgreSQL 15
      (with-open [s (db/init-db! (postgres/storage url {:pool-size 1}))]
        (when (<= 150000 (:v (jdbc/execute-one! s ["SELECT current_setting('server_version_num')::int AS v"])))
          (let [updates (fn []
                          (jdbc/execute! s ["SELECT pg_stat_force_next_flush()"])
                          (jdbc/execute-one! s ["SELECT n_tup_upd AS updated, n_tup_hot_upd AS hot
                                                   FROM pg_stat_user_tables WHERE relid = 'workflow'::regclass"]
                                             {:builder-fn rs/as-unqualified-maps}))
                parent (db/insert-workflow! s "test.ns/parent" "[]" nil nil 60000)
                children (vec (for [i (range 20)]
                                (let [c (db/insert-workflow! s "test.ns/child" (str "[" i "]") parent nil nil)]
                                  (db/finish! s c "DONE" "1")
                                  c)))
                before (updates)]
            (testing "calls update no workflow row"
              (doseq [c children] (db/record-reuse! s c parent nil))
              (dotimes [i 20] (db/insert-workflow! s "test.ns/child" (str "[more " i "]") parent nil nil))
              (is (= before (updates))))
            (testing "lease renewals are HOT"
              (dotimes [_ 50] (db/extend-lease! s #{parent} 60000))
              (let [after (updates)]
                (is (= 50 (- (:updated after) (:updated before))))
                (is (= 50 (- (:hot after) (:hot before))))))
            (testing "a state change is not: it moves the row in and out of the indexes of the running and failed ones"
              (let [before (updates)]
                (db/finish! s parent "DONE" "1")
                (let [after (updates)]
                  (is (= 1 (- (:updated after) (:updated before))))
                  (is (= 0 (- (:hot after) (:hot before)))))))))))))
