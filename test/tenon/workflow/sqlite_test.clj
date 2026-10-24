(ns tenon.workflow.sqlite-test
  (:require [clojure.test :refer [deftest is]]
            [next.jdbc :as jdbc]
            [tenon.workflow.db :as db]
            [tenon.workflow.test-util :as test-util]
            [tenon.workflow.sqlite :as sqlite]
            [tenon.workflow.storage-contract :as contract]))

(defn- with-temp-db [f]
  (let [path (.getAbsolutePath (java.io.File/createTempFile "tenon-test" ".db"))]
    (try
      (f path)
      (finally
        (doseq [suffix ["" "-wal" "-shm"]]
          (.delete (java.io.File. (str path suffix))))))))

(deftest sqlite-storage-contract-test
  (with-temp-db
    (fn [path]
      (with-open [ds (sqlite/datasource path)]
        (contract/check-storage (db/init-db! ds))))))

(deftest sqlite-listing-contract-test
  (with-temp-db
    (fn [path]
      (with-open [ds (sqlite/datasource path)]
        (contract/check-listing ds)))))

(deftest sqlite-timeouts-contract-test
  (with-temp-db
    (fn [path]
      (with-open [ds (sqlite/datasource path)]
        (contract/check-timeouts ds)))))

(deftest concurrent-init-of-a-new-database-test
  ;; Several processes starting up on a new database: one creates the
  ;; schema, the others find it done.
  (with-temp-db
    (fn [path]
      (let [pools (vec (repeatedly 4 #(sqlite/datasource path)))]
        (try
          ;; the file's first connection switches it to WAL mode, which
          ;; connections opened at the same instant can fail to wait for -
          ;; not what is under test
          (jdbc/execute! (first pools) ["SELECT 1"])
          (let [started (promise)
                inits (mapv #(future @started (db/init-db! %)) pools)]
            (deliver started true)
            (is (every? some? (mapv deref inits))))
          (is (= [] (db/top-level-workflows (first pools) {})))
          (finally
            (run! #(.close ^java.io.Closeable %) pools)))))))

(deftest the-wal-is-checkpointed-under-single-writes-test
  ;; SQLite checkpoints the WAL (every 1000 pages) only when a statement
  ;; steps to its end. A write whose RETURNING row is read and then reset
  ;; never does - its WAL grew for as long as the process ran.
  (with-temp-db
    (fn [path]
      (with-open [ds (sqlite/datasource path)]
        (db/init-db! ds)
        (dotimes [i 1500]
          (db/finish! ds (test-util/insert-workflow! ds "test.ns/wal" (pr-str [i]) nil nil nil) "DONE" "1"))
        (let [page-size (:page_size (jdbc/execute-one! ds ["PRAGMA page_size"]))
              wal-pages (quot (.length (java.io.File. (str path "-wal"))) (+ 24 page-size))]
          (is (< wal-pages 1100) (str "the WAL has " wal-pages " pages")))))))

(deftest idempotence-key-collisions-test
  ;; The idempotence key is a 64-bit hash: workflows whose keys collide
  ;; are still told apart by their wf_def and params.
  (with-temp-db
    (fn [path]
      (with-open [ds (sqlite/datasource path)]
        (db/init-db! ds)
        (with-redefs [sqlite/idempotence-key (fn ^long [^String _ ^String _] 42)]
          (let [a (test-util/insert-workflow! ds "test.ns/a" "[1]" nil nil nil)
                b (test-util/insert-workflow! ds "test.ns/a" "[2]" nil nil nil)
                c (test-util/insert-workflow! ds "test.ns/c" "[1]" nil nil nil)]
            (is (every? int? [a b c]) "colliding keys don't make them the same workflow")
            (is (= 3 (count (distinct [a b c]))))
            (is (nil? (test-util/insert-workflow! ds "test.ns/a" "[2]" nil nil nil)) "but the same one is still found")
            (is (= [a b c] (mapv #(:workflow/invocation-id (db/find-by-wf-def-and-params ds %1 %2))
                                 ["test.ns/a" "test.ns/a" "test.ns/c"] ["[1]" "[2]" "[1]"])))
            (is (nil? (db/find-by-wf-def-and-params ds "test.ns/c" "[2]")))))))))
