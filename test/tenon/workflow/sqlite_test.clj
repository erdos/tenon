(ns tenon.workflow.sqlite-test
  (:require [clojure.test :refer [deftest is]]
            [next.jdbc :as jdbc]
            [tenon.workflow.db :as db]
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
          (db/finish! ds (db/insert-workflow! ds "test.ns/wal" (pr-str [i]) nil nil nil) "DONE" "1"))
        (let [page-size (:page_size (jdbc/execute-one! ds ["PRAGMA page_size"]))
              wal-pages (quot (.length (java.io.File. (str path "-wal"))) (+ 24 page-size))]
          (is (< wal-pages 1100) (str "the WAL has " wal-pages " pages")))))))
