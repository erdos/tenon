(ns tenon.workflow.sqlite-test
  (:require [clojure.test :refer [deftest is testing]]
            [matcher-combinators.test :refer [match?]]
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

(deftest idempotence-key-collisions-test
  ;; The idempotence key is a 64-bit hash: workflows whose keys collide
  ;; are still told apart by their wf_def and params.
  (with-temp-db
    (fn [path]
      (with-open [ds (sqlite/datasource path)]
        (db/init-db! ds)
        (with-redefs [sqlite/idempotence-key (fn ^long [^String _ ^String _ ^String _] 42)]
          (let [a (db/insert-workflow! ds "test.ns/a" "[1]" nil nil nil)
                b (db/insert-workflow! ds "test.ns/a" "[2]" nil nil nil)
                c (db/insert-workflow! ds "test.ns/c" "[1]" nil nil nil)
                d (db/insert-workflow! ds "test.ns/a" "2" "[1]" nil nil nil nil)]
            (is (every? int? [a b c d]) "colliding keys don't make them the same workflow")
            (is (= 4 (count (distinct [a b c d]))))
            (is (nil? (db/insert-workflow! ds "test.ns/a" "[2]" nil nil nil)) "but the same one is still found")
            (is (nil? (db/insert-workflow! ds "test.ns/a" "2" "[1]" nil nil nil nil)) "versioned too")
            (is (= [a b c] (mapv #(:workflow/invocation-id (db/find-by-wf-def-and-params ds %1 %2))
                                 ["test.ns/a" "test.ns/a" "test.ns/c"] ["[1]" "[2]" "[1]"])))
            (is (= d (:workflow/invocation-id (db/find-by-wf-def-and-params ds "test.ns/a" "2" "[1]"))))
            (is (nil? (db/find-by-wf-def-and-params ds "test.ns/a" "3" "[1]")))
            (is (nil? (db/find-by-wf-def-and-params ds "test.ns/c" "[2]")))))))))

(deftest version-migration-test
  ;; A database from before versions: no version and system_metadata
  ;; columns, and the old new_workflow view. Its workflows are unversioned.
  (with-temp-db
    (fn [path]
      (with-open [ds (sqlite/datasource path)]
        (db/init-db! ds)
        (db/finish! ds (db/insert-workflow! ds "test.ns/old" "[1]" nil nil nil) "DONE" ":old"))
      (jdbc/execute! (jdbc/get-datasource (str "jdbc:sqlite:" path))
                     ["DROP VIEW new_workflow"])
      (with-open [c (jdbc/get-connection (jdbc/get-datasource (str "jdbc:sqlite:" path)))]
        (doseq [ddl ["ALTER TABLE workflow DROP COLUMN version"
                     "ALTER TABLE event DROP COLUMN system_metadata"
                     "CREATE VIEW new_workflow (key, id, wf_def, params, parent_invocation_id, metadata, occurred_at,
                                                expires_at)
                        AS SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL WHERE 0"]]
          (jdbc/execute! c [ddl])))
      (with-open [ds (sqlite/datasource path)]
        (db/init-db! ds)
        (let [old (db/find-by-wf-def-and-params ds "test.ns/old" "[1]")]
          (is (= ":old" (:workflow/result old)) "the old workflow is found, unversioned")
          (is (nil? (:workflow/version old))))
        (is (nil? (db/insert-workflow! ds "test.ns/old" "[1]" nil nil nil)) "and still taken")
        (let [id (db/insert-workflow! ds "test.ns/old" "2" "[1]" nil nil "{:code-hash \"h\"}" nil)]
          (is (int? id) "another version is a workflow of its own")
          (is (match? {:workflow/version "2" :workflow/system-metadata "{:code-hash \"h\"}"}
                      (db/get-workflow ds id))))
        (testing "init-db! of the migrated database changes nothing"
          (is (identical? ds (db/init-db! ds))))))))
