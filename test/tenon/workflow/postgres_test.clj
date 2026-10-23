(ns tenon.workflow.postgres-test
  (:require [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
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
