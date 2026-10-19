(ns tenon.workflow.sqlite.pool-test
  (:require [clojure.test :refer [deftest is]]
            [next.jdbc :as jdbc]
            [tenon.workflow.sqlite :as sqlite]
            [tenon.workflow.sqlite.pool :as pool]))

(defn- with-pool [f]
  (let [path (.getAbsolutePath (java.io.File/createTempFile "tenon-pool" ".db"))]
    (try
      (with-open [ds (sqlite/datasource path)]
        (f ds))
      (finally
        (doseq [suffix ["" "-wal" "-shm"]]
          (.delete (java.io.File. (str path suffix))))))))

(deftest concurrent-writes-are-committed-together-and-fail-alone-test
  ;; 400 writes from 16 threads, every fourth a duplicate key: each
  ;; duplicate fails on its own, every other write is committed.
  (with-pool
    (fn [ds]
      (jdbc/execute! ds ["CREATE TABLE t (x INTEGER PRIMARY KEY)"])
      (let [insert (fn [x] (try (pool/write! ds #(jdbc/execute-one! % ["INSERT INTO t VALUES (?)" x]))
                                :ok
                                (catch Exception _ :failed)))
            xs (for [i (range 400)] (if (zero? (mod i 4)) (dec i) i))
            results (->> (partition-all 25 xs)
                         (mapv (fn [part] (future (mapv (juxt identity insert) part))))
                         (mapcat deref))
            stored (set (map :t/x (jdbc/execute! ds ["SELECT x FROM t"])))]
        (is (= (count (distinct xs)) (count stored)))
        (is (= stored (set (map first (filter #(= :ok (second %)) results)))))
        (is (= (- (count xs) (count (distinct xs))) (count (filter #(= :failed (second %)) results))))))))

(deftest a-write-sees-the-writes-before-it-in-its-batch-test
  (with-pool
    (fn [ds]
      (jdbc/execute! ds ["CREATE TABLE c (n INTEGER NOT NULL)"])
      (jdbc/execute! ds ["INSERT INTO c VALUES (0)"])
      (->> (range 16)
           (mapv (fn [_] (future (dotimes [_ 50]
                                   (pool/write! ds #(jdbc/execute-one! % ["UPDATE c SET n = n + 1"]))))))
           (run! deref))
      (is (= 800 (:c/n (jdbc/execute-one! ds ["SELECT n FROM c"])))))))
