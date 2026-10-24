(ns tenon.workflow.postgres.group-commit-test
  (:require [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [tenon.workflow.postgres.group-commit :as group-commit]
            [tenon.workflow.test-util :as test-util])
  (:import [java.sql Connection]))

(defn- execute! [^Connection c sql & params]
  (with-open [ps (.prepareStatement c sql)]
    (doseq [[i p] (map-indexed vector params)] (.setObject ps (int (inc i)) p))
    (.executeUpdate ps)))

(defn- with-table [f]
  (test-util/with-postgres-schema
    (fn [url]
      (let [ds (jdbc/get-datasource url)
            schema (re-find #"test_\d+" url)]
        (jdbc/execute! ds [(str "CREATE SCHEMA " schema)])
        (jdbc/execute! ds ["CREATE TABLE t (x bigint PRIMARY KEY, n bigint NOT NULL DEFAULT 0)"])
        ;; pooled: a lane holds a connection while it runs a batch
        (with-open [pool (com.zaxxer.hikari.HikariDataSource.
                          (doto (com.zaxxer.hikari.HikariConfig.) (.setJdbcUrl url) (.setMaximumPoolSize 8)))]
          (f pool))))))

(deftest writes-are-committed-together-and-fail-alone-test
  ;; 400 writes from 16 threads, every fourth a duplicate key: each
  ;; duplicate fails on its own, every other write is committed.
  (with-table
    (fn [ds]
      (let [w (group-commit/writer ds 4)
            insert (fn [x] (try (group-commit/write! w #(execute! % "INSERT INTO t (x) VALUES (?)" x))
                                :ok
                                (catch Exception _ :failed)))
            xs (for [i (range 400)] (if (zero? (mod i 4)) (dec i) i))
            results (->> (partition-all 25 xs)
                         (mapv (fn [part] (future (mapv (juxt identity insert) part))))
                         (mapcat deref))
            stored (set (map :t/x (jdbc/execute! ds ["SELECT x FROM t"])))]
        (is (= (count (distinct xs)) (count stored)))
        (is (= (- (count xs) (count (distinct xs))) (count (filter #(= :failed (second %)) results)))
            "the duplicates, and only they, failed")))))

(deftest lock-conflicts-between-batches-are-resolved-test
  ;; Batches take their writes' row locks in no particular order - each
  ;; increments one of a few rows, while others increment them all, in id
  ;; order - so they conflict, and deadlock. Every write is still applied,
  ;; exactly once, and none fails.
  (with-table
    (fn [ds]
      (jdbc/execute! ds ["INSERT INTO t (x) SELECT generate_series(1, 10)"])
      (let [w (group-commit/writer ds 4)
            errors (atom [])
            start (System/nanoTime)]
        (->> (range 16)
             (mapv (fn [t]
                     (future
                       (let [rnd (java.util.Random. t)]
                         (dotimes [i 150]
                           (try
                             (if (zero? (mod i 10))
                               (group-commit/write! w #(execute! % "UPDATE t SET n = n + 1 WHERE x IN
                                                                     (SELECT x FROM t ORDER BY x FOR UPDATE)"))
                               (group-commit/write! w #(execute! % "UPDATE t SET n = n + 1 WHERE x = ?"
                                                                 (long (inc (.nextInt rnd 10))))))
                             (catch Exception e (swap! errors conj e))))))))
             (run! deref))
        (is (= [] @errors))
        (testing "each write applied once: 15 of each thread's writes added 10, the other 135 one"
          (is (= (* 16 (+ (* 15 10) 135)) (:sum (jdbc/execute-one! ds ["SELECT sum(n)::bigint AS sum FROM t"])))))
        (testing "deadlocks were broken at once, not after the server's second"
          (is (< (/ (- (System/nanoTime) start) 1e9) 20)))))))
