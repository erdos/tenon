(ns tenon.workflow.redis-test
  "The Redis storage against the storage contract, and against the SQLite one.
   Needs a Redis server: TENON_REDIS_URI (default redis://localhost:6379),
   else the tests are skipped."
  (:require [clojure.test :refer [deftest is testing]]
            [taoensso.carmine :as car]
            [tenon.workflow :as engine]
            [tenon.workflow.db :as db]
            [tenon.workflow.timeout :as timeout]
            [tenon.workflow.redis :as redis]
            [tenon.workflow.sqlite :as sqlite]
            [tenon.workflow.storage-contract :as contract]))

(def ^:private uri (or (System/getenv "TENON_REDIS_URI") "redis://localhost:6379"))

(def ^:private available?
  (delay (try (with-open [ds (redis/datasource {:uri uri :prefix "tenon-probe"})]
                (= "PONG" (car/wcar {:pool (.-pool ^tenon.workflow.redis.RedisStorage ds) :spec {:uri uri}} (car/ping))))
              (catch Exception _ false))))

(defn- with-redis
  "Calls f with a storage on an unused prefix, deleting its keys after."
  [f]
  (if-not @available?
    (println "Redis is not reachable at" uri "- skipping")
    (with-open [ds (redis/datasource {:uri uri :prefix (str "tenon-test-" (random-uuid))})]
      (try (f ds)
           (finally (redis/flush! ds))))))

(deftest redis-storage-contract-test
  (with-redis #(contract/check-storage (db/init-db! %))))

(deftest redis-listing-contract-test
  (with-redis contract/check-listing))

(deftest redis-timeouts-contract-test
  (with-redis contract/check-timeouts))

;;; Same behaviour as SQLite

(defn- without-clock
  "A row without what the two storages' clocks make differ: timestamps - of
   the lease, only whether there is one."
  [row]
  (when row
    (-> row
        (dissoc :workflow/state-changed-at :event/created-at)
        (cond-> (contains? row :workflow/expires-at) (update :workflow/expires-at some?)))))

(defn- run-history!
  "Applies n random writes to every storage in storages, one after the
   other, checking each returns what the first does - the writes include
   many that must fail: finishing twice, restarting a running workflow,
   inserting a duplicate, using an invocation that was restarted since.
   Returns the invocation ids used, in order."
  [storages seed n]
  (let [rnd (java.util.Random. seed)
        pick (fn [coll] (nth coll (.nextInt rnd (count coll))))
        wf-defs ["h/a" "h/b" "h/c" "h/d"]
        ids (volatile! [])
        each! (fn [f]
                (let [results (mapv f storages)]
                  (is (apply = results) (str "storages agree: " (pr-str results)))
                  (first results)))
        id-or-none #(if (and (seq @ids) (pos? (.nextInt rnd 40))) (pick @ids) Long/MAX_VALUE)]
    (dotimes [i n]
      (case (.nextInt rnd 8)
        (0 1 2) (let [parent (when (and (seq @ids) (zero? (.nextInt rnd 3))) (pick @ids))
                      params (str "[" (.nextInt rnd (quot n 3)) "]")
                      metadata (when (zero? (.nextInt rnd 2)) (str "{:i " i "}"))
                      timeout (pick [nil 60000 0])
                      wf-def (pick wf-defs)]
                  (when-let [id (each! #(db/insert-workflow! % wf-def params parent metadata timeout))]
                    (vswap! ids conj id)))
        3 (let [id (id-or-none) state (pick ["DONE" "ERROR"])
                result (when (pos? (.nextInt rnd 5)) (str "{:r " i "}"))]
            (each! #(db/finish! % id state result)))
        4 (let [id (id-or-none)]
            (each! #(db/time-out! % id "{:message \"timed out\"}")))
        5 (let [id (id-or-none)
                metadata (when (zero? (.nextInt rnd 2)) "{:again true}")
                timeout (pick [nil 60000 0])]
            (when-let [new-id (each! #(db/restart! % id metadata timeout))]
              (vswap! ids conj new-id)))
        6 (let [id (id-or-none) parent (when (zero? (.nextInt rnd 2)) (id-or-none))
                metadata (when (zero? (.nextInt rnd 2)) "{:reuse 1}")]
            (each! #(db/record-reuse! % id parent metadata)))
        7 (let [batch (set (repeatedly (inc (.nextInt rnd 5)) id-or-none))]
            (each! #(db/extend-lease! % batch 60000)))))
    @ids))

(defn- compare-reads! [storages ids]
  (let [each! (fn [what f]
                (let [results (mapv f storages)]
                  (is (apply = results) (str what ": " (pr-str (vec (take 2 results)))))))
        max-id (+ 5 (apply max 0 ids))]
    (each! "list-expired" #(mapv without-clock (db/list-expired %)))
    (each! "top-level-wf-defs" #(db/top-level-wf-defs % true))
    (each! "all top-level-wf-defs" #(db/top-level-wf-defs % false))
    (doseq [id (range 0 max-id)]
      (each! (str "get-workflow " id) #(without-clock (db/get-workflow % id)))
      (each! (str "full-timeline " id) #(mapv without-clock (db/full-timeline % id))))
    (each! "find-by-wf-def-and-params"
           #(vec (for [wf-def ["h/a" "h/b" "h/c" "h/d" "h/none"] i (range 12)]
                   (without-clock (db/find-by-wf-def-and-params % wf-def (str "[" i "]"))))))
    (doseq [state [nil "STARTED" "DONE" "ERROR" "TIMEOUT" "REUSED"]
            wf-def [nil "h/a" "h/none"]
            top-level-only? [true false]
            limit [nil 3]
            before [nil 30 max-id]]
      (let [filters (cond-> {:top-level-only? top-level-only?}
                      state (assoc :state state) wf-def (assoc :wf-def wf-def)
                      limit (assoc :limit limit) before (assoc :before before))]
        (each! (pr-str filters) #(mapv without-clock (db/top-level-workflows % filters)))))
    (each! "top-level-workflows" #(mapv without-clock (db/top-level-workflows %)))))

(deftest redis-behaves-like-sqlite-test
  (with-redis
    (fn [redis-ds]
      (doseq [seed [1 2 3 4 5 6]]
        (let [path (.getAbsolutePath (java.io.File/createTempFile "tenon-diff" ".db"))]
          (try
            (with-open [sqlite-ds (sqlite/datasource path)]
              (redis/flush! redis-ds)
              (let [storages [(db/init-db! sqlite-ds) (db/init-db! redis-ds)]
                    ids (run-history! storages seed 300)]
                (testing (str "seed " seed)
                  (is (> (count ids) 80))
                  (compare-reads! storages ids))))
            (finally
              (doseq [suffix ["" "-wal" "-shm"]]
                (.delete (java.io.File. (str path suffix)))))))))))

;;; Compare-and-set under concurrency

(defn- race
  "Runs each of fs on its own thread, all released together; returns their results."
  [fs]
  (let [start (promise)
        futures (mapv (fn [f] (future @start (f))) fs)]
    (deliver start true)
    (mapv deref futures)))

(deftest redis-compare-and-set-under-concurrency-test
  (with-redis
    (fn [ds]
      (db/init-db! ds)
      (dotimes [round 20]
        (testing "one of several identical inserts wins"
          (let [ids (race (repeat 8 #(db/insert-workflow! ds "race/insert" (str "[" round "]") nil nil nil)))]
            (is (= 1 (count (remove nil? ids))))))
        (testing "one of several finishes wins"
          (let [id (db/insert-workflow! ds "race/finish" (str "[" round "]") nil nil 60000)
                results (race (map (fn [state] #(db/finish! ds id state "1")) (take 8 (cycle ["DONE" "ERROR"]))))]
            (is (= 1 (count (filter true? results))))
            (is (= 2 (count (db/full-timeline ds id))) "STARTED and the one end that won")))
        (testing "one of several restarts wins"
          (let [id (db/insert-workflow! ds "race/restart" (str "[" round "]") nil nil nil)]
            (db/finish! ds id "ERROR" "1")
            (is (= 1 (count (remove nil? (race (repeat 8 #(db/restart! ds id nil nil)))))))))
        (testing "a finish and a time-out of an expired invocation: exactly one wins"
          (let [id (db/insert-workflow! ds "race/timeout" (str "[" round "]") nil nil 0)
                results (race [#(db/finish! ds id "DONE" "1") #(db/time-out! ds id "{}")
                               #(db/finish! ds id "ERROR" "1") #(db/time-out! ds id "{}")])]
            (is (= 1 (count (filter true? results))))))
        (testing "ids are unique across concurrent writers"
          (let [ids (race (for [i (range 8)] #(db/insert-workflow! ds "race/unique" (str "[" round "," i "]") nil nil nil)))]
            (is (= 8 (count (set ids))))))))))

;;; The engine on Redis

(def ^:private inner-fn (fn [n] (* n 2)))
(def ^:private outer-fn (fn [n] (+ 1 (engine/run-invocation #'inner-fn inner-fn [n]))))
(def ^:private boom-fn (fn [msg] (throw (ex-info msg {}))))

(deftest engine-on-redis-test
  (with-redis
    (fn [ds]
      (let [engine (engine/init ds :tenon/sweep-ms nil :tenon/metadata-fn (constantly nil))]
        (try
          (binding [engine/*workflow-engine* engine]
            (is (= 11 (engine/run-invocation #'outer-fn outer-fn [5])))
            (is (= 11 (engine/run-invocation #'outer-fn outer-fn [5])) "the second call reuses the result")
            (is (= 1 (count (filter :event/reused (engine/list-invocations {:wf-def (str `outer-fn)})))))
            (is (thrown? Exception (engine/run-invocation #'boom-fn boom-fn ["x"])))
            (is (= ["ERROR"] (mapv :workflow/state (engine/list-invocations {:wf-def (str `boom-fn)}))))
            (is (= [] (engine/list-pending)))
            (is (= [["tenon.workflow.redis-test/inner-fn" 1] ["tenon.workflow.redis-test/outer-fn" 0]]
                   (->> (engine/list-invocations {:wf-def (str `outer-fn)})
                        (remove :event/reused)
                        first :workflow/invocation-id
                        (engine/full-timeline)
                        (filter #(= "DONE" (:workflow/state %)))
                        (mapv (juxt :workflow/name :workflow/depth))))
                "the nested call finishes first, a level below"))
          (finally
            (timeout/stop! engine)))))))
