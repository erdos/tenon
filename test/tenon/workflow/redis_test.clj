(ns tenon.workflow.redis-test
  "The Redis storage against the storage contract, and against the SQLite one.
   Needs a Redis server: TENON_REDIS_URI (default redis://localhost:6379),
   else the tests are skipped."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.walk :as walk]
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
   other, checking each gives the same outcome - the writes include many
   that must fail: finishing twice, restarting a running workflow, inserting
   a duplicate, using an invocation that was restarted since. The storages
   hand out different ids (integers, UUIDs): an invocation is picked by its
   position in each storage's own list. Returns those lists."
  [storages seed n]
  (let [rnd (java.util.Random. seed)
        pick (fn [coll] (nth coll (.nextInt rnd (count coll))))
        wf-defs ["h/a" "h/b" "h/c" "h/d"]
        ids (mapv (fn [_] (volatile! [])) storages)
        position-of (fn [k] (if (and (pos? (count @(first ids))) (pos? (.nextInt rnd 40)))
                              (.nextInt rnd (count @(first ids)))
                              k))
        id-at (fn [i pos] (if (neg? pos) Long/MAX_VALUE (nth @(nth ids i) pos)))
        each! (fn [f]
                (let [results (vec (map-indexed f storages))]
                  (is (apply = (map #(if (or (nil? %) (boolean? %) (set? %)) % :id) results))
                      (str "storages agree: " (pr-str results)))
                  results))
        remember! (fn [results]
                    (when (every? some? results)
                      (doseq [[v id] (map vector ids results)] (vswap! v conj id))))]
    (dotimes [i n]
      (case (.nextInt rnd 8)
        (0 1 2) (let [parent (when (and (pos? (count @(first ids))) (zero? (.nextInt rnd 3)))
                               (.nextInt rnd (count @(first ids))))
                      params (str "[" (.nextInt rnd (quot n 3)) "]")
                      metadata (when (zero? (.nextInt rnd 2)) (str "{:i " i "}"))
                      timeout (pick [nil 60000 0])
                      wf-def (pick wf-defs)]
                  (remember! (each! (fn [k s] (db/insert-workflow! s wf-def params (some->> parent (id-at k)) metadata timeout)))))
        3 (let [pos (position-of -1) state (pick ["DONE" "ERROR"])
                result (when (pos? (.nextInt rnd 5)) (str "{:r " i "}"))]
            (each! (fn [k s] (db/finish! s (id-at k pos) state result))))
        4 (let [pos (position-of -1)]
            (each! (fn [k s] (db/time-out! s (id-at k pos) "{:message \"timed out\"}"))))
        5 (let [pos (position-of -1)
                metadata (when (zero? (.nextInt rnd 2)) "{:again true}")
                timeout (pick [nil 60000 0])]
            (remember! (each! (fn [k s] (db/restart! s (id-at k pos) metadata timeout)))))
        6 (let [pos (position-of -1) parent (when (zero? (.nextInt rnd 2)) (position-of -1))
                metadata (when (zero? (.nextInt rnd 2)) "{:reuse 1}")]
            (each! (fn [k s] (db/record-reuse! s (id-at k pos) (some->> parent (id-at k)) metadata))))
        7 (let [batch (vec (distinct (repeatedly (inc (.nextInt rnd 5)) #(position-of -1))))
                results (each! (fn [k s]
                                 (let [extended (db/extend-lease! s (set (map (partial id-at k) batch)) 60000)]
                                   ;; as positions: the ids differ
                                   (set (keep (fn [pos] (when (extended (id-at k pos)) pos)) batch)))))]
            results)))
    (mapv deref ids)))

(def ^:private id-keys
  #{:workflow/invocation-id :workflow/parent-invocation-id :workflow/current-invocation-id :event/call-id})

(defn- renumbered
  "data with every id replaced by its rank among the ids in it - ids are
   ordered, and storages differ in which they hand out."
  [data]
  (let [found (volatile! (sorted-set-by compare))]
    (walk/postwalk (fn [x] (when (map? x) (doseq [k id-keys] (some->> (get x k) (vswap! found conj)))) x) data)
    (let [rank (zipmap @found (range))]
      (walk/postwalk (fn [x] (if (map? x)
                               (reduce (fn [m k] (cond-> m (some? (get m k)) (update k rank))) x id-keys)
                               x))
                     data))))

(defn- read-everything
  "What every read method says about storage, whose invocations are ids."
  [storage ids]
  (let [everything (db/top-level-workflows storage {:top-level-only? false})
        befores (mapv #(when % (some-> (nth everything % nil) :event/call-id)) [nil 15 40 90 1000])]
    {:list-expired (mapv without-clock (db/list-expired storage))
     :top-level-wf-defs [(db/top-level-wf-defs storage true) (db/top-level-wf-defs storage false)]
     :get-workflow (mapv #(without-clock (db/get-workflow storage %)) (conj ids Long/MAX_VALUE))
     :full-timeline (mapv #(mapv without-clock (db/full-timeline storage %)) (conj ids Long/MAX_VALUE))
     :find-by-wf-def-and-params
     (vec (for [wf-def ["h/a" "h/b" "h/c" "h/d" "h/none"] i (range 12)]
            (without-clock (db/find-by-wf-def-and-params storage wf-def (str "[" i "]")))))
     :unfiltered (mapv without-clock (db/top-level-workflows storage))
     :listings (into {}
                     (for [state [nil "STARTED" "DONE" "ERROR" "TIMEOUT" "REUSED"]
                           wf-def [nil "h/a" "h/none"]
                           top-level-only? [true false]
                           limit [nil 3]
                           [before-pos before] (map-indexed vector befores)]
                       [[state wf-def top-level-only? limit before-pos]
                        (mapv without-clock
                              (db/top-level-workflows storage
                                                      (cond-> {:top-level-only? top-level-only?}
                                                        state (assoc :state state) wf-def (assoc :wf-def wf-def)
                                                        limit (assoc :limit limit) before (assoc :before before))))]))}))

(defn- compare-reads! [storages id-lists]
  (let [[expected & others] (mapv (fn [s ids] (renumbered (read-everything s ids))) storages id-lists)]
    (doseq [other others, k (keys expected)]
      (if (= :listings k)
        (doseq [filters (keys (k expected))]
          (is (= (get-in expected [k filters]) (get-in other [k filters])) (pr-str filters)))
        (is (= (k expected) (k other)) (name k))))))

(deftest redis-behaves-like-sqlite-test
  (with-redis
    (fn [redis-ds]
      (doseq [seed [1 2 3 4 5 6]]
        (let [path (.getAbsolutePath (java.io.File/createTempFile "tenon-diff" ".db"))]
          (try
            (with-open [sqlite-ds (sqlite/datasource path)]
              (redis/flush! redis-ds)
              (let [storages [(db/init-db! sqlite-ds) (db/init-db! redis-ds)]
                    id-lists (run-history! storages seed 300)]
                (testing (str "seed " seed)
                  (is (> (count (first id-lists)) 80))
                  (compare-reads! storages id-lists))))
            (finally
              (doseq [suffix ["" "-wal" "-shm"]]
                (.delete (java.io.File. (str path suffix)))))))))))

(deftest redis-ids-are-uuids-in-the-order-they-were-made-test
  (with-redis
    (fn [ds]
      (db/init-db! ds)
      (let [ids (vec (for [i (range 300)]
                       (let [id (db/insert-workflow! ds "uuid/w" (str "[" i "]") nil nil nil)]
                         (when (zero? (mod i 3)) (db/finish! ds id "DONE" "1"))
                         id)))]
        (is (every? #(re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}" %) ids))
        (is (every? #(= % (str (java.util.UUID/fromString %))) ids) "parse as UUIDs")
        (is (= ids (sort ids)) "later ones sort later, also within the same millisecond")
        (is (= 300 (count (set ids))))
        (is (= (reverse ids) (mapv :workflow/invocation-id (db/top-level-workflows ds))) "newest first")))))

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
