(ns tenon.workflow.concurrency-test
  "Every storage under concurrent writes: many threads inserting, finishing,
   timing out, restarting and reusing the same few workflows, and renewing
   their leases. None of it may fail - no deadlock, no constraint violation -
   and what is stored must be exactly what the writes that succeeded wrote:
   the event lists each storage walks agree with a plain scan of its
   tables."
  (:require [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [tenon.workflow.db :as db]
            [tenon.workflow.postgres :as postgres]
            [tenon.workflow.sqlite :as sqlite]
            [tenon.workflow.test-util :as test-util]))

(defn- open-storage [backend db]
  (case backend
    :sqlite (sqlite/datasource db)
    :postgres (postgres/storage db {:pool-size 16})))

(defn- run-workload!
  "threads threads each running n random writes against storage, on a
   shared set of workflows. Returns {:inserted {[wf-def params] [id ...]}
   :errors [...]}."
  [storage threads n]
  (let [ids (atom [])                    ; first invocation ids
        inserted (atom {})
        errors (atom [])
        ;; mostly the first few: writes meet on the same rows all the time
        pick (fn [^java.util.Random rnd coll]
               (when (seq coll)
                 (nth coll (.nextInt rnd (if (.nextBoolean rnd) (min 8 (count coll)) (count coll))))))
        current (fn [id] (:workflow/invocation-id (db/get-workflow storage id)))
        op (fn [^java.util.Random rnd]
             (case (.nextInt rnd 10)
               (0 1 2) (let [wf-def (pick rnd ["c/a" "c/b" "c/c"])
                             params (str "[" (.nextInt rnd 60) "]")
                             parent (when (.nextBoolean rnd) (some-> (pick rnd @ids) current))
                             id (test-util/insert-workflow! storage wf-def params parent nil (pick rnd [nil 0 60000]))]
                         (when id
                           (swap! ids conj id)
                           (swap! inserted update [wf-def params] (fnil conj []) id)))
               (3 4) (when-let [id (some-> (pick rnd @ids) current)]
                       (db/finish! storage id (pick rnd ["DONE" "ERROR"]) "1"))
               5 (when-let [id (some-> (pick rnd @ids) current)]
                   (db/time-out! storage id "{:message \"timed out\"}"))
               6 (when-let [id (some-> (pick rnd @ids) current)]
                   (db/restart! storage id nil (pick rnd [nil 0 60000])))
               (7 8) (when-let [id (pick rnd @ids)]
                       ;; any invocation of it - also an earlier one - from
                       ;; any caller, so reuses cross each other's way
                       (db/record-reuse! storage (pick rnd [id (current id)]) (some-> (pick rnd @ids) current) nil))
               9 (when-let [some-ids (seq (repeatedly 20 #(some-> (pick rnd @ids) current)))]
                   (db/extend-lease! storage (set (remove nil? some-ids)) 60000))))
        workers (mapv (fn [t]
                        (future
                          (let [rnd (java.util.Random. t)]
                            (dotimes [_ n]
                              (try (op rnd)
                                   (catch Throwable e (swap! errors conj e)))))))
                      (range threads))]
    (run! deref workers)
    {:inserted @inserted :errors @errors}))

(def ^:private unqualified {:builder-fn rs/as-unqualified-lower-maps})

(defn- walk
  "The ids of the list starting at id, following next-key through events."
  [events id next-key]
  (loop [id id acc []]
    (if id
      (recur (get-in events [id next-key]) (conj acc id))
      acc)))

(defn- check-tables
  "The invariants of a storage's tables: its workflow rows' state lists
   hold exactly the events a scan finds for them, newest first, a row's
   current state is its newest state event, and the states follow each
   other as they may. Then those of backend: SQLite's lists of the calls a
   workflow made and of its reuses hold exactly theirs; PostgreSQL's calls
   name their caller's workflow."
  [backend ds]
  (let [sqlite? (= :sqlite backend)
        events (into {} (map (juxt :id identity))
                     (jdbc/execute! ds [(str "SELECT id, workflow_id, state, invocation_id, parent_invocation_id, wf_def_id,
                                                     prev_id, " (if sqlite? "prev_call_id" "caller_id") "
                                                FROM event")]
                                    unqualified))
        workflows (jdbc/execute! ds [(str "SELECT id, invocation_id, state, state_id, expires_at"
                                          (when sqlite? ", last_reuse_id, last_call_id") " FROM workflow")]
                                 unqualified)
        of-workflow (group-by :workflow_id (vals events))
        caller-of #(get-in events [(:parent_invocation_id %) :workflow_id])
        calls-by (group-by caller-of (filter #(and (:wf_def_id %) (:parent_invocation_id %)) (vals events)))
        newest-first #(sort > (map :id %))
        problems (for [w workflows
                       :let [own (of-workflow (:id w))
                             states (remove #(= 4 (:state %)) own)
                             state-seq (map :state (sort-by :id states))]
                       problem [(when-not (= (newest-first states) (walk events (:state_id w) :prev_id))
                                  :state-list)
                                (when-not (= (:state w) (get-in events [(:state_id w) :state]))
                                  :current-state)
                                (when-not (= (:invocation_id w) (apply max (map :id (filter #(zero? (:state %)) own))))
                                  :current-invocation)
                                (when-not (or (zero? (:state w)) (nil? (:expires_at w)))
                                  :lease-outside-started)
                                ;; STARTED, then one way it ended, then maybe
                                ;; a restart: STARTED, ...
                                (when-not (every? (fn [[a b]] (= (zero? a) (pos? b))) (partition 2 1 state-seq))
                                  :state-sequence)
                                (when-not (zero? (first state-seq))
                                  :first-state)
                                ;; an end names the invocation it ended
                                (when-not (every? (fn [[a b]] (or (zero? (:state b)) (= (:id a) (:invocation_id b))))
                                                  (partition 2 1 (sort-by :id states)))
                                  :ended-invocation)
                                (when (and sqlite? (not= (newest-first (filter #(= 4 (:state %)) own))
                                                         (walk events (:last_reuse_id w) :prev_id)))
                                  :reuse-list)
                                (when (and sqlite? (not= (newest-first (calls-by (:id w)))
                                                         (walk events (:last_call_id w) :prev_call_id)))
                                  :call-list)
                                (when-not (or sqlite? (every? #(= (caller-of %) (:caller_id %))
                                                              (filter :wf_def_id own)))
                                  :caller)]
                       :when problem]
                   [(:id w) problem])]
    (is (= [] (vec (take 10 problems))) "workflows whose tables disagree")
    (count workflows)))

(deftest concurrent-writes-test
  (doseq [backend test-util/backends]
    (testing (name backend)
      (test-util/with-db backend
        (fn [db _]
          (with-open [storage (open-storage backend db)]
            (db/init-db! storage)
            (let [{:keys [inserted errors]} (run-workload! storage 16 400)]
              (is (empty? errors) (str "writes failed: " (vec (take 3 errors))))
              (testing "each (wf_def, params) was inserted once"
                (is (every? #(= 1 (count %)) (vals inserted))))
              (testing "the tables agree with themselves"
                (is (< 100 (check-tables backend storage))))
              (testing "each workflow's timeline is its history"
                (doseq [id (take 100 (mapcat val inserted))]
                  (is (= (map (juxt :workflow/invocation-id :workflow/state)
                              (remove #(= "REUSED" (:workflow/state %)) (test-util/history storage id)))
                         (map (juxt :workflow/invocation-id :workflow/state)
                              (remove #(= "REUSED" (:workflow/state %))
                                      (filter #(zero? (:workflow/depth %)) (db/full-timeline storage id)))))
                      (str "workflow " id))))
              (testing "the listing has every call: each first start and each reuse"
                (let [calls (db/top-level-workflows storage {:top-level-only? false})]
                  (is (= (count (mapcat val inserted)) (count (remove :event/reused calls))))
                  (is (= (sort > (map :event/call-id calls)) (map :event/call-id calls))))))))))))

(defn- race
  "Calls f from n threads released at once, returning their results."
  [n f]
  (let [ready (java.util.concurrent.CountDownLatch. n)
        go (promise)
        racers (mapv (fn [_] (future (.countDown ready) @go (f))) (range n))]
    (.await ready)
    (deliver go true)
    (mapv deref racers)))

(deftest races-test
  ;; The same write, from 8 threads at the very same time - for 30 rounds:
  ;; one of them succeeds, every time.
  (doseq [backend test-util/backends]
    (testing (name backend)
      (test-util/with-db backend
        (fn [db _]
          (with-open [s (open-storage backend db)]
            (db/init-db! s)
            (dotimes [round 30]
              (let [params (str "[" round "]")]
                (testing "inserts of one workflow: one inserts it"
                  (is (= 1 (count (remove nil? (race 8 #(test-util/insert-workflow! s "race/insert" params nil nil 60000)))))))
                (let [id (:workflow/invocation-id (db/find-by-wf-def-and-params s "race/insert" params))]
                  (testing "finishes of one invocation: one finishes it"
                    (is (= 1 (count (filter true? (race 8 #(db/finish! s id (rand-nth ["DONE" "ERROR"]) "1")))))))
                  (testing "restarts of one invocation: one restarts it"
                    (let [restarted (remove nil? (race 8 #(db/restart! s id nil 0)))]
                      (is (= 1 (count restarted)))
                      (testing "and a time-out racing finishes of it: one ends it"
                        (is (= 1 (count (filter true? (race 8 (let [n (atom 0)]
                                                                #(if (odd? (swap! n inc))
                                                                   (db/time-out! s (first restarted) "{}")
                                                                   (db/finish! s (first restarted) "DONE" "2"))))))))))
                    (is (= ["STARTED" "DONE" "STARTED" "DONE"]
                           (map #(if (= "ERROR" %) "DONE" %) ; either way it ended
                                (map #(if (= "TIMEOUT" %) "DONE" %)
                                     (map :workflow/state (test-util/history s id)))))
                        "its history: one start, one end, one restart, one end"))
                  (testing "reuses of it: each is logged"
                    (race 8 #(db/record-reuse! s id nil nil))
                    (is (= 8 (count (filter #(= "REUSED" (:workflow/state %)) (db/full-timeline s id))))))
                  (testing "starts of it, finished: each reuses it, and is logged"
                    (is (= (repeat 8 :reused)
                           (map :start/outcome (race 8 #(db/start-workflow! s "race/insert" params nil nil 60000)))))
                    (is (= 16 (count (filter #(= "REUSED" (:workflow/state %)) (db/full-timeline s id)))))))))
            (check-tables backend s)))))))
