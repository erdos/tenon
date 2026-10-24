(ns tenon.workflow.storage-contract
  "A test suite any tenon.workflow.db/Storage implementation must pass:
   every method's return value has the documented shape - each required
   key present, each value of the documented type. Call check-storage
   from a deftest with a fresh, empty storage of the implementation under
   test - and check-timeouts, for the db/Timeouts methods, with another."
  (:require [clojure.test :refer [is testing]]
            [tenon.workflow.db :as db]
            [tenon.workflow.test-util :as test-util]))

(defn- nilable [pred] (fn [v] (or (nil? v) (pred v))))

(def ^:private run-state? #{"STARTED" "DONE" "ERROR" "TIMEOUT"})

(def workflow-shape
  "find-by-wf-def-and-params rows."
  {:workflow/invocation-id        int?
   :workflow/name               string?
   :workflow/params               string?
   :workflow/parent-invocation-id (nilable int?)
   :workflow/state                run-state?
   :workflow/state-changed-at     int?
   :workflow/expires-at           (nilable int?)
   :workflow/metadata             (nilable string?)
   :workflow/result               (nilable string?)})

(def get-workflow-shape
  "get-workflow rows."
  (assoc workflow-shape :workflow/expired boolean?))

(def start-shape
  "start-workflow! rows."
  (assoc get-workflow-shape :start/outcome #{:started :reused :running}))

(def call-shape
  "top-level-workflows rows."
  (assoc workflow-shape
         :event/call-id    int?
         :event/created-at int?
         :event/reused     boolean?))

(def timeline-shape
  "full-timeline rows."
  {:workflow/invocation-id         int?
   :workflow/current-invocation-id int?
   :workflow/parent-invocation-id  (nilable int?)
   :workflow/name                string?
   :workflow/state                 #{"STARTED" "DONE" "ERROR" "TIMEOUT" "REUSED"}
   :workflow/state-changed-at      int?
   :event/data                  (nilable string?)
   :workflow/depth                 nat-int?})

(defn- check-shape [shape row]
  (is (map? row) (str "a map, not " (pr-str row)))
  (doseq [[k valid?] shape]
    (is (contains? row k) (str "has " k))
    (when (contains? row k)
      (is (valid? (get row k)) (str k " has the wrong type: " (pr-str (get row k)))))))

(defn- check-rows
  "Checks every row of rows against shape - and that rows exercise each
   key with a non-nil value at least once, so a nilable key's type is
   checked too, not just its absence."
  [label shape rows]
  (testing label
    (is (seq rows) "the scenario yields rows to check")
    (doseq [row rows] (check-shape shape row))
    (doseq [k (keys shape)]
      (is (some #(some? (get % k)) rows) (str k " is non-nil in some row")))))

(defn check-storage
  "Runs the suite against storage, a fresh and empty Storage."
  [storage]
  (testing "init-db! returns the storage"
    (is (identical? storage (db/init-db! storage))))
  (let [root (test-util/insert-workflow! storage "contract/root" "[1]" nil "{:actor 1}" 60000)
        child (test-util/insert-workflow! storage "contract/child" "[2]" root nil nil)
        failing (test-util/insert-workflow! storage "contract/failing" "[3]" root nil nil)
        failed (test-util/insert-workflow! storage "contract/failed" "[5]" nil nil nil)
        expiring (test-util/insert-workflow! storage "contract/expiring" "[4]" nil nil 0)]
    (testing "start-workflow! of a new workflow starts it"
      (is (every? int? [root child failing failed expiring]))
      (let [wf (db/start-workflow! storage "contract/started" "[6]" root "{:actor 5}" 0)]
        (check-shape start-shape wf)
        (is (= {:start/outcome :started :workflow/name "contract/started" :workflow/params "[6]"
                :workflow/state "STARTED" :workflow/parent-invocation-id root :workflow/metadata "{:actor 5}"
                :workflow/result nil :workflow/expired true}
               (select-keys wf [:start/outcome :workflow/name :workflow/params :workflow/state
                                :workflow/parent-invocation-id :workflow/metadata :workflow/result
                                :workflow/expired])))
        (is (= (dissoc wf :start/outcome) (db/get-workflow storage (:workflow/invocation-id wf)))
            "the row get-workflow returns")))
    (testing "start-workflow! of a running workflow returns it, as running"
      (let [wf (db/start-workflow! storage "contract/root" "[1]" nil nil nil)]
        (check-shape start-shape wf)
        (is (= {:start/outcome :running :workflow/invocation-id root :workflow/state "STARTED"}
               (select-keys wf [:start/outcome :workflow/invocation-id :workflow/state])))))
    (testing "finish! returns whether it finished the invocation"
      (is (true? (db/finish! storage child "DONE" "42")))
      (is (true? (db/finish! storage failing "ERROR" "{:message \"boom\"}")))
      (is (true? (db/finish! storage failed "ERROR" "{:message \"bad\"}")))
      (is (false? (db/finish! storage child "DONE" "43"))))
    (let [expired (db/get-workflow storage expiring)]
      (testing "get-workflow of an expired lease"
        (check-shape get-workflow-shape expired)
        (is (true? (:workflow/expired expired)) "expired")))
    (testing "time-out! returns whether it timed the invocation out"
      (is (false? (db/time-out! storage root "{:message \"timed out\"}")))
      (is (true? (db/time-out! storage expiring "{:message \"timed out\"}"))))
    (testing "restart! returns the new invocation id, nil unless DONE, ERROR or TIMEOUT"
      (is (int? (db/restart! storage failing "{:actor 2}" nil)))
      (is (nil? (db/restart! storage failing nil nil))))
    (testing "start-workflow! of a finished workflow reuses it"
      (let [wf (db/start-workflow! storage "contract/child" "[2]" root "{:actor 4}" nil)]
        (check-shape start-shape wf)
        (is (= {:start/outcome :reused :workflow/invocation-id child :workflow/state "DONE" :workflow/result "42"}
               (select-keys wf [:start/outcome :workflow/invocation-id :workflow/state :workflow/result])))))
    (testing "record-reuse! returns nil"
      (is (nil? (db/record-reuse! storage child root "{:actor 3}")))
      (is (nil? (db/record-reuse! storage child nil nil))))

    (let [ids [root child failing expiring]]
      (check-rows "get-workflow" get-workflow-shape (mapv #(db/get-workflow storage %) ids))
      (check-rows "find-by-wf-def-and-params" workflow-shape
                  (mapv #(db/find-by-wf-def-and-params storage %1 %2)
                        ["contract/root" "contract/child" "contract/failing" "contract/expiring"]
                        ["[1]" "[2]" "[3]" "[4]"]))
      (testing "a missing workflow is nil"
        (is (nil? (db/get-workflow storage Long/MAX_VALUE)))
        (is (nil? (db/find-by-wf-def-and-params storage "contract/none" "[]")))))

    (let [calls (db/top-level-workflows storage {:top-level-only? false})]
      (check-rows "top-level-workflows" call-shape calls)
      (doseq [filters [nil {} {:state "STARTED"} {:state "ERROR"} {:state "TIMEOUT"} {:state "ERROR" :top-level-only? false}
                       {:state "DONE" :top-level-only? false} {:wf-def "contract/child"}
                       {:wf-def "contract/child" :top-level-only? false} {:limit 1}
                       {:before (:event/call-id (first calls)) :top-level-only? false}]]
        (testing (str "top-level-workflows " filters)
          (let [rows (if (nil? filters) (db/top-level-workflows storage) (db/top-level-workflows storage filters))]
            (is (seq rows))
            (doseq [row rows] (check-shape call-shape row)))))
      (testing "filtering by TIMEOUT only matches workflows whose latest run timed out"
        (is (= ["contract/expiring"]
               (mapv :workflow/name (db/top-level-workflows storage {:state "TIMEOUT" :top-level-only? false})))))
      (testing "filtering by ERROR only matches workflows whose latest run is in ERROR"
        (is (= ["contract/failed"]
               (mapv :workflow/name (db/top-level-workflows storage {:state "ERROR" :top-level-only? false}))))))

    (check-rows "full-timeline" timeline-shape (db/full-timeline storage root))

    (testing "top-level-wf-defs"
      (is (= ["contract/child" "contract/expiring" "contract/failed" "contract/failing" "contract/root"]
             (db/top-level-wf-defs storage false)))
      (is (= ["contract/child" "contract/expiring" "contract/failed" "contract/root"]
             (db/top-level-wf-defs storage true))
          "child was reused at the top level"))))

(defn check-timeouts
  "Runs the suite of the tenon.workflow.db/Timeouts methods against storage,
   a fresh and empty implementation of both Storage and Timeouts."
  [storage]
  (db/init-db! storage)
  (let [insert! (fn [wf-def parent timeout-ms]
                  (test-util/insert-workflow! storage (str "timeouts/" wf-def) "[]" parent
                                       (when (= "dead2" wf-def) "{:actor 1}") timeout-ms))
        leased (insert! "leased" nil 60000)
        nested (insert! "nested" leased 60000)
        unleased (insert! "unleased" nil nil)
        done (insert! "done" nil 60000)
        failing (insert! "failing" nil 60000)
        dead1 (insert! "dead1" nil 0)
        dead2 (insert! "dead2" dead1 0)
        expires-at #(:workflow/expires-at (db/get-workflow storage %))]
    (db/finish! storage done "DONE" "1")
    (db/finish! storage failing "ERROR" "{:message \"boom\"}")
    (let [restarted (db/restart! storage failing nil 60000)]

      (testing "list-expired lists the STARTED workflows whose lease ran out, oldest first, at any nesting"
        (let [rows (db/list-expired storage)]
          (is (= [dead1 dead2] (mapv :workflow/invocation-id rows))
              "not the ones with time left, without a lease, finished or restarted")
          (doseq [row rows]
            (check-shape get-workflow-shape row)
            (is (true? (:workflow/expired row))))))

      (testing "extend-lease! of nothing extends nothing"
        (is (= #{} (db/extend-lease! storage #{} 90000))))

      (testing "extend-lease! takes any number of invocations"
        (is (= #{leased} (db/extend-lease! storage (conj (set (range 1000000 1050000)) leased) 60000))))

      (testing "extend-lease! returns the invocations it extended"
        (is (= #{leased nested restarted}
               (db/extend-lease! storage #{leased nested restarted unleased done failing Long/MAX_VALUE} 90000))
            "not the ones without a lease, finished, restarted since or missing")
        (is (nil? (expires-at unleased)) "the never-expiring one still never expires")
        (is (= "DONE" (:workflow/state (db/get-workflow storage done)))))

      (testing "extend-lease! sets the lease end to timeout-ms from now"
        (let [long-lease (expires-at leased)]
          (is (= #{leased} (db/extend-lease! storage #{leased} 1000)))
          (is (< (expires-at leased) long-lease) "also when that is sooner")
          (is (false? (:workflow/expired (db/get-workflow storage leased))))))

      (testing "extend-lease! renews a lease that ran out but was not timed out yet"
        (is (= #{dead1} (db/extend-lease! storage #{dead1} 60000)))
        (is (false? (:workflow/expired (db/get-workflow storage dead1))))
        (is (false? (db/time-out! storage dead1 "{:message \"timed out\"}")) "it is not expired any more")
        (is (= [dead2] (mapv :workflow/invocation-id (db/list-expired storage)))))

      (testing "extend-lease! leaves a timed out invocation timed out"
        (is (true? (db/time-out! storage dead2 "{:message \"timed out\"}")))
        (is (= #{} (db/extend-lease! storage #{dead2} 60000)))
        (is (= "TIMEOUT" (:workflow/state (db/get-workflow storage dead2))))
        (is (= [] (db/list-expired storage)) "no longer STARTED"))

      (testing "extend-lease! applies to the new invocation of a restarted workflow only"
        (let [again (db/restart! storage dead2 nil 60000)]
          (is (= #{again} (db/extend-lease! storage #{dead2 again} 60000))))))))

(defn check-listing
  "Runs the suite of top-level-workflows' filters against storage, a fresh
   and empty Storage: whatever the combination of filters, it lists the
   calls of the unfiltered listing that match them, in the same order -
   over a randomly (but always the same way) built history of nested,
   running, done, failed, timed out, restarted and reused workflows."
  [storage]
  (db/init-db! storage)
  (let [rnd (java.util.Random. 42)
        pick (fn [coll] (nth coll (.nextInt rnd (count coll))))
        wf-defs ["listing/a" "listing/b" "listing/c"]
        ids (reduce
             (fn [ids i]
               (let [parent (when (and (seq ids) (zero? (.nextInt rnd 3))) (pick ids))
                     id (test-util/insert-workflow! storage (pick wf-defs) (str "[" i "]") parent nil (pick [nil 60000 0]))]
                 (case (.nextInt rnd 6)
                   0 (db/finish! storage id "DONE" "1")
                   1 (db/finish! storage id "ERROR" "{:message \"boom\"}")
                   2 (db/time-out! storage id "{:message \"timed out\"}")
                   3 (do (db/finish! storage id "ERROR" "{:message \"boom\"}") (db/restart! storage id nil nil))
                   4 (do (db/time-out! storage id "{:message \"timed out\"}") (db/restart! storage id nil 0)
                         (db/time-out! storage id "{:message \"timed out\"}"))
                   nil)
                 (conj ids id)))
             [] (range 90))]
    (dotimes [_ 120]
      (db/record-reuse! storage (pick ids) (when (zero? (.nextInt rnd 2)) (pick ids)) nil))
    (let [call-ids #(mapv :event/call-id %)
          everything (db/top-level-workflows storage {:top-level-only? false})]
      (testing "the history has calls in every state, reused ones too"
        (doseq [state ["STARTED" "DONE" "ERROR" "TIMEOUT"]]
          (is (some #(= state (:workflow/state %)) everything) state)
          (is (some #(and (= state (:workflow/state %)) (:event/reused %)) everything) (str state " reused"))))
      (doseq [state [nil "STARTED" "DONE" "ERROR" "TIMEOUT"]
              wf-def [nil "listing/a"]
              top-level-only? [true false]
              limit [nil 4]
              :let [baseline (db/top-level-workflows storage (cond-> {:top-level-only? top-level-only?}
                                                               wf-def (assoc :wf-def wf-def)))
                    matching (filter #(or (nil? state) (= state (:workflow/state %))) baseline)]
              before [nil (:event/call-id (nth matching (quot (count matching) 2) nil))]]
        (let [filters (cond-> {:top-level-only? top-level-only?}
                        state (assoc :state state)
                        wf-def (assoc :wf-def wf-def)
                        limit (assoc :limit limit)
                        before (assoc :before before))
              expected (cond->> (filter #(or (nil? before) (< (:event/call-id %) before)) matching)
                         limit (take (inc limit)))]
          (is (= (call-ids expected) (call-ids (db/top-level-workflows storage filters)))
              (pr-str filters)))))))
