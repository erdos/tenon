(ns tenon.workflow.storage-contract
  "A test suite any tenon.workflow.db/Storage implementation must pass:
   every method's return value has the documented shape - each required
   key present, each value of the documented type. Call check-storage
   from a deftest with a fresh, empty storage of the implementation under
   test."
  (:require [clojure.test :refer [is testing]]
            [tenon.workflow.db :as db]))

(defn- nilable [pred] (fn [v] (or (nil? v) (pred v))))

(def ^:private run-state? #{"STARTED" "DONE" "ERROR"})

(def workflow-shape
  "find-by-wf-def-and-params rows."
  {:invocation_id        int?
   :wf_def               string?
   :params               string?
   :parent_invocation_id (nilable int?)
   :state                run-state?
   :state_changed_at     int?
   :expires_at           (nilable int?)
   :metadata             (nilable string?)
   :result               (nilable string?)})

(def get-workflow-shape
  "get-workflow rows."
  (assoc workflow-shape :expired boolean?))

(def call-shape
  "top-level-workflows rows."
  (assoc workflow-shape
         :call_id    int?
         :created_at int?
         :reused     boolean?))

(def timeline-shape
  "full-timeline rows."
  {:invocation_id         int?
   :current_invocation_id int?
   :parent_invocation_id  (nilable int?)
   :wf_def                string?
   :state                 #{"STARTED" "DONE" "ERROR" "REUSED"}
   :state_changed_at      int?
   :data                  (nilable string?)
   :depth                 nat-int?})

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
  (let [root (db/insert-workflow! storage "contract/root" "[1]" nil "{:actor 1}" 60000)
        child (db/insert-workflow! storage "contract/child" "[2]" root nil nil)
        failing (db/insert-workflow! storage "contract/failing" "[3]" root nil nil)
        expiring (db/insert-workflow! storage "contract/expiring" "[4]" nil nil 0)]
    (testing "insert-workflow! returns the invocation id, nil for an existing workflow"
      (is (every? int? [root child failing expiring]))
      (is (nil? (db/insert-workflow! storage "contract/root" "[1]" nil nil nil))))
    (testing "finish! returns whether it finished the invocation"
      (is (true? (db/finish! storage child "DONE" "42")))
      (is (true? (db/finish! storage failing "ERROR" "{:message \"boom\"}")))
      (is (false? (db/finish! storage child "DONE" "43"))))
    (let [expired (db/get-workflow storage expiring)]
      (testing "get-workflow of an expired lease"
        (check-shape get-workflow-shape expired)
        (is (true? (:expired expired)) "expired")))
    (testing "time-out! returns whether it timed the invocation out"
      (is (false? (db/time-out! storage root "{:message \"timed out\"}")))
      (is (true? (db/time-out! storage expiring "{:message \"timed out\"}"))))
    (testing "restart! returns the new invocation id, nil unless DONE or ERROR"
      (is (int? (db/restart! storage failing "{:actor 2}" nil)))
      (is (nil? (db/restart! storage failing nil nil))))
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
      (doseq [filters [nil {} {:state "STARTED"} {:state "ERROR"} {:state "ERROR" :top-level-only? false}
                       {:state "DONE" :top-level-only? false} {:wf-def "contract/child"}
                       {:wf-def "contract/child" :top-level-only? false} {:limit 1}
                       {:before (:call_id (first calls)) :top-level-only? false}]]
        (testing (str "top-level-workflows " filters)
          (let [rows (if (nil? filters) (db/top-level-workflows storage) (db/top-level-workflows storage filters))]
            (is (seq rows))
            (doseq [row rows] (check-shape call-shape row)))))
      (testing "filtering by ERROR only matches workflows whose latest run is in ERROR"
        (is (= ["contract/expiring"]
               (mapv :wf_def (db/top-level-workflows storage {:state "ERROR" :top-level-only? false}))))))

    (check-rows "full-timeline" timeline-shape (db/full-timeline storage root))

    (testing "top-level-wf-defs"
      (is (= ["contract/child" "contract/expiring" "contract/failing" "contract/root"]
             (db/top-level-wf-defs storage false)))
      (is (= ["contract/child" "contract/expiring" "contract/root"] (db/top-level-wf-defs storage true))
          "child was reused at the top level"))))
