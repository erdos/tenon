(ns tenon.workflow.db-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.edn :as edn]
            [next.jdbc :as jdbc]
            [tenon.workflow.db :as db]
            [tenon.workflow.test-util :as test-util :refer [temp-db-fixture]]))

(use-fixtures :each temp-db-fixture)

(deftest insert-and-fetch-workflow-test
  (let [id (db/insert-workflow! (test-util/ds) "test.ns/foo" (pr-str [1 2]) nil nil nil)
        wf (db/get-workflow (test-util/ds) id)]
    (is (integer? id))
    (is (= "test.ns/foo" (:wf_def wf)))
    (is (= [1 2] (edn/read-string (:params wf))))
    (is (= "STARTED" (:state wf)) "a workflow is inserted already STARTED")
    (is (integer? (:state_changed_at wf)) "state_changed_at is supplied by the db")
    (is (nil? (:expires_at wf)) "nil timeout-ms never expires")
    (is (nil? (:parent_invocation_id wf)))
    (is (nil? (:metadata wf)))))

(deftest insert-workflow-dedups-on-wf-def-and-params-test
  (let [id (db/insert-workflow! (test-util/ds) "test.ns/dup" (pr-str [1]) nil nil nil)]
    (is (nil? (db/insert-workflow! (test-util/ds) "test.ns/dup" (pr-str [1]) nil nil nil))
        "same (wf_def, params) - nothing inserted")
    (is (some? (db/insert-workflow! (test-util/ds) "test.ns/dup" (pr-str [2]) nil nil nil)))
    (is (= id (:invocation_id (db/find-by-wf-def-and-params (test-util/ds) "test.ns/dup" (pr-str [1])))))
    (is (nil? (db/find-by-wf-def-and-params (test-util/ds) "test.ns/dup" (pr-str [3]))))))

(deftest insert-workflow-stores-metadata-parent-and-lease-test
  (let [parent (test-util/insert! "test.ns/parent")
        child (db/insert-workflow! (test-util/ds) "test.ns/child" (pr-str []) parent (pr-str {:actor-id 42}) 1000)
        wf (db/get-workflow (test-util/ds) child)]
    (is (= parent (:parent_invocation_id wf)))
    (is (= {:actor-id 42} (edn/read-string (:metadata wf))))
    (is (= 1000 (- (:expires_at wf) (:state_changed_at wf))))))

(deftest finish-is-compare-and-set-test
  (let [id (test-util/insert! "test.ns/finish" :timeout-ms 60000)]
    (is (true? (db/finish! (test-util/ds) id "DONE" (pr-str 42))))
    (is (false? (db/finish! (test-util/ds) id "ERROR" (pr-str {:message "x"})))
        "no longer STARTED")
    (let [wf (db/get-workflow (test-util/ds) id)]
      (is (= "DONE" (:state wf)))
      (is (= 42 (edn/read-string (:result wf))))
      (is (nil? (:expires_at wf)) "the lease is cleared when leaving STARTED"))
    (is (= [["STARTED" nil] ["DONE" (pr-str 42)]]
           (mapv (juxt :state :data) (test-util/history (test-util/ds) id))))))

(deftest time-out-only-past-the-lease-test
  (let [id (test-util/insert! "test.ns/lease" :timeout-ms 60000)]
    (is (false? (db/time-out! (test-util/ds) id (pr-str {:message "timed out"})))
        "lease not expired yet")
    (test-util/expire! (test-util/ds) id)
    (is (true? (db/time-out! (test-util/ds) id (pr-str {:message "timed out"}))))
    (is (= "ERROR" (:state (db/get-workflow (test-util/ds) id)))))
  (let [id (test-util/insert! "test.ns/no-lease")]
    (is (false? (db/time-out! (test-util/ds) id (pr-str {})))
        "no lease never expires")))

(deftest restart-assigns-new-invocation-id-test
  (let [id (test-util/insert! "test.ns/restart" :metadata (pr-str {:run 1}))]
    (is (nil? (db/restart! (test-util/ds) id nil nil)) "STARTED can not be restarted")
    (db/finish! (test-util/ds) id "ERROR" (pr-str {:message "first"}))
    (let [id2 (db/restart! (test-util/ds) id (pr-str {:run 2}) 5000)]
      (is (> id2 id))
      (is (nil? (db/restart! (test-util/ds) id nil nil))
          "the old invocation id no longer matches - a concurrent restart loses")
      (let [wf (db/get-workflow (test-util/ds) id2)]
        (is (= "STARTED" (:state wf)))
        (is (nil? (:result wf)))
        (is (= {:run 2} (edn/read-string (:metadata wf))))
        (is (some? (:expires_at wf))))
      (is (= id2 (:invocation_id (db/get-workflow (test-util/ds) id)))
          "an old invocation id resolves to the current row")
      (db/finish! (test-util/ds) id2 "DONE" (pr-str :ok))
      (is (= [[id "STARTED" (pr-str {:run 1})]
              [id "ERROR" (pr-str {:message "first"})]
              [id2 "STARTED" (pr-str {:run 2})]
              [id2 "DONE" (pr-str :ok)]]
             (mapv (juxt :invocation_id :state :data) (test-util/history (test-util/ds) id2)))))))

(deftest restart-never-reuses-invocation-ids-test
  (let [a (test-util/insert! "test.ns/a")
        b (test-util/insert! "test.ns/b")]
    (db/finish! (test-util/ds) a "DONE" "1")
    (let [a2 (db/restart! (test-util/ds) a nil nil)]
      (db/finish! (test-util/ds) a2 "DONE" "1")
      (let [a3 (db/restart! (test-util/ds) a2 nil nil)
            c (test-util/insert! "test.ns/c")]
        (is (< a b a2 a3 c) "restarts and inserts share one increasing sequence")))))

(deftest lease-only-update-is-not-logged-test
  (let [id (test-util/insert! "test.ns/renew" :timeout-ms 1000)]
    (jdbc/execute! (test-util/ds) ["UPDATE workflow SET expires_at = expires_at + 1000 WHERE invocation_id = ?" id])
    (is (= ["STARTED"] (mapv :state (test-util/history (test-util/ds) id))))))

(deftest expires-at-must-be-cleared-outside-started-test
  (let [id (test-util/insert! "test.ns/check")]
    (is (thrown? Exception
          (jdbc/execute! (test-util/ds) ["UPDATE workflow SET state = 'DONE', expires_at = 1 WHERE invocation_id = ?" id])))))

(deftest history-requires-existing-workflow-test
  (is (thrown? Exception
        (jdbc/execute! (test-util/ds) ["INSERT INTO workflow_history (idempotence_key, wf_invocation_id, state, state_changed_at)
                                        VALUES (x'00', 1, 'DONE', 0)"]))
      "foreign keys are enforced"))

(deftest top-level-workflows-seek-pagination-test
  ;; 10 rows, paged 4 at a time via :limit/:before - every page after the
  ;; first must be requested with :before set to the previous page's last
  ;; row, and the pages together must cover every row exactly once (no
  ;; gaps, no repeats), with :limit n actually returning up to n+1 rows so
  ;; the caller can tell whether a further page exists.
  (dotimes [n 10]
    (test-util/insert! (str "test.ns/seek" n) :params (pr-str [n])))
  (let [page1 (db/top-level-workflows (test-util/ds) {:limit 4})
        trimmed1 (vec (take 4 page1))
        page2 (db/top-level-workflows (test-util/ds) {:limit 4 :before (:invocation_id (last trimmed1))})
        trimmed2 (vec (take 4 page2))
        page3 (db/top-level-workflows (test-util/ds) {:limit 4 :before (:invocation_id (last trimmed2))})]
    (is (= 5 (count page1)) "limit+1, so the caller can detect there's a next page")
    (is (= 5 (count page2)) "still more after page 2")
    (is (<= (count page3) 4) "nothing left beyond page 3")
    (let [all-ids (concat (map :invocation_id trimmed1) (map :invocation_id trimmed2) (map :invocation_id page3))]
      (is (= 10 (count all-ids)) "all 10 rows covered across the 3 pages")
      (is (= 10 (count (distinct all-ids))) "no row repeated across page boundaries")
      (is (= all-ids (sort > all-ids)) "newest first"))))

(deftest top-level-workflows-created-at-survives-restart-test
  (let [id (test-util/insert! "test.ns/created")
        created-at (:created_at (first (db/top-level-workflows (test-util/ds))))]
    (is (= created-at (:state_changed_at (db/get-workflow (test-util/ds) id))))
    (db/finish! (test-util/ds) id "DONE" "1")
    (Thread/sleep 5)
    (db/restart! (test-util/ds) id nil nil)
    (is (= created-at (:created_at (first (db/top-level-workflows (test-util/ds)))))
        "created_at is the start of the first invocation")))

(deftest full-timeline-follows-children-of-past-invocations-test
  ;; A child started under the parent's first invocation keeps pointing at
  ;; it after the parent is restarted - it must still show in the tree.
  (let [parent (test-util/insert! "test.ns/tl-parent")
        child (test-util/insert! "test.ns/tl-child" :parent parent)]
    (db/finish! (test-util/ds) child "DONE" "1")
    (db/finish! (test-util/ds) parent "DONE" "2")
    (let [parent2 (db/restart! (test-util/ds) parent nil nil)
          timeline (db/full-timeline (test-util/ds) parent2)]
      (is (= 5 (count timeline)) "parent: STARTED DONE STARTED, child: STARTED DONE")
      (is (= #{[0 "test.ns/tl-parent"] [1 "test.ns/tl-child"]}
             (set (map (juxt :depth :wf_def) timeline))))
      (is (= #{parent2} (set (map :current_invocation_id (filter #(zero? (:depth %)) timeline)))))
      (is (= timeline (db/full-timeline (test-util/ds) parent))
          "an old invocation id gives the same timeline"))))

(deftest get-workflow-flags-expired-lease-test
  (let [id (test-util/insert! "test.ns/expiry" :timeout-ms 60000)
        wf (db/get-workflow (test-util/ds) id)]
    (is (false? (:expired wf)))
    (is (< (abs (- (:state_changed_at wf) (System/currentTimeMillis))) 5000) "epoch milliseconds")
    (test-util/expire! (test-util/ds) id)
    (is (true? (:expired (db/get-workflow (test-util/ds) id))))
    (db/time-out! (test-util/ds) id (pr-str {}))
    (is (false? (:expired (db/get-workflow (test-util/ds) id))) "no lease outside STARTED"))
  (is (false? (:expired (db/get-workflow (test-util/ds) (test-util/insert! "test.ns/no-expiry"))))
      "no lease never expires"))

(deftest history-rows-carry-parent-invocation-id-test
  (let [parent (test-util/insert! "test.ns/hp-parent")
        child (test-util/insert! "test.ns/hp-child" :parent parent)]
    (db/finish! (test-util/ds) child "ERROR" (pr-str {:message "x"}))
    (db/finish! (test-util/ds) (db/restart! (test-util/ds) child nil nil) "DONE" "1")
    (is (= [parent parent parent parent]
           (mapv :parent_invocation_id (test-util/history (test-util/ds) child)))
        "every history state, not only the current row, names the parent")))

(deftest record-reuse-logs-reused-state-test
  (let [child (test-util/insert! "test.ns/reused")
        parent (test-util/insert! "test.ns/reuser")]
    (db/finish! (test-util/ds) child "DONE" (pr-str 1))
    (db/record-reuse! (test-util/ds) child parent (pr-str {:actor-id 7}))
    (is (= [[child "STARTED" nil nil]
            [child "DONE" (pr-str 1) nil]
            [child "REUSED" (pr-str {:actor-id 7}) parent]]
           (mapv (juxt :invocation_id :state :data :parent_invocation_id)
                 (test-util/history (test-util/ds) child))))
    (is (= "DONE" (:state (db/get-workflow (test-util/ds) child)))
        "the workflow row itself is untouched - still the current state")))

(deftest record-reuse-accepts-past-invocation-id-test
  ;; The served invocation may have been restarted between reading its
  ;; result and logging the reuse - the reuse still belongs to its workflow.
  (let [child (test-util/insert! "test.ns/reused-past")]
    (db/finish! (test-util/ds) child "DONE" "1")
    (db/restart! (test-util/ds) child nil nil)
    (db/record-reuse! (test-util/ds) child nil nil)
    (is (= [child "REUSED"]
           ((juxt :invocation_id :state) (last (test-util/history (test-util/ds) child)))))))

(deftest full-timeline-includes-reused-children-test
  (let [parent1 (test-util/insert! "test.ns/rt-parent1")
        child (test-util/insert! "test.ns/rt-child" :parent parent1)
        parent2 (test-util/insert! "test.ns/rt-parent2")]
    (db/finish! (test-util/ds) child "DONE" "1")
    (db/record-reuse! (test-util/ds) child parent2 nil)
    (let [timeline (db/full-timeline (test-util/ds) parent2)]
      (is (= [[0 parent2 nil "test.ns/rt-parent2" "STARTED"]
              [1 child parent2 "test.ns/rt-child" "REUSED"]]
             (map (juxt :depth :invocation_id :parent_invocation_id :wf_def :state) timeline))
          "only the REUSED row - the child ran under parent1, so its own states aren't in parent2's timeline"))
    (is (= [[0 child parent1 "STARTED"]
            [0 child parent1 "DONE"]
            [0 child parent2 "REUSED"]]
           (->> (db/full-timeline (test-util/ds) child)
                (map (juxt :depth :invocation_id :parent_invocation_id :state))
                (sort-by #({"STARTED" 0 "DONE" 1 "REUSED" 2} (peek %)))))
        "the child's own timeline still has its reuse")
    (is (= #{"test.ns/rt-parent1" "test.ns/rt-child"}
           (set (map :wf_def (db/full-timeline (test-util/ds) parent1))))
        "the child's states stay in the timeline of the parent it ran under")
    (db/finish! (test-util/ds) parent2 "DONE" "2")
    (is (some #(= "REUSED" (:state %))
              (db/full-timeline (test-util/ds) (db/restart! (test-util/ds) parent2 nil nil)))
        "still reached via the parent's past invocation")))
