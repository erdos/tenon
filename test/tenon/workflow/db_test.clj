(ns tenon.workflow.db-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [matcher-combinators.test :refer [match?]]
            [matcher-combinators.matchers :as m]
            [next.jdbc :as jdbc]
            [tenon.workflow.db :as db]
            [tenon.workflow.test-util :as test-util :refer [temp-db-fixture]]))

(use-fixtures :each temp-db-fixture)

(deftest insert-and-fetch-workflow-test
  (let [id (db/insert-workflow! (test-util/ds) "test.ns/foo" (pr-str [1 2]) nil nil nil)
        wf (db/get-workflow (test-util/ds) id)]
    (is (integer? id))
    (is (match? {:workflow/name "test.ns/foo"
                 :workflow/params (pr-str [1 2])
                 :workflow/state "STARTED"
                 :workflow/state-changed-at integer?
                 :workflow/expires-at nil?
                 :workflow/parent-invocation-id nil?
                 :workflow/metadata nil?}
                wf)
        "a workflow is inserted already STARTED with db-supplied state_changed_at and nil lease")))

(deftest insert-workflow-dedups-on-wf-def-and-params-test
  (let [id (db/insert-workflow! (test-util/ds) "test.ns/dup" (pr-str [1]) nil nil nil)]
    (is (nil? (db/insert-workflow! (test-util/ds) "test.ns/dup" (pr-str [1]) nil nil nil))
        "same (wf_def, params) - nothing inserted")
    (is (some? (db/insert-workflow! (test-util/ds) "test.ns/dup" (pr-str [2]) nil nil nil)))
    (is (= id (:workflow/invocation-id (db/find-by-wf-def-and-params (test-util/ds) "test.ns/dup" (pr-str [1])))))
    (is (nil? (db/find-by-wf-def-and-params (test-util/ds) "test.ns/dup" (pr-str [3]))))))

(deftest insert-workflow-stores-metadata-parent-and-lease-test
  (let [parent (test-util/insert! "test.ns/parent")
        child (db/insert-workflow! (test-util/ds) "test.ns/child" (pr-str []) parent (pr-str {:actor-id 42}) 1000)
        wf (db/get-workflow (test-util/ds) child)]
    (is (match? {:workflow/parent-invocation-id parent
                 :workflow/metadata (pr-str {:actor-id 42})}
                wf))
    (is (= 1000 (- (:workflow/expires-at wf) (:workflow/state-changed-at wf))))))

(deftest finish-is-compare-and-set-test
  (let [id (test-util/insert! "test.ns/finish" :timeout-ms 60000)]
    (is (true? (db/finish! (test-util/ds) id "DONE" (pr-str 42))))
    (is (false? (db/finish! (test-util/ds) id "ERROR" (pr-str {:message "x"})))
        "no longer STARTED")
    (is (match? {:workflow/state "DONE"
                 :workflow/result (pr-str 42)
                 :workflow/expires-at nil?}
                (db/get-workflow (test-util/ds) id))
        "the lease is cleared when leaving STARTED")
    (is (match? [{:workflow/state "STARTED" :event/data nil}
                 {:workflow/state "DONE" :event/data (pr-str 42)}]
                (test-util/history (test-util/ds) id)))))

(deftest time-out-only-past-the-lease-test
  (let [id (test-util/insert! "test.ns/lease" :timeout-ms 60000)]
    (is (false? (db/time-out! (test-util/ds) id (pr-str {:message "timed out"})))
        "lease not expired yet")
    (test-util/expire! (test-util/ds) id)
    (is (true? (db/time-out! (test-util/ds) id (pr-str {:message "timed out"}))))
    (is (= "TIMEOUT" (:workflow/state (db/get-workflow (test-util/ds) id)))))
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
      (is (match? {:workflow/state "STARTED"
                   :workflow/result nil?
                   :workflow/metadata (pr-str {:run 2})
                   :workflow/expires-at some?}
                  (db/get-workflow (test-util/ds) id2)))
      (is (= id2 (:workflow/invocation-id (db/get-workflow (test-util/ds) id)))
          "an old invocation id resolves to the current row")
      (db/finish! (test-util/ds) id2 "DONE" (pr-str :ok))
      (is (match? [{:workflow/invocation-id id :workflow/state "STARTED" :event/data (pr-str {:run 1})}
                   {:workflow/invocation-id id :workflow/state "ERROR" :event/data (pr-str {:message "first"})}
                   {:workflow/invocation-id id2 :workflow/state "STARTED" :event/data (pr-str {:run 2})}
                   {:workflow/invocation-id id2 :workflow/state "DONE" :event/data (pr-str :ok)}]
                  (test-util/history (test-util/ds) id2))))))

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
    (is (match? [{:workflow/state "STARTED"}] (test-util/history (test-util/ds) id)))))

(deftest expires-at-must-be-cleared-outside-started-test
  (let [id (test-util/insert! "test.ns/check")]
    (test-util/finish! id "DONE" "1")
    (is (thrown? Exception
          (jdbc/execute! (test-util/ds) ["UPDATE event SET expires_at = 1 WHERE state = 1"])))))

(deftest list-calls-seek-pagination-test
  ;; 10 rows, paged 4 at a time via :limit/:before - every page after the
  ;; first must be requested with :before set to the previous page's last
  ;; row, and the pages together must cover every row exactly once (no
  ;; gaps, no repeats), with :limit n actually returning up to n+1 rows so
  ;; the caller can tell whether a further page exists.
  (dotimes [n 10]
    (test-util/insert! (str "test.ns/seek" n) :params (pr-str [n])))
  (let [page1 (db/list-calls (test-util/ds) {:limit 4})
        trimmed1 (vec (take 4 page1))
        page2 (db/list-calls (test-util/ds) {:limit 4 :before (:event/call-id (last trimmed1))})
        trimmed2 (vec (take 4 page2))
        page3 (db/list-calls (test-util/ds) {:limit 4 :before (:event/call-id (last trimmed2))})]
    (is (= 5 (count page1)) "limit+1, so the caller can detect there's a next page")
    (is (= 5 (count page2)) "still more after page 2")
    (is (<= (count page3) 4) "nothing left beyond page 3")
    (let [all-ids (concat (map :workflow/invocation-id trimmed1) (map :workflow/invocation-id trimmed2) (map :workflow/invocation-id page3))]
      (is (= 10 (count all-ids)) "all 10 rows covered across the 3 pages")
      (is (= 10 (count (distinct all-ids))) "no row repeated across page boundaries")
      (is (= all-ids (sort > all-ids)) "newest first"))))

(deftest list-calls-created-at-survives-restart-test
  (let [id (test-util/insert! "test.ns/created")
        created-at (:event/created-at (first (db/list-calls (test-util/ds) nil)))]
    (is (= created-at (:workflow/state-changed-at (db/get-workflow (test-util/ds) id))))
    (db/finish! (test-util/ds) id "DONE" "1")
    (Thread/sleep 5)
    (db/restart! (test-util/ds) id nil nil)
    (is (= created-at (:event/created-at (first (db/list-calls (test-util/ds) nil))))
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
             (set (map (juxt :workflow/depth :workflow/name) timeline))))
      (is (match? [{:workflow/current-invocation-id parent2}
                   {:workflow/current-invocation-id parent2}
                   {:workflow/current-invocation-id parent2}]
                  (filter #(zero? (:workflow/depth %)) timeline)))
      (is (= timeline (db/full-timeline (test-util/ds) parent))
          "an old invocation id gives the same timeline"))))

(deftest get-workflow-flags-expired-lease-test
  (let [id (test-util/insert! "test.ns/expiry" :timeout-ms 60000)
        wf (db/get-workflow (test-util/ds) id)]
    (is (false? (:workflow/expired wf)))
    (is (< (abs (- (:workflow/state-changed-at wf) (System/currentTimeMillis))) 5000) "epoch milliseconds")
    (test-util/expire! (test-util/ds) id)
    (is (true? (:workflow/expired (db/get-workflow (test-util/ds) id))))
    (db/time-out! (test-util/ds) id (pr-str {}))
    (is (false? (:workflow/expired (db/get-workflow (test-util/ds) id))) "no lease outside STARTED"))
  (is (false? (:workflow/expired (db/get-workflow (test-util/ds) (test-util/insert! "test.ns/no-expiry"))))
      "no lease never expires"))

(deftest history-rows-carry-parent-invocation-id-test
  (let [parent (test-util/insert! "test.ns/hp-parent")
        child (test-util/insert! "test.ns/hp-child" :parent parent)]
    (db/finish! (test-util/ds) child "ERROR" (pr-str {:message "x"}))
    (db/finish! (test-util/ds) (db/restart! (test-util/ds) child nil nil) "DONE" "1")
    (is (match? [{:workflow/parent-invocation-id parent}
                 {:workflow/parent-invocation-id parent}
                 {:workflow/parent-invocation-id parent}
                 {:workflow/parent-invocation-id parent}]
                (test-util/history (test-util/ds) child))
        "every history state, not only the current row, names the parent")))

(deftest record-reuse-logs-reused-state-test
  (let [child (test-util/insert! "test.ns/reused")
        parent (test-util/insert! "test.ns/reuser")]
    (db/finish! (test-util/ds) child "DONE" (pr-str 1))
    (db/record-reuse! (test-util/ds) child parent (pr-str {:actor-id 7}))
    (is (match? [{:workflow/invocation-id child :workflow/state "STARTED" :event/data nil :workflow/parent-invocation-id nil?}
                 {:workflow/invocation-id child :workflow/state "DONE" :event/data (pr-str 1) :workflow/parent-invocation-id nil?}
                 {:workflow/invocation-id child :workflow/state "REUSED" :event/data (pr-str {:actor-id 7}) :workflow/parent-invocation-id parent}]
                (test-util/history (test-util/ds) child)))
    (is (= "DONE" (:workflow/state (db/get-workflow (test-util/ds) child)))
        "the workflow row itself is untouched - still the current state")))

(deftest record-reuse-accepts-past-invocation-id-test
  ;; The served invocation may have been restarted between reading its
  ;; result and logging the reuse - the reuse still belongs to its workflow.
  (let [child (test-util/insert! "test.ns/reused-past")]
    (db/finish! (test-util/ds) child "DONE" "1")
    (db/restart! (test-util/ds) child nil nil)
    (db/record-reuse! (test-util/ds) child nil nil)
    (is (match? {:workflow/invocation-id child :workflow/state "REUSED"}
                (last (test-util/history (test-util/ds) child))))))

(deftest full-timeline-includes-reused-children-test
  (let [parent1 (test-util/insert! "test.ns/rt-parent1")
        child (test-util/insert! "test.ns/rt-child" :parent parent1)
        parent2 (test-util/insert! "test.ns/rt-parent2")]
    (db/finish! (test-util/ds) child "DONE" "1")
    (db/record-reuse! (test-util/ds) child parent2 nil)
    (let [timeline (db/full-timeline (test-util/ds) parent2)]
      (is (match? [{:workflow/depth 0 :workflow/invocation-id parent2 :workflow/parent-invocation-id nil? :workflow/name "test.ns/rt-parent2" :workflow/state "STARTED"}
                   {:workflow/depth 1 :workflow/invocation-id child :workflow/parent-invocation-id parent2 :workflow/name "test.ns/rt-child" :workflow/state "REUSED"}]
                  timeline)
          "only the REUSED row - the child ran under parent1, so its own states aren't in parent2's timeline"))
    (is (match? (m/in-any-order [{:workflow/depth 0 :workflow/invocation-id child :workflow/parent-invocation-id parent1 :workflow/state "STARTED"}
                                 {:workflow/depth 0 :workflow/invocation-id child :workflow/parent-invocation-id parent1 :workflow/state "DONE"}
                                 {:workflow/depth 0 :workflow/invocation-id child :workflow/parent-invocation-id parent2 :workflow/state "REUSED"}])
                (db/full-timeline (test-util/ds) child))
        "the child's own timeline still has its reuse")
    (is (= #{"test.ns/rt-parent1" "test.ns/rt-child"}
           (set (map :workflow/name (db/full-timeline (test-util/ds) parent1))))
        "the child's states stay in the timeline of the parent it ran under")
    (db/finish! (test-util/ds) parent2 "DONE" "2")
    (is (some #(= "REUSED" (:workflow/state %))
              (db/full-timeline (test-util/ds) (db/restart! (test-util/ds) parent2 nil nil)))
        "still reached via the parent's past invocation")))

(deftest list-calls-lists-calls-test
  ;; One call per workflow (its first start - restarts aren't new calls)
  ;; plus one per reuse of its result, newest first.
  (let [a (test-util/insert! "test.ns/calls-a")
        b (test-util/insert! "test.ns/calls-b" :parent a)]
    (test-util/finish! a "DONE" "1")
    (test-util/finish! b "DONE" "2")
    (test-util/record-reuse! a nil)
    (test-util/record-reuse! b a)
    (let [a' (test-util/restart! a)]
      (is (match? [{:workflow/invocation-id b :workflow/name "test.ns/calls-b" :workflow/parent-invocation-id a :workflow/state "DONE" :event/reused true}
                   {:workflow/invocation-id a' :workflow/name "test.ns/calls-a" :workflow/parent-invocation-id nil? :workflow/state "STARTED" :event/reused true}
                   {:workflow/invocation-id b :workflow/name "test.ns/calls-b" :workflow/parent-invocation-id a :workflow/state "DONE" :event/reused false}
                   {:workflow/invocation-id a' :workflow/name "test.ns/calls-a" :workflow/parent-invocation-id nil? :workflow/state "STARTED" :event/reused false}]
                  (db/list-calls (test-util/ds) {:top-level-only? false})))
      (is (match? [{:workflow/invocation-id a' :workflow/name "test.ns/calls-a" :workflow/parent-invocation-id nil? :workflow/state "STARTED" :event/reused true}
                   {:workflow/invocation-id a' :workflow/name "test.ns/calls-a" :workflow/parent-invocation-id nil? :workflow/state "STARTED" :event/reused false}]
                  (db/list-calls (test-util/ds) {})))
      (let [expected-done [{:workflow/invocation-id b :workflow/name "test.ns/calls-b" :workflow/parent-invocation-id a :workflow/state "DONE" :event/reused true}
                           {:workflow/invocation-id b :workflow/name "test.ns/calls-b" :workflow/parent-invocation-id a :workflow/state "DONE" :event/reused false}]]
        (is (match? expected-done
                    (db/list-calls (test-util/ds) {:top-level-only? false :state "DONE"})))
        (is (match? expected-done
                    (db/list-calls (test-util/ds) {:top-level-only? false :wf-def "test.ns/calls-b"}))))
      (let [[c1 c2 c3 c4] (db/list-calls (test-util/ds) {:top-level-only? false})]
        (is (= [c3 c4] (db/list-calls (test-util/ds) {:top-level-only? false :before (:event/call-id c2)})))
        (is (= [c1 c2 c3] (db/list-calls (test-util/ds) {:top-level-only? false :limit 2}))
            "limit+1, so the caller can detect there's a next page")))))

(deftest list-wf-defs-include-top-level-reuses-test
  (let [parent (test-util/insert! "test.ns/defs-parent")
        child (test-util/insert! "test.ns/defs-child" :parent parent)]
    (is (= ["test.ns/defs-parent"] (db/list-wf-defs (test-util/ds) true)))
    (test-util/finish! child "DONE" "1")
    (test-util/record-reuse! child nil)
    (is (= ["test.ns/defs-child" "test.ns/defs-parent"] (db/list-wf-defs (test-util/ds) true)))))

(deftest concurrent-inserts-of-one-workflow-insert-once-test
  (let [results (->> (range 8)
                     (mapv (fn [_] (future (db/insert-workflow! (test-util/ds) "test.ns/race" "[1]" nil nil nil))))
                     (mapv deref))]
    (is (= 1 (count (remove nil? results))) "exactly one inserter wins")
    (is (= 7 (count (filter nil? results))))))

(deftest concurrent-restarts-of-one-workflow-restart-once-test
  (let [id (test-util/insert! "test.ns/restart-race")
        _ (db/finish! (test-util/ds) id "DONE" "1")
        results (->> (range 8)
                     (mapv (fn [_] (future (db/restart! (test-util/ds) id nil nil))))
                     (mapv deref))]
    (is (= 1 (count (remove nil? results))) "exactly one restarter wins")
    (is (match? {:workflow/invocation-id (first (remove nil? results)) :workflow/state "STARTED"}
                (db/get-workflow (test-util/ds) id)))
    (is (match? [{:workflow/state "STARTED"} {:workflow/state "DONE"} {:workflow/state "STARTED"}] (test-util/history (test-util/ds) id)))))

(defn- populate!
  "A varied set of workflows: three wf_defs, nested and top-level, DONE,
   ERROR and STARTED, some restarted, many reused from the top level and
   from within other workflows. Returns nothing."
  []
  (let [ds (test-util/ds)
        defs ["test.ns/a" "test.ns/b" "test.ns/c"]
        ids (reduce (fn [ids i]
                      (let [parent (when (and (pos? i) (zero? (mod i 3))) (nth ids (quot i 2)))
                            id (db/insert-workflow! ds (nth defs (mod i 3)) (pr-str [i]) parent nil nil)]
                        (conj ids id)))
                    [] (range 45))]
    (doseq [[i id] (map-indexed vector ids)]
      (case (mod i 5)
        0 nil
        1 (db/finish! ds id "ERROR" "{}")
        (db/finish! ds id "DONE" "1")))
    (doseq [[i id] (map-indexed vector ids)]
      (when (zero? (mod i 4)) (db/restart! ds id nil nil)))
    (doseq [[i id] (map-indexed vector ids)]
      (dotimes [k (mod i 3)]
        (db/record-reuse! ds id (when (odd? (+ i k)) (nth ids (mod (* 7 i) 45))) nil)))
    (doseq [[i id] (map-indexed vector ids)]
      (when (and (zero? (mod i 8)) (zero? (mod i 4)))
        (let [wf (db/get-workflow ds id)]
          (when (= "STARTED" (:workflow/state wf)) (db/finish! ds (:workflow/invocation-id wf) "ERROR" "{}")
                (db/restart! ds (:workflow/invocation-id wf) nil nil)))))))

(deftest list-calls-filters-agree-with-unfiltered-listing-test
  ;; Every filter combination - whichever index it is answered from - must
  ;; equal filtering the unfiltered listing, which is the plain history walk.
  (populate!)
  (let [ds (test-util/ds)
        all (db/list-calls ds {:top-level-only? false})
        matches? (fn [{:keys [state wf-def top-level-only? before]} row]
                   (and (or (not top-level-only?) (nil? (:workflow/parent-invocation-id row)))
                        (or (nil? state) (= state (:workflow/state row)))
                        (or (nil? wf-def) (= wf-def (:workflow/name row)))
                        (or (nil? before) (< (:event/call-id row) before))))
        mid (:event/call-id (nth all (quot (count all) 2)))]
    (is (> (count all) 60))
    (is (= (sort > (map :event/call-id all)) (map :event/call-id all)) "newest first")
    (doseq [state [nil "STARTED" "DONE" "ERROR"]
            wf-def [nil "test.ns/a" "test.ns/b" "test.ns/c"]
            top-level-only? [true false]
            before [nil mid]
            limit [nil 4]]
      (let [filters {:state state :wf-def wf-def :top-level-only? top-level-only? :before before :limit limit}
            expected (cond->> (filter #(matches? filters %) all)
                       limit (take (inc limit)))
            actual (db/list-calls ds (into {} (remove (comp nil? val)) filters))]
        (is (= expected actual) (pr-str filters))))))

(deftest list-wf-defs-agree-with-calls-test
  (populate!)
  (let [ds (test-util/ds)
        all (db/list-calls ds {:top-level-only? false})]
    (is (= (vec (sort (distinct (map :workflow/name all)))) (db/list-wf-defs ds false)))
    (is (= (vec (sort (distinct (map :workflow/name (filter (comp nil? :workflow/parent-invocation-id) all)))))
           (db/list-wf-defs ds true)))))

(deftest full-timeline-covers-the-history-of-the-tree-test
  (populate!)
  (let [ds (test-util/ds)
        roots (->> (db/list-calls ds {:top-level-only? false})
                   (remove :event/reused)
                   (map :workflow/invocation-id))
        shape (juxt :workflow/invocation-id :workflow/state :workflow/state-changed-at :event/data)]
    (is (> (count roots) 40))
    (doseq [root roots]
      (let [rows (db/full-timeline ds root)
            own (filter #(= 0 (:workflow/depth %)) rows)]
        (is (= (map :workflow/state-changed-at rows) (sort (map :workflow/state-changed-at rows))) "chronological")
        (is (= (map shape (remove #(= "REUSED" (:workflow/state %)) (test-util/history ds root)))
               (map shape (remove #(= "REUSED" (:workflow/state %)) own)))
            "the workflow's own states, in order")
        (is (= (count (filter #(= "REUSED" (:workflow/state %)) (test-util/history ds root)))
               (count (filter #(= "REUSED" (:workflow/state %)) own)))
            "its own reuses are part of it")))))


(deftest full-timeline-places-reuses-of-the-root-by-where-they-came-from-test
  ;; A reuse of the root made from inside its tree is a call, a level below
  ;; the caller; one made from outside is the root's own history, depth 0.
  (let [ds (test-util/ds)
        root (test-util/insert! "test.ns/tl-root")
        child (test-util/insert! "test.ns/tl-child" :parent root)
        other (test-util/insert! "test.ns/tl-other")]
    (test-util/finish! child "DONE" "1")
    (test-util/finish! root "DONE" "1")
    (test-util/record-reuse! root child)
    (test-util/record-reuse! root other)
    (test-util/record-reuse! root nil)
    (is (match? [{:workflow/state "STARTED" :workflow/depth 0 :workflow/invocation-id root}
                 {:workflow/state "STARTED" :workflow/depth 1 :workflow/invocation-id child}
                 {:workflow/state "DONE" :workflow/depth 1}
                 {:workflow/state "DONE" :workflow/depth 0}
                 {:workflow/state "REUSED" :workflow/depth 2 :workflow/parent-invocation-id child}
                 {:workflow/state "REUSED" :workflow/depth 0 :workflow/parent-invocation-id other}
                 {:workflow/state "REUSED" :workflow/depth 0 :workflow/parent-invocation-id nil}]
                (db/full-timeline ds root)))))
