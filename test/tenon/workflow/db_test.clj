(ns tenon.workflow.db-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.edn :as edn]
            [tenon.workflow.db :as db]
            [tenon.workflow.test-util :as test-util :refer [temp-db-fixture]]))

(use-fixtures :each temp-db-fixture)

(deftest insert-and-fetch-workflow-test
  (db/insert-workflow! (test-util/ds) "id-1" "test.ns/foo" (pr-str [1 2]) nil nil)
  (let [wf (db/get-workflow (test-util/ds) "id-1")]
    (is (= "test.ns/foo" (:wf_def wf)))
    (is (= [1 2] (edn/read-string (:arguments wf))))
    (is (nil? (:parent_workflow_id wf)))))

(deftest insert-workflow-4-arity-defaults-metadata-to-nil-test
  (db/insert-workflow! (test-util/ds) "id-1b" "test.ns/no-meta" (pr-str []) nil nil)
  (is (nil? (:metadata (db/get-workflow (test-util/ds) "id-1b")))))

(deftest insert-workflow-stores-metadata-test
  (db/insert-workflow! (test-util/ds) "id-1c" "test.ns/with-meta" (pr-str []) nil (pr-str {:actor-id 42}))
  (is (= {:actor-id 42} (edn/read-string (:metadata (db/get-workflow (test-util/ds) "id-1c"))))))

(deftest insert-workflow-stores-parent-workflow-id-test
  (db/insert-workflow! (test-util/ds) "parent-1" "test.ns/parent" (pr-str []) nil nil)
  (db/insert-workflow! (test-util/ds) "child-1" "test.ns/child" (pr-str []) "parent-1" nil)
  (is (nil? (:parent_workflow_id (db/get-workflow (test-util/ds) "parent-1"))))
  (is (= "parent-1" (:parent_workflow_id (db/get-workflow (test-util/ds) "child-1")))))

(deftest insert-and-fetch-events-test
  (db/insert-workflow! (test-util/ds) "id-2" "test.ns/bar" (pr-str []) nil nil)
  (db/insert-event! (test-util/ds) "id-2" "STARTED" nil)
  (db/insert-event! (test-util/ds) "id-2" "DONE" (pr-str 42))
  (let [events (test-util/get-events (test-util/ds) "id-2")]
    (is (= ["STARTED" "DONE"] (mapv :state events)))
    (is (every? some? (map :changed_at events)) "changed_at is supplied by the db")
    (is (every? #(re-matches #"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}" %) (map :changed_at events))
        "changed_at has millisecond precision")
    (is (= 42 (edn/read-string (:payload (last events)))))))

(deftest latest-event-test
  (db/insert-workflow! (test-util/ds) "id-3" "test.ns/baz" (pr-str []) nil nil)
  (db/insert-event! (test-util/ds) "id-3" "STARTED" nil)
  (is (= "STARTED" (:state (db/latest-event (test-util/ds) "id-3"))))
  (db/insert-event! (test-util/ds) "id-3" "ERROR" (pr-str {:message "x"}))
  (is (= "ERROR" (:state (db/latest-event (test-util/ds) "id-3")))))

(deftest find-by-wf-def-test
  (db/insert-workflow! (test-util/ds) "id-4" "test.ns/qux" (pr-str []) nil nil)
  (is (= 1 (count (test-util/find-by-wf-def (test-util/ds) "test.ns/qux"))))
  (is (= 0 (count (test-util/find-by-wf-def (test-util/ds) "test.ns/nope")))))

(deftest top-level-workflows-seek-pagination-test
  ;; 10 rows, paged 4 at a time via :limit/:before - every page after the
  ;; first must be requested with :before set to the previous page's last
  ;; row, and the pages together must cover every row exactly once (no
  ;; gaps, no repeats), with :limit n actually returning up to n+1 rows so
  ;; the caller can tell whether a further page exists.
  (dotimes [n 10]
    (let [id (str "seek-" n)]
      (db/insert-workflow! (test-util/ds) id (str "test.ns/seek" n) (pr-str [n]) nil nil)
      (db/insert-event! (test-util/ds) id "STARTED" nil)))
  (let [page1 (db/top-level-workflows (test-util/ds) {:limit 4})
        trimmed1 (vec (take 4 page1))
        cursor1 {:created-at (:created_at (last trimmed1)) :id (:id (last trimmed1))}
        page2 (db/top-level-workflows (test-util/ds) {:limit 4 :before cursor1})
        trimmed2 (vec (take 4 page2))
        cursor2 {:created-at (:created_at (last trimmed2)) :id (:id (last trimmed2))}
        page3 (db/top-level-workflows (test-util/ds) {:limit 4 :before cursor2})]
    (is (= 5 (count page1)) "limit+1, so the caller can detect there's a next page")
    (is (= 5 (count page2)) "still more after page 2")
    (is (<= (count page3) 4) "nothing left beyond page 3")
    (let [all-ids (concat (map :id trimmed1) (map :id trimmed2) (map :id page3))]
      (is (= 10 (count all-ids)) "all 10 rows covered across the 3 pages")
      (is (= 10 (count (distinct all-ids))) "no row repeated across page boundaries"))))

(deftest pending-workflows-test
  (db/insert-workflow! (test-util/ds) "id-5" "test.ns/pending" (pr-str []) nil nil)
  (db/insert-event! (test-util/ds) "id-5" "STARTED" nil)
  (db/insert-workflow! (test-util/ds) "id-6" "test.ns/done" (pr-str []) nil nil)
  (db/insert-event! (test-util/ds) "id-6" "STARTED" nil)
  (db/insert-event! (test-util/ds) "id-6" "DONE" (pr-str :ok))
  (let [pending (db/pending-workflows (test-util/ds))]
    (is (some #(= "id-5" (:id %)) pending))
    (is (not (some #(= "id-6" (:id %)) pending)))))

(deftest current-time-test
  (db/insert-workflow! (test-util/ds) "id-7" "test.ns/clock" (pr-str []) nil nil)
  (db/insert-event! (test-util/ds) "id-7" "STARTED" nil)
  (let [now (db/current-time (test-util/ds))
        changed-at (:changed_at (db/latest-event (test-util/ds) "id-7"))]
    (is (instance? java.time.LocalDateTime now))
    (is (instance? java.time.LocalDateTime changed-at))
    (is (not (.isAfter ^java.time.LocalDateTime changed-at now))
        "same clock as changed_at, so an event is never later than current-time")))
