(ns tenon.workflow.reader-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.edn :as edn]
            [tenon.workflow.fixtures :as fixtures]
            [tenon.workflow :as engine]
            [tenon.workflow.test-util :as test-util :refer [temp-db-fixture]]))

(use-fixtures :each temp-db-fixture)

(deftest tagged-defn-registers-and-audits-test
  (is (some? (engine/lookup 'tenon.workflow.fixtures/add-numbers)))
  (is (= 7 (fixtures/add-numbers 3 4)))
  (let [wf (first (test-util/find-by-wf-def "tenon.workflow.fixtures/add-numbers"))
        events (test-util/timeline (:invocation_id wf))]
    (is (= [3 4] (edn/read-string (:params wf))))
    (is (= ["STARTED" "DONE"] (mapv :state events)))
    (is (= 7 (edn/read-string (:data (last events)))))))

(deftest tagged-defn-error-path-test
  (is (thrown? clojure.lang.ExceptionInfo (fixtures/boom "kaboom")))
  (let [wf (first (test-util/find-by-wf-def "tenon.workflow.fixtures/boom"))
        events (test-util/timeline (:invocation_id wf))]
    (is (= ["STARTED" "ERROR"] (mapv :state events)))
    (is (= "kaboom" (:message (edn/read-string (:data (last events))))))))

(deftest tagged-defn-with-docstring-test
  (is (= 12 (fixtures/with-docstring 3 4)))
  (let [wf (first (test-util/find-by-wf-def "tenon.workflow.fixtures/with-docstring"))
        events (test-util/timeline (:invocation_id wf))]
    (is (= [3 4] (edn/read-string (:params wf))))
    (is (= ["STARTED" "DONE"] (mapv :state events)))
    (is (= 12 (edn/read-string (:data (last events)))))))

(deftest tagged-defn-restart-via-reader-path-test
  ;; Exercises the real user-facing path end to end: #workflow tag ->
  ;; engine/register! -> wf_def stored as a string -> restart-invocation
  ;; parses that string back into a symbol -> engine/lookup -> re-run.
  (reset! fixtures/retryable-should-fail? true)
  (is (thrown? clojure.lang.ExceptionInfo (fixtures/retryable 1)))
  (let [wf (first (test-util/find-by-wf-def "tenon.workflow.fixtures/retryable"))
        id (:invocation_id wf)]
    (is (= "ERROR" (:state (engine/get-invocation id))))
    (reset! fixtures/retryable-should-fail? false)
    (let [result (engine/restart-invocation id)]
      (is (= :recovered result))
      (is (= ["STARTED" "ERROR" "STARTED" "DONE"] (mapv :state (test-util/timeline id)))))))

(deftest tagged-defn-nested-invocation-records-parent-test
  ;; nested-parent calls nested-child from within its own body (both real
  ;; #workflow defns) - nested-child's row must record nested-parent's id
  ;; as parent_invocation_id, via *invocation-stack*, with neither fn passing
  ;; anything explicitly.
  (is (= 11 (fixtures/nested-parent 5)))
  (let [parent-wf (first (test-util/find-by-wf-def "tenon.workflow.fixtures/nested-parent"))
        child-wf (first (test-util/find-by-wf-def "tenon.workflow.fixtures/nested-child"))]
    (is (nil? (:parent_invocation_id parent-wf)))
    (is (= (:invocation_id parent-wf) (:parent_invocation_id child-wf)))))

(deftest tagged-defn-fibonacci-test
  (is (= 55 (fixtures/fibonacci 10)))
  ;; find-by-wf-def-and-params, not find-by-wf-def - fibonacci's own
  ;; recursive calls are audited too now (see the dedup test below), so
  ;; several rows share this wf_def; only this exact-args lookup is
  ;; guaranteed to be the outermost n=10 call.
  (let [wf (test-util/find-by-wf-def-and-params "tenon.workflow.fixtures/fibonacci" (pr-str [10]))
        events (test-util/timeline (:invocation_id wf))]
    (is (= [10] (edn/read-string (:params wf))))
    (is (= ["STARTED" "DONE"] (mapv :state events)))
    (is (= 55 (edn/read-string (:data (last events)))))))

(deftest tagged-defn-fibonacci-recursive-calls-are-audited-and-deduped-test
  ;; fibonacci's own recursive calls now go through run-invocation too (not
  ;; just the outermost call) - and since run-invocation dedups by
  ;; (wf_def, params), every repeated (fibonacci k) across the naive,
  ;; unmemoized call tree collapses onto one row per distinct k. Naive
  ;; fib(10)'s call tree touches every k from 0 to 10, so exactly 11 rows
  ;; should exist - proof the recursion is both audited and memoized for
  ;; free by the existing dedup machinery.
  (is (= 55 (fixtures/fibonacci 10)))
  (let [rows (test-util/find-by-wf-def "tenon.workflow.fixtures/fibonacci")
        arg-of (fn [row] (first (edn/read-string (:params row))))]
    (is (= 11 (count rows)))
    (is (= (set (range 11)) (set (map arg-of rows))))
    (doseq [row rows]
      (is (= ["STARTED" "DONE"] (->> (test-util/timeline (:invocation_id row))
                                     (map :state)
                                     (remove #{"REUSED"})))
          (str "row for n=" (arg-of row) " ran to completion exactly once")))))

(deftest tagged-defn-repeated-sub-workflow-full-timeline-test
  ;; The second (say-hi "Bob") reuses the first one's result, so the
  ;; timeline holds say-hi's single run plus a REUSED row, both nested
  ;; under say-hi-twice, between its STARTED and DONE.
  (is (= "Hello Bob" (fixtures/say-hi-twice "Bob")))
  (let [parent (:invocation_id (first (test-util/find-by-wf-def "tenon.workflow.fixtures/say-hi-twice")))
        child (:invocation_id (first (test-util/find-by-wf-def "tenon.workflow.fixtures/say-hi")))]
    (is (= [[0 parent nil "tenon.workflow.fixtures/say-hi-twice" "STARTED" nil]
            [1 child parent "tenon.workflow.fixtures/say-hi" "STARTED" nil]
            [1 child parent "tenon.workflow.fixtures/say-hi" "DONE" (pr-str "Hello Bob")]
            [1 child parent "tenon.workflow.fixtures/say-hi" "REUSED" nil]
            [0 parent nil "tenon.workflow.fixtures/say-hi-twice" "DONE" (pr-str "Hello Bob")]]
           (mapv (juxt :depth :invocation_id :parent_invocation_id :wf_def :state :data)
                 (engine/full-timeline parent))))))
