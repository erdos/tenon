(ns tenon.workflow.reader-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.edn :as edn]
            [matcher-combinators.test :refer [match? thrown-match?]]
            [tenon.workflow.fixtures :as fixtures]
            [tenon.workflow :as engine]
            [tenon.workflow.test-util :as test-util :refer [temp-db-fixture]]))

(use-fixtures :each temp-db-fixture)

(deftest tagged-defn-registers-and-audits-test
  (is (some? (engine/lookup 'tenon.workflow.fixtures/add-numbers)))
  (is (= 7 (fixtures/add-numbers 3 4)))
  (let [wf (first (test-util/find-by-wf-def "tenon.workflow.fixtures/add-numbers"))
        events (test-util/timeline (:workflow/invocation-id wf))]
    (is (= [3 4] (edn/read-string (:workflow/params wf))))
    (is (match? [{:workflow/state "STARTED"} {:workflow/state "DONE" :event/data (pr-str 7)}] events))))

(deftest tagged-defn-error-path-test
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"kaboom" (fixtures/boom "kaboom")))
  (let [wf (first (test-util/find-by-wf-def "tenon.workflow.fixtures/boom"))
        events (test-util/timeline (:workflow/invocation-id wf))]
    (is (match? [{:workflow/state "STARTED"} {:workflow/state "ERROR"}] events))
    (is (match? {:cause "kaboom"} (edn/read-string (:event/data (last events)))))))

(deftest tagged-defn-with-docstring-test
  (is (match? {:doc "Multiplies two numbers."
               :arglists '([a b])
               :custom-flag true
               :custom-key :custom-val}
              (meta #'fixtures/with-docstring)))
  (is (= 12 (fixtures/with-docstring 3 4)))
  (let [wf (first (test-util/find-by-wf-def "tenon.workflow.fixtures/with-docstring"))
        events (test-util/timeline (:workflow/invocation-id wf))]
    (is (= [3 4] (edn/read-string (:workflow/params wf))))
    (is (match? [{:workflow/state "STARTED"} {:workflow/state "DONE" :event/data (pr-str 12)}] events))))

(deftest tagged-defn-restart-via-reader-path-test
  ;; Exercises the real user-facing path end to end: #workflow tag ->
  ;; engine/register! -> wf_def stored as a string -> restart-invocation
  ;; parses that string back into a symbol -> engine/lookup -> re-run.
  (reset! fixtures/retryable-should-fail? true)
  (is (thrown? clojure.lang.ExceptionInfo (fixtures/retryable 1)))
  (let [wf (first (test-util/find-by-wf-def "tenon.workflow.fixtures/retryable"))
        id (:workflow/invocation-id wf)]
    (is (= "ERROR" (:workflow/state (engine/get-invocation id))))
    (reset! fixtures/retryable-should-fail? false)
    (let [result (engine/restart-invocation id)]
      (is (= :recovered result))
      (is (match? [{:workflow/state "STARTED"} {:workflow/state "ERROR"} {:workflow/state "STARTED"} {:workflow/state "DONE"}]
                  (test-util/timeline id))))))

(deftest tagged-defn-nested-invocation-records-parent-test
  ;; nested-parent calls nested-child from within its own body (both real
  ;; #workflow defns) - nested-child's row must record nested-parent's id
  ;; as parent_invocation_id, via *invocation-stack*, with neither fn passing
  ;; anything explicitly.
  (is (= 11 (fixtures/nested-parent 5)))
  (let [parent-wf (first (test-util/find-by-wf-def "tenon.workflow.fixtures/nested-parent"))
        child-wf (first (test-util/find-by-wf-def "tenon.workflow.fixtures/nested-child"))]
    (is (match? {:workflow/parent-invocation-id nil?} parent-wf))
    (is (match? {:workflow/parent-invocation-id (:workflow/invocation-id parent-wf)} child-wf))))

(deftest tagged-defn-fibonacci-test
  (is (= 55 (fixtures/fibonacci 10)))
  ;; find-by-wf-def-and-params, not find-by-wf-def - fibonacci's own
  ;; recursive calls are audited too now (see the dedup test below), so
  ;; several rows share this wf_def; only this exact-args lookup is
  ;; guaranteed to be the outermost n=10 call.
  (let [wf (test-util/find-by-wf-def-and-params "tenon.workflow.fixtures/fibonacci" (pr-str [10]))
        events (test-util/timeline (:workflow/invocation-id wf))]
    (is (= [10] (edn/read-string (:workflow/params wf))))
    (is (match? [{:workflow/state "STARTED"} {:workflow/state "DONE" :event/data (pr-str 55)}] events))))

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
        arg-of (fn [row] (first (edn/read-string (:workflow/params row))))]
    (is (= 11 (count rows)))
    (is (= (set (range 11)) (set (map arg-of rows))))
    (doseq [row rows]
      (is (match? [{:workflow/state "STARTED"} {:workflow/state "DONE"}]
                  (remove #(= "REUSED" (:workflow/state %)) (test-util/timeline (:workflow/invocation-id row))))
          (str "row for n=" (arg-of row) " ran to completion exactly once")))))

(deftest tagged-defn-repeated-sub-workflow-full-timeline-test
  ;; The second (say-hi "Bob") reuses the first one's result, so the
  ;; timeline holds say-hi's single run plus a REUSED row, both nested
  ;; under say-hi-twice, between its STARTED and DONE.
  (is (= "Hello Bob" (fixtures/say-hi-twice "Bob")))
  (let [parent (:workflow/invocation-id (first (test-util/find-by-wf-def "tenon.workflow.fixtures/say-hi-twice")))
        child (:workflow/invocation-id (first (test-util/find-by-wf-def "tenon.workflow.fixtures/say-hi")))]
    (is (match? [{:workflow/depth 0 :workflow/invocation-id parent :workflow/parent-invocation-id nil? :workflow/name "tenon.workflow.fixtures/say-hi-twice" :workflow/state "STARTED" :event/data "{}"}
                 {:workflow/depth 1 :workflow/invocation-id child :workflow/parent-invocation-id parent :workflow/name "tenon.workflow.fixtures/say-hi" :workflow/state "STARTED" :event/data "{}"}
                 {:workflow/depth 1 :workflow/invocation-id child :workflow/parent-invocation-id parent :workflow/name "tenon.workflow.fixtures/say-hi" :workflow/state "DONE" :event/data (pr-str "Hello Bob")}
                 {:workflow/depth 1 :workflow/invocation-id child :workflow/parent-invocation-id parent :workflow/name "tenon.workflow.fixtures/say-hi" :workflow/state "REUSED" :event/data "{}"}
                 {:workflow/depth 0 :workflow/invocation-id parent :workflow/parent-invocation-id nil? :workflow/name "tenon.workflow.fixtures/say-hi-twice" :workflow/state "DONE" :event/data (pr-str "Hello Bob")}]
                (engine/full-timeline parent)))))

(deftest tagged-defn-fibonacci-full-timeline-test
  ;; fib(4) runs fib(3), which runs fib(2), which runs fib(1) and fib(0).
  ;; fib(3)'s own (fibonacci 1) and fib(4)'s (fibonacci 2) then reuse the
  ;; stored results, so each shows up as a REUSED row under its caller.
  (is (= 3 (fixtures/fibonacci 4)))
  (let [id (fn [n] (:workflow/invocation-id (test-util/find-by-wf-def-and-params
                                    "tenon.workflow.fixtures/fibonacci" (pr-str [n]))))
        [f0 f1 f2 f3 f4] (map id (range 5))
        fib "tenon.workflow.fixtures/fibonacci"]
    (is (match? [{:workflow/depth 0 :workflow/invocation-id f4 :workflow/parent-invocation-id nil? :workflow/name fib :workflow/state "STARTED" :event/data "{}"}
                 {:workflow/depth 1 :workflow/invocation-id f3 :workflow/parent-invocation-id f4   :workflow/name fib :workflow/state "STARTED" :event/data "{}"}
                 {:workflow/depth 2 :workflow/invocation-id f2 :workflow/parent-invocation-id f3   :workflow/name fib :workflow/state "STARTED" :event/data "{}"}
                 {:workflow/depth 3 :workflow/invocation-id f1 :workflow/parent-invocation-id f2   :workflow/name fib :workflow/state "STARTED" :event/data "{}"}
                 {:workflow/depth 3 :workflow/invocation-id f1 :workflow/parent-invocation-id f2   :workflow/name fib :workflow/state "DONE"    :event/data (pr-str 1)}
                 {:workflow/depth 3 :workflow/invocation-id f0 :workflow/parent-invocation-id f2   :workflow/name fib :workflow/state "STARTED" :event/data "{}"}
                 {:workflow/depth 3 :workflow/invocation-id f0 :workflow/parent-invocation-id f2   :workflow/name fib :workflow/state "DONE"    :event/data (pr-str 0)}
                 {:workflow/depth 2 :workflow/invocation-id f2 :workflow/parent-invocation-id f3   :workflow/name fib :workflow/state "DONE"    :event/data (pr-str 1)}
                 {:workflow/depth 2 :workflow/invocation-id f1 :workflow/parent-invocation-id f3   :workflow/name fib :workflow/state "REUSED"  :event/data "{}"}
                 {:workflow/depth 1 :workflow/invocation-id f3 :workflow/parent-invocation-id f4   :workflow/name fib :workflow/state "DONE"    :event/data (pr-str 2)}
                 {:workflow/depth 1 :workflow/invocation-id f2 :workflow/parent-invocation-id f4   :workflow/name fib :workflow/state "REUSED"  :event/data "{}"}
                 {:workflow/depth 0 :workflow/invocation-id f4 :workflow/parent-invocation-id nil? :workflow/name fib :workflow/state "DONE"    :event/data (pr-str 3)}]
                (engine/full-timeline f4)))))

(deftest tagged-defn-fibonacci-reuses-earlier-call-full-timeline-test
  ;; (fibonacci 3) runs first, as a workflow of its own. (fibonacci 4) then
  ;; reuses its stored fib(3) and fib(2) results instead of running them
  ;; again - so fib(4)'s timeline is just its own states plus a REUSED row
  ;; for each. fib(3) and fib(2) ran under the earlier call, so their own
  ;; states stay in its timeline, not fib(4)'s.
  (is (= 2 (fixtures/fibonacci 3)))
  (is (= 3 (fixtures/fibonacci 4)))
  (let [id (fn [n] (:workflow/invocation-id (test-util/find-by-wf-def-and-params
                                    "tenon.workflow.fixtures/fibonacci" (pr-str [n]))))
        [f2 f3 f4] (map id [2 3 4])
        fib "tenon.workflow.fixtures/fibonacci"]
    (is (match? [{:workflow/depth 0 :workflow/invocation-id f4 :workflow/parent-invocation-id nil? :workflow/name fib :workflow/state "STARTED" :event/data "{}"}
                 {:workflow/depth 1 :workflow/invocation-id f3 :workflow/parent-invocation-id f4   :workflow/name fib :workflow/state "REUSED"  :event/data "{}"}
                 {:workflow/depth 1 :workflow/invocation-id f2 :workflow/parent-invocation-id f4   :workflow/name fib :workflow/state "REUSED"  :event/data "{}"}
                 {:workflow/depth 0 :workflow/invocation-id f4 :workflow/parent-invocation-id nil? :workflow/name fib :workflow/state "DONE"    :event/data (pr-str 3)}]
                (engine/full-timeline f4)))))

(deftest tagged-defn-repeated-call-is-listed-twice-test
  ;; The second call reuses the first one's result - no new workflow row,
  ;; but it's a call of its own, so list-invocations returns both.
  (is (= 7 (fixtures/add-numbers 3 4)))
  (is (= 7 (fixtures/add-numbers 3 4)))
  (let [id (:workflow/invocation-id (first (test-util/find-by-wf-def "tenon.workflow.fixtures/add-numbers")))]
    (is (match? [{:workflow/invocation-id id :workflow/state "DONE" :event/reused true}
                 {:workflow/invocation-id id :workflow/state "DONE" :event/reused false}]
                (engine/list-invocations {:wf-def "tenon.workflow.fixtures/add-numbers"})))))

(deftest tagged-defn-unbound-workflow-engine-throws-test
  (binding [engine/*workflow-engine* nil]
    (is (thrown-match? clojure.lang.ExceptionInfo
                       {:type ::engine/precondition-failed}
                       (fixtures/add-numbers 1 2)))))

(defn- task-wf-def
  "The wf_def of the one #task in the fixtures namespace."
  []
  (str "tenon.workflow.fixtures/"
       (first (filter #(re-find #"^task-" %) (map name (keys (ns-interns 'tenon.workflow.fixtures)))))))

(deftest task-captures-used-locals-and-audits-test
  (is (= 12 (fixtures/multiply-in-task 3 4 :unused)))
  (let [wf (first (test-util/find-by-wf-def (task-wf-def)))]
    ;; only the locals the expression uses are captured, in order of appearance
    (is (= [3 4] (edn/read-string (:workflow/params wf))))
    (is (match? [{:workflow/state "STARTED"} {:workflow/state "DONE" :event/data (pr-str 12)}]
                (test-util/timeline (:workflow/invocation-id wf))))))
