(ns tenon.workflow.defworkflow-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.edn :as edn]
            [matcher-combinators.test :refer [match? thrown-match?]]
            [tenon.workflow.defworkflow-fixtures :as fixtures]
            [tenon.workflow :as engine]
            [tenon.workflow.test-util :as test-util :refer [temp-db-fixture]]))

(use-fixtures :each temp-db-fixture)

(def ^:private prefix "tenon.workflow.defworkflow-fixtures/")

(deftest defworkflow-registers-and-audits-test
  (is (some? (engine/lookup 'tenon.workflow.defworkflow-fixtures/add-numbers)))
  (is (= 7 (fixtures/add-numbers 3 4)))
  (let [wf (first (test-util/find-by-wf-def (str prefix "add-numbers")))
        events (test-util/timeline (:invocation_id wf))]
    (is (= [3 4] (edn/read-string (:params wf))))
    (is (match? [{:state "STARTED"} {:state "DONE" :data (pr-str 7)}] events))))

(deftest defworkflow-error-path-test
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"kaboom" (fixtures/boom "kaboom")))
  (let [wf (first (test-util/find-by-wf-def (str prefix "boom")))
        events (test-util/timeline (:invocation_id wf))]
    (is (match? [{:state "STARTED"} {:state "ERROR"}] events))
    (is (match? {:cause "kaboom"} (edn/read-string (:data (last events)))))))

(deftest defworkflow-keeps-docstring-and-metadata-test
  (is (match? {:doc "Multiplies two numbers."
               :arglists '([a b])
               :custom-flag true
               :custom-key :custom-val}
              (meta #'fixtures/with-docstring)))
  (is (= 12 (fixtures/with-docstring 3 4)))
  (let [wf (first (test-util/find-by-wf-def (str prefix "with-docstring")))
        events (test-util/timeline (:invocation_id wf))]
    (is (= [3 4] (edn/read-string (:params wf))))
    (is (match? [{:state "STARTED"} {:state "DONE" :data (pr-str 12)}] events))))

(deftest defworkflow-multi-arity-test
  ;; The 1-arg call delegates to the 2-arg one, so both calls are audited.
  (is (= 6 (fixtures/multi-arity 5)))
  (is (= 9 (fixtures/multi-arity 4 5)))
  (let [wf-def (str prefix "multi-arity")
        done-result (fn [params]
                      (let [wf (test-util/find-by-wf-def-and-params wf-def (pr-str params))]
                        (:data (last (test-util/timeline (:invocation_id wf))))))]
    (is (= (pr-str 6) (done-result [5])))
    (is (= (pr-str 6) (done-result [5 1])))
    (is (= (pr-str 9) (done-result [4 5])))))

(deftest defworkflow-restart-via-registry-path-test
  ;; wf_def is stored as a string, parsed back into a symbol, looked up in
  ;; the registry and re-run - the same path the #workflow fns take.
  (reset! fixtures/retryable-should-fail? true)
  (is (thrown? clojure.lang.ExceptionInfo (fixtures/retryable 1)))
  (let [wf (first (test-util/find-by-wf-def (str prefix "retryable")))
        id (:invocation_id wf)]
    (is (= "ERROR" (:state (engine/get-invocation id))))
    (reset! fixtures/retryable-should-fail? false)
    (let [result (engine/restart-invocation id)]
      (is (= :recovered result))
      (is (match? [{:state "STARTED"} {:state "ERROR"} {:state "STARTED"} {:state "DONE"}]
                  (test-util/timeline id))))))

(deftest defworkflow-nested-invocation-records-parent-test
  (is (= 11 (fixtures/nested-parent 5)))
  (let [parent-wf (first (test-util/find-by-wf-def (str prefix "nested-parent")))
        child-wf (first (test-util/find-by-wf-def (str prefix "nested-child")))]
    (is (match? {:parent_invocation_id nil?} parent-wf))
    (is (match? {:parent_invocation_id (:invocation_id parent-wf)} child-wf))))

(deftest defworkflow-recursive-calls-are-audited-and-deduped-test
  ;; Naive fib(10) touches every k from 0 to 10, and run-invocation dedups by
  ;; (wf_def, params), so exactly 11 rows should exist.
  (is (= 55 (fixtures/fibonacci 10)))
  (let [rows (test-util/find-by-wf-def (str prefix "fibonacci"))
        arg-of (fn [row] (first (edn/read-string (:params row))))]
    (is (= 11 (count rows)))
    (is (= (set (range 11)) (set (map arg-of rows))))
    (doseq [row rows]
      (is (match? [{:state "STARTED"} {:state "DONE"}]
                  (remove #(= "REUSED" (:state %)) (test-util/timeline (:invocation_id row))))
          (str "row for n=" (arg-of row) " ran to completion exactly once")))))

(deftest defworkflow-repeated-sub-workflow-reuses-result-test
  (is (= "Hello Bob" (fixtures/say-hi-twice "Bob")))
  (let [parent (:invocation_id (first (test-util/find-by-wf-def (str prefix "say-hi-twice"))))
        child (:invocation_id (first (test-util/find-by-wf-def (str prefix "say-hi"))))]
    (is (match? [{:depth 0 :invocation_id parent :parent_invocation_id nil? :state "STARTED"}
                 {:depth 1 :invocation_id child :parent_invocation_id parent :state "STARTED"}
                 {:depth 1 :invocation_id child :parent_invocation_id parent :state "DONE" :data (pr-str "Hello Bob")}
                 {:depth 1 :invocation_id child :parent_invocation_id parent :state "REUSED"}
                 {:depth 0 :invocation_id parent :parent_invocation_id nil? :state "DONE" :data (pr-str "Hello Bob")}]
                (engine/full-timeline parent)))))

(deftest defworkflow-unbound-workflow-engine-throws-test
  (binding [engine/*workflow-engine* nil]
    (is (thrown-match? clojure.lang.ExceptionInfo
                       {:type ::engine/precondition-failed}
                       (fixtures/add-numbers 1 2)))))
