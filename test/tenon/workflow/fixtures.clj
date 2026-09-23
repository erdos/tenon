(ns tenon.workflow.fixtures
  ;; Required (not just used via #workflow below) because data_readers.clj
  ;; only interns an unbound var for the #workflow tag target - it does not
  ;; load tenon.workflow itself. Do not remove as "unused": without this
  ;; require the #workflow reads below fail with an unbound var error.
  (:require tenon.workflow))

#workflow
(defn add-numbers [a b]
  (+ a b))

#workflow
(defn boom [msg]
  (throw (ex-info msg {:reason :boom})))

;; Backs retryable below: toggled by tests to make the tagged fn fail on the
;; first call and succeed on a subsequent (restarted) call, exercising the
;; real #workflow -> registry -> engine/restart-invocation path end to end.
(defonce retryable-should-fail? (atom true))

#workflow
(defn retryable [n]
  (if @retryable-should-fail?
    (throw (ex-info "not yet" {:n n}))
    :recovered))

#workflow
(defn with-docstring
  "Multiplies two numbers. Exists to prove #workflow strips a leading
   docstring from the defn tail instead of splicing it into fn."
  [a b]
  (* a b))

#workflow
(defn nested-child [n]
  (* n 2))

;; Calls another #workflow fn from within its own body, so tests can prove
;; nested-child's workflow row records nested-parent's id as its
;; parent_workflow_id via the real #workflow -> engine/run-invocation path.
#workflow
(defn nested-parent [n]
  (+ 1 (nested-child n)))

#workflow
(defn fibonacci [n]
  (if (< n 2)
    n
    (+ (fibonacci (dec n)) (fibonacci (- n 2)))))
