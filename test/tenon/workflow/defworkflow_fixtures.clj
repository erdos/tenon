(ns tenon.workflow.defworkflow-fixtures
  (:require [tenon.workflow :refer [defworkflow]]))

;; Mirrors tenon.workflow.fixtures, but every fn is declared with defworkflow
;; rather than the #workflow tag, so the tests exercise the macro path.

(defworkflow add-numbers [a b]
  (+ a b))

(defworkflow boom [msg]
  (throw (ex-info msg {:reason :boom})))

;; Backs retryable below: toggled by tests to make the fn fail on the first
;; call and succeed on a restarted one.
(defonce retryable-should-fail? (atom true))

(defworkflow retryable [n]
  (if @retryable-should-fail?
    (throw (ex-info "not yet" {:n n}))
    :recovered))

(defworkflow ^:custom-flag with-docstring
  "Multiplies two numbers."
  {:custom-key :custom-val}
  [a b]
  (* a b))

(defworkflow nested-child [n]
  (* n 2))

(defworkflow nested-parent [n]
  (+ 1 (nested-child n)))

(defworkflow fibonacci [n]
  (if (< n 2)
    n
    (+ (fibonacci (dec n)) (fibonacci (- n 2)))))

(defworkflow say-hi [x]
  (str "Hello " x))

(defworkflow say-hi-twice [x]
  (say-hi x)
  (say-hi x))

(defworkflow multi-arity
  ([x] (multi-arity x 1))
  ([x y] (+ x y)))
