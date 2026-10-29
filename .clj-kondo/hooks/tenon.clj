(ns hooks.tenon)

(defn- fill-columns
  "form with each % in a call replaced by a result set and a column index,
   as tenon.workflow.sqlite.jdbc/row-fn does."
  [form]
  (cond
    (seq? form) (apply list (mapcat (fn [x] (if (= '% x) '[r 1] [(fill-columns x)])) form))
    (vector? form) (mapv fill-columns form)
    :else form))

(defmacro row-fn [& kvs]
  (list 'fn ['r] (cons 'hash-map (map fill-columns kvs))))

(defmacro current-row-fn [& kvs]
  (cons 'tenon.workflow.sqlite.jdbc/row-fn kvs))
