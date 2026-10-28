(ns tenon.workflow.common
  "Helpers shared by tenon.workflow and tenon.workflow.timeout."
  (:require [clojure.walk :as walk]))

(defn emit!
  "Reports event with data to engine's :tenon/on-event."
  [engine event data]
  ((:tenon/on-event engine) event data))

(defn- canonical-compare [a b]
  (if (= a b) 0 (compare (pr-str a) (pr-str b))))

(defn edn-str
  "Like pr-str, but ignores any caller-bound *print-length*/*print-level*
   and canonicalizes maps and sets into a deterministic order."
  [x]
  (binding [*print-length* nil *print-level* nil]
    (pr-str (walk/postwalk
             (fn [v]
               (cond
                 (and (map? v) (not (record? v))) (into (sorted-map-by canonical-compare) v)
                 (set? v) (into (sorted-set-by canonical-compare) v)
                 :else v))
             x))))

(defn exception-str
  "Throwable e saved as EDN: its Throwable->map."
  [e]
  (edn-str (Throwable->map e)))
