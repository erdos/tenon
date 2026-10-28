(ns tenon.workflow.common-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.walk :as walk]
            [tenon.workflow.common :as common]))

(defrecord Point [x y])

(defn- reference-edn-str
  "edn-str as it was first written - sorting by rebuilding maps and sets as
   sorted ones, then pr-str - with namespace maps off. edn-str must print
   exactly the same: a stored params string is part of the workflow's
   identity."
  [x]
  (let [canonical-compare (fn [a b] (if (= a b) 0 (compare (pr-str a) (pr-str b))))]
    (binding [*print-length* nil *print-level* nil *print-namespace-maps* false]
      (pr-str (walk/postwalk
               (fn [v]
                 (cond
                   (and (map? v) (not (record? v))) (into (sorted-map-by canonical-compare) v)
                   (set? v) (into (sorted-set-by canonical-compare) v)
                   :else v))
               x)))))

(def ^:private samples
  [nil true 1 -2.5 1/3 1N 1.5M ##Inf ##NaN \a "s\"q\n" :k :ns/k 'sym 'ns/sym
   #uuid "00000000-0000-0000-0000-000000000001" #inst "2026-01-02T03:04:05.678Z"
   [] () #{} {} '(1 (2)) (range 3) (map inc [1 2])
   {:b 1 :a 2} {:ns/b 1 :ns/a 2} {"b" 1 'a 2 :c 3 1 4 nil 5}
   #{3 1 2} #{{:b 1 :a 2} {:a 1}} {{:b 1 :a 2} :map-key [2 1] :vector-key}
   (->Point {:b 1 :a 2} #{2 1}) (assoc (->Point 1 2) :extra {:z 1 :y 2})
   (first {:b {:d 1 :c 2}})
   (Throwable->map (ex-info "boom" {:b 1 :a [{:d 1 :c 2}]}))])

(defn- random-value [^java.util.Random rnd depth]
  (let [leaf #(case (.nextInt rnd 6)
                0 (.nextInt rnd 100)
                1 (keyword (str (char (+ 97 (.nextInt rnd 26)))))
                2 (str (char (+ 97 (.nextInt rnd 26))) "\"")
                3 (symbol "n" (str (char (+ 97 (.nextInt rnd 26)))))
                4 nil
                5 (.nextDouble rnd))
        items #(repeatedly (.nextInt rnd 5) (fn [] (random-value rnd (dec depth))))]
    (if (zero? depth)
      (leaf)
      (case (.nextInt rnd 6)
        0 (leaf)
        1 (vec (items))
        2 (apply list (items))
        3 (set (items))
        4 (zipmap (items) (items))
        5 (zipmap (map (fn [_] (leaf)) (items)) (items))))))

(deftest edn-str-prints-what-pr-str-of-the-sorted-value-did-test
  (doseq [x samples]
    (is (= (reference-edn-str x) (common/edn-str x)) (pr-str x)))
  (let [rnd (java.util.Random. 42)]
    (dotimes [_ 2000]
      (let [x (random-value rnd 4)]
        (is (= (reference-edn-str x) (common/edn-str x)) (pr-str x))))))

(deftest edn-str-ignores-the-callers-printing-vars-test
  (let [x {:ns/b [1 2 3] :ns/a (with-meta [1] {:m 1})}]
    (is (= "{:ns/a [1], :ns/b [1 2 3]}"
           (binding [*print-length* 1 *print-level* 1 *print-meta* true *print-namespace-maps* true]
             (common/edn-str x))))))
