(ns tenon.workflow.sqlite.jdbc
  "Reading rows straight off a JDBC ResultSet, by column index, into maps.

   Text is fetched as its UTF-8 bytes and decoded here - sqlite-jdbc's
   getString is much slower - and a row's map is built in one go as an
   array map, not assoc'ed together."
  (:require [clojure.walk :as walk])
  (:import [clojure.lang PersistentArrayMap RT]
           [java.nio.charset StandardCharsets]
           [java.sql ResultSet]))

(defn int-at ^Object [^ResultSet r ^long i]
  (.getLong r i))

(defn int-or-nil-at ^Object [^ResultSet r ^long i]
  (let [v (.getLong r i)]
    (when-not (.wasNull r) v)))

(defn text-at ^Object [^ResultSet r ^long i]
  (when-let [b (.getBytes r i)]
    (String. ^bytes b StandardCharsets/UTF_8)))

(defn- column? [x]
  (and (symbol? x) (= "%" (name x))))

;; (row-fn k1 v1 k2 v2 ...): a fn of a ResultSet, returning its row as an
;; array map. In each value with a % in it, % stands for the result set and
;; the index of the next column - 1 for the first such value, 2 for the
;; second, and so on: (row-fn :a (int-at %) :b true :c (text-at %)) reads
;; :a with (int-at r 1) and :c with (text-at r 2).
(defmacro row-fn [& kvs]
  (let [r (with-meta (gensym "r") {:tag 'java.sql.ResultSet})
        a (gensym "a")
        col (volatile! 0)
        value (fn [form]
                (if (some column? (tree-seq coll? seq form))
                  (let [i (vswap! col inc)]
                    (walk/postwalk #(if (seq? %) (apply list (mapcat (fn [x] (if (column? x) [r i] [x])) %)) %)
                                   form))
                  form))]
    `(fn [~r]
       (let [~a (object-array ~(count kvs))]
         ~@(map-indexed (fn [i x] (if (odd? i)
                                    `(aset ~a ~i (RT/box ~(value x))) ; boxes primitives without reflection
                                    `(aset ~a ~i ~x)))
                        kvs)
         (PersistentArrayMap. ~a)))))
