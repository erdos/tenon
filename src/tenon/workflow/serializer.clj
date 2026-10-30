(ns tenon.workflow.serializer
  "How the values tenon persists - a workflow's params, its result, its
   failure (a Throwable->map) and its metadata - become the strings it
   stores, and back."
  (:require [clojure.edn :as edn])
  (:import [java.io StringWriter Writer]))

(defprotocol Serializer
  (serialize ^String [serializer x]
    "x as a string. Equal values must give equal strings: a workflow is
     identified by its serialized params.")
  (deserialize [serializer s]
    "The value the string s, as serialize returned it, stands for."))

(declare ^:private write-edn)

(defn- edn-key ^String [x]
  (if (keyword? x)
    (str x)
    (let [w (StringWriter.)] (write-edn x w) (str w))))

(defn- write-items [^Writer w ^String open ^String close write-item items]
  (.write w open)
  (loop [items (seq items) first? true]
    (when items
      (when-not first? (.write w " "))
      (write-item (first items))
      (recur (next items) false)))
  (.write w close))

(defn- write-entries [^Writer w entries]
  (.write w "{")
  (loop [entries (seq entries) first? true]
    (when entries
      (let [[^String k v] (first entries)]
        (when-not first? (.write w ", "))
        (.write w k)
        (.write w " ")
        (write-edn v w))
      (recur (next entries) false)))
  (.write w "}"))

(defn- write-edn
  "Prints x as pr would, but maps and sets in the order of their keys'
   (elements') printed forms - so equal values always print the same."
  [x ^Writer w]
  (cond
    (record? x) (do (.write w "#")
                    (.write w (.getName (class x)))
                    (write-entries w (map (fn [[k v]] [(edn-key k) v]) x)))
    (map? x) (write-entries w (sort-by first (map (fn [[k v]] [(edn-key k) v]) x)))
    (set? x) (write-items w "#{" "}" #(.write w ^String %) (sort (map edn-key x)))
    (vector? x) (write-items w "[" "]" #(write-edn % w) x)
    (seq? x) (write-items w "(" ")" #(write-edn % w) x)
    :else (print-method x w)))

(def edn-serializer
  "The default Serializer: EDN, read back by clojure.edn/read-string.
   Prints like pr-str, but canonically - maps and sets in a deterministic
   order - and ignoring the caller's printing vars (*print-length*,
   *print-level*, *print-meta*, *print-namespace-maps*, ...)."
  (reify Serializer
    (serialize [_ x]
      (binding [*print-length* nil *print-level* nil *print-meta* false *print-dup* false
                *print-readably* true *print-namespace-maps* false]
        (let [w (StringWriter.)]
          (write-edn x w)
          (str w))))
    (deserialize [_ s]
      (edn/read-string s))))
