(ns tenon.workflow.common
  "Helpers shared by tenon.workflow and tenon.workflow.timeout."
  (:import [java.io StringWriter Writer]))

(defn emit!
  "Reports event with data to engine's :tenon/on-event."
  [engine event data]
  ((:tenon/on-event engine) event data))

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

(defn edn-str
  "Like pr-str, but canonical: maps and sets print in a deterministic
   order, and the caller's printing vars (*print-length*, *print-level*,
   *print-meta*, *print-namespace-maps*, ...) are ignored."
  ^String [x]
  (binding [*print-length* nil *print-level* nil *print-meta* false *print-dup* false
            *print-readably* true *print-namespace-maps* false]
    (let [w (StringWriter.)]
      (write-edn x w)
      (str w))))

(defn exception-str
  "Throwable e saved as EDN: its Throwable->map."
  [e]
  (edn-str (Throwable->map e)))
