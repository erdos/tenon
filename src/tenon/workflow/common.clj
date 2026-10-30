(ns tenon.workflow.common
  "Helpers shared by tenon.workflow and tenon.workflow.timeout."
  (:require [tenon.workflow.serializer :as serializer]))

(defn emit!
  "Reports event with data to engine's :tenon/on-event."
  [engine event data]
  ((:tenon/on-event engine) event data))

(defn serialize
  "x as stored by engine's :tenon/serializer."
  ^String [engine x]
  (serializer/serialize (:tenon/serializer engine) x))

(defn deserialize
  "The value s, as stored by engine's :tenon/serializer, stands for."
  [engine s]
  (serializer/deserialize (:tenon/serializer engine) s))

(defn serialize-failure
  "Throwable e as stored by engine's :tenon/serializer: its Throwable->map."
  ^String [engine e]
  (serialize engine (Throwable->map e)))
