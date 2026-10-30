(ns tenon.workflow.bench
  "Benchmarks of what tenon persists per call: the codec alone - writing
   params, results and failures, reading them back - and whole calls,
   storage included. Run with ./run bench."
  (:require [criterium.core :as crit]
            [tenon.workflow :as engine]
            [tenon.workflow.serializer :as serializer])
  (:import [java.io File]))

(def ^:private small-params [42 "order-17" :express])

(def ^:private large-params
  [{:customer {:id 1234 :name "Ada" :tags #{:vip :beta :eu}}
    :items (vec (for [i (range 50)]
                  {:sku (str "sku-" i) :qty i :price (* 1.5 i) :attrs {:color :red :size i}}))}])

(def ^:private failure
  (ex-info "boom" {:reason :out-of-stock :sku "sku-7"} (IllegalStateException. "root cause")))

(def ^:private codec
  "[write read write-failure] of the codec under test."
  [#(serializer/serialize serializer/edn-serializer %)
   #(serializer/deserialize serializer/edn-serializer %)
   #(serializer/serialize serializer/edn-serializer (Throwable->map %))])

(defn- bench [label f]
  (let [{[mean] :mean} (crit/quick-benchmark* f {})]
    (println (format "%-28s %12.2f us" label (* 1e6 mean)))))

(defn- codec-benches []
  (let [[write read write-failure] codec
        small (write small-params)
        large (write large-params)
        failed (write-failure failure)]
    (bench "write small params" #(write small-params))
    (bench "write large params" #(write large-params))
    (bench "write failure" #(write-failure failure))
    (bench "read small params" #(read small))
    (bench "read large params" #(read large))
    (bench "read failure" #(read failed))))

(def ^:private counter (atom 0))

(defn- raw-echo [_n x] x)
(defn- raw-fail [_n] (throw failure))

(defn- call-benches []
  (let [file (File/createTempFile "tenon-bench" ".db")
        engine (engine/init (str file) :tenon/timeout-ms nil)
        echo (doto (intern *ns* 'echo) (engine/register! raw-echo))
        fail (doto (intern *ns* 'fail) (engine/register! raw-fail))]
    (try
      (engine/run-invocation engine echo raw-echo [0 large-params])
      (try (engine/run-invocation engine fail raw-fail [0]) (catch Exception _))
      (bench "call new, small" #(engine/run-invocation engine echo raw-echo [(swap! counter inc) small-params]))
      (bench "call new, large" #(engine/run-invocation engine echo raw-echo [(swap! counter inc) large-params]))
      (bench "call reused, large" #(engine/run-invocation engine echo raw-echo [0 large-params]))
      (bench "call reused failure" #(try (engine/run-invocation engine fail raw-fail [0]) (catch Exception _)))
      (finally
        (engine/close! engine)
        (.delete file)))))

(defn -main [& _]
  (codec-benches)
  (call-benches)
  (shutdown-agents))
