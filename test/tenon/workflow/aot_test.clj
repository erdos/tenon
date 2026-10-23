(ns tenon.workflow.aot-test
  (:require [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.test :refer [deftest is]]
            [clojure.string :as str]))

(def ^:private app-ns
  "(ns aotapp.core (:require tenon.workflow))
(defn never-called [a b c] #task (* a b))")

(defn- java [classpath & clojure-main-args]
  (apply sh "java" "-cp" classpath "clojure.main" clojure-main-args))

(deftest task-of-aot-compiled-ns-is-restartable-in-fresh-jvm-test
  ;; JVM 1 compiles aotapp.core ahead of time and runs its task, so that
  ;; the database holds an invocation. JVM 2 has only the compiled classes
  ;; (no sources of aotapp.core) and never calls never-called, but must
  ;; still find the task registered when restarting the invocation.
  (let [dir (.toFile (java.nio.file.Files/createTempDirectory "tenon-aot" (make-array java.nio.file.attribute.FileAttribute 0)))
        src (io/file dir "src") classes (io/file dir "classes") db (str (io/file dir "tenon.db"))
        cp (System/getProperty "java.class.path")]
    (try
      (io/make-parents (io/file src "aotapp" "core.clj"))
      (.mkdirs classes)
      (spit (io/file src "aotapp" "core.clj") app-ns)
      (let [first-run (java (str/join ":" [src classes cp])
                            "-e" (str "(require 'tenon.workflow)"
                                      "(binding [*compile-path* " (pr-str (str classes)) "] (compile 'aotapp.core))"
                                      "(def e (tenon.workflow/init " (pr-str db) " :tenon/sweep-ms nil))"
                                      "(alter-var-root #'tenon.workflow/*workflow-engine* (constantly e))"
                                      "(println :result (aotapp.core/never-called 3 4 5))"
                                      "(tenon.workflow/close! e) (shutdown-agents)"))]
        (is (= 0 (:exit first-run)) (:err first-run))
        (is (str/includes? (:out first-run) ":result 12")))
      (let [second-run (java (str/join ":" [classes cp])
                             "-e" (str "(require 'tenon.workflow 'aotapp.core)"
                                       "(def e (tenon.workflow/init " (pr-str db) " :tenon/sweep-ms nil))"
                                       "(alter-var-root #'tenon.workflow/*workflow-engine* (constantly e))"
                                       "(let [id (:workflow/invocation-id (first (tenon.workflow/list-invocations {})))]"
                                       "  (println :restarted (tenon.workflow/restart-invocation id)))"
                                       "(tenon.workflow/close! e) (shutdown-agents)"))]
        (is (= 0 (:exit second-run)) (:err second-run))
        (is (str/includes? (:out second-run) ":restarted 12")))
      (finally
        (doseq [f (reverse (file-seq dir))] (.delete f))))))
