(ns tenon.workflow.test-util
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [tenon.workflow :as engine]))

(def ^:dynamic *db-path*
  "Absolute path of the current test's temp SQLite file, bound by
   temp-db-fixture. Lets a test open its own raw next.jdbc connection
   against the exact same database tenon.workflow.db is using."
  nil)

(defn ds
  "The datasource of the current test's engine map - a shorthand for
   direct tenon.workflow.db calls in tests, e.g. (db/get-workflow (ds) id)."
  []
  (:tenon/db engine/*workflow-engine*))

(defn get-events
  "All workflow_events rows for workflow-id, oldest first. Test-only -
   nothing in the library reads the full event list at once, only
   latest-event (tenon.workflow.db, part of the Storage protocol)."
  [ds workflow-id]
  (jdbc/execute! ds ["SELECT * FROM workflow_events WHERE workflow_id = ? ORDER BY id ASC" workflow-id]
                 {:builder-fn rs/as-unqualified-lower-maps}))

(defn find-by-wf-def
  "Workflow rows whose wf_def is exactly wf-def. Test-only - nothing in
   the library looks workflows up by wf_def alone, only by the
   (wf_def, arguments) pair (tenon.workflow.db/find-by-wf-def-and-arguments,
   part of the Storage protocol)."
  [ds wf-def]
  (jdbc/execute! ds ["SELECT * FROM workflow WHERE wf_def = ?" wf-def]
                 {:builder-fn rs/as-unqualified-lower-maps}))

(defn temp-db-fixture [f]
  (let [file (java.io.File/createTempFile "tenon-test" ".db")]
    (.deleteOnExit file)
    (try
      (binding [*db-path* (.getAbsolutePath file)
                engine/*workflow-engine* (engine/init (.getAbsolutePath file))]
        (f))
      (finally
        (.delete file)))))
