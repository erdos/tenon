(ns tenon.workflow.test-util
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [tenon.workflow :as engine]
            [tenon.workflow.db :as db]))

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

(defn history
  "Every state - past ones from workflow_history, then the current one - of
   the workflow invocation-id belongs to, oldest first. Test-only - the
   library reads it only as part of db/full-timeline."
  [ds invocation-id]
  (jdbc/execute! ds ["SELECT h.* FROM workflow_full_history h
                       JOIN workflow w ON w.idempotence_key = h.idempotence_key
                      WHERE w.invocation_id = ?
                      ORDER BY h.id IS NULL, h.id"
                     (:invocation_id (db/get-workflow ds invocation-id))]
                 {:builder-fn rs/as-unqualified-lower-maps}))

(defn find-by-wf-def
  "Workflow rows whose wf_def is exactly wf-def. Test-only - nothing in
   the library looks workflows up by wf_def alone, only by the
   (wf_def, params) pair (tenon.workflow.db/find-by-wf-def-and-params,
   part of the Storage protocol)."
  [ds wf-def]
  (jdbc/execute! ds ["SELECT * FROM workflow WHERE wf_def = ?" wf-def]
                 {:builder-fn rs/as-unqualified-lower-maps}))

(defn insert!
  "Inserts a STARTED workflow row for tests setting up a workflow's history,
   returning its invocation id. opts: :params (an edn string, \"[]\" by
   default), :parent, :metadata and :timeout-ms (nil by default - never
   expires)."
  [wf-def & {:keys [params parent metadata timeout-ms] :or {params "[]"}}]
  (db/insert-workflow! (ds) wf-def params parent metadata timeout-ms))

(defn expire!
  "Moves the lease end of STARTED invocation-id into the past, so it looks
   like it started long ago and timed out."
  [ds invocation-id]
  (jdbc/execute! ds ["UPDATE workflow SET state_changed_at = 0, expires_at = 1
                       WHERE invocation_id = ? AND state = 'STARTED'"
                     invocation-id]))

(defn temp-db-fixture [f]
  (let [file (java.io.File/createTempFile "tenon-test" ".db")
        path (.getAbsolutePath file)]
    (try
      (binding [*db-path* path
                engine/*workflow-engine* (engine/init path)]
        (f))
      (finally
        (doseq [suffix ["" "-wal" "-shm"]]
          (.delete (java.io.File. (str path suffix))))))))
