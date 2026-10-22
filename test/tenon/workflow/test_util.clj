(ns tenon.workflow.test-util
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
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
  "Every state - the current one included - of the workflow invocation-id
   belongs to, oldest first, straight from the event table (by a plain scan,
   not the per-workflow event list the library walks), keyed like
   full-timeline's rows. Test-only - the
   library reads it only as part of db/full-timeline."
  [ds invocation-id]
  (jdbc/execute! ds ["SELECT e.id, coalesce(e.invocation_id, e.id) AS invocation_id,
                             e.state, e.at AS state_changed_at, e.data,
                             iif(e.state = 'REUSED', e.parent_invocation_id, f.parent_invocation_id)
                               AS parent_invocation_id
                        FROM event e NOT INDEXED
                        JOIN event f ON f.id = e.workflow_id
                       WHERE e.workflow_id = (SELECT workflow_id FROM event WHERE id = ?)
                       ORDER BY e.id"
                     invocation-id]
                 {:builder-fn (fn [rs opts]
                                (rs/as-modified-maps rs (assoc opts
                                                               :label-fn #(str/replace (str/lower-case %) \_ \-)
                                                               :qualifier-fn (constantly "workflow"))))}))

(defn timeline
  "The states of the workflow invocation-id belongs to - not those of its
   sub-workflows - oldest first, via the public tenon.workflow/full-timeline."
  [invocation-id]
  (filterv #(zero? (:workflow/depth %)) (engine/full-timeline invocation-id)))

(defn find-by-wf-def
  "Workflows (of any nesting) whose wf_def is exactly wf-def, newest
   first, via the public tenon.workflow/list-invocations - reuses left out."
  [wf-def]
  (remove :workflow/reused (engine/list-invocations {:wf-def wf-def :top-level-only? false})))

(defn find-by-wf-def-and-params
  "The workflow run with exactly this wf-def and params (an edn string), or
   nil."
  [wf-def params]
  (first (filter #(= params (:workflow/params %)) (find-by-wf-def wf-def))))

(defn insert!
  "Inserts a STARTED workflow row for tests setting up a workflow's history,
   returning its invocation id. opts: :params (an edn string, \"[]\" by
   default), :parent, :metadata and :timeout-ms (nil by default - never
   expires)."
  [wf-def & {:keys [params parent metadata timeout-ms] :or {params "[]"}}]
  (db/insert-workflow! (ds) wf-def params parent metadata timeout-ms))

(defn finish!
  "Moves STARTED invocation-id to state (DONE or ERROR) with result, an edn
   string - as if the run it belongs to ended."
  [invocation-id state result]
  (db/finish! (ds) invocation-id state result))

(defn restart!
  "Moves DONE or ERROR invocation-id back to STARTED under a new
   invocation id, which it returns - without running anything."
  [invocation-id]
  (db/restart! (ds) invocation-id nil nil))

(defn record-reuse!
  "Logs that a caller under parent-invocation-id reused invocation-id's
   result, with metadata (an edn string, nil by default)."
  ([invocation-id parent-invocation-id] (record-reuse! invocation-id parent-invocation-id nil))
  ([invocation-id parent-invocation-id metadata]
   (db/record-reuse! (ds) invocation-id parent-invocation-id metadata)))

(defn expire!
  "Moves the lease end of STARTED invocation-id into the past, so it looks
   like it started long ago and timed out."
  [ds invocation-id]
  (jdbc/execute! ds ["UPDATE event SET at = 0, expires_at = 1 WHERE id = ? AND state = 'STARTED'"
                     invocation-id]))

(defn temp-db-fixture [f]
  (let [file (java.io.File/createTempFile "tenon-test" ".db")
        path (.getAbsolutePath file)]
    (try
      (let [engine (engine/init path)]
        (try
          (binding [*db-path* path
                    engine/*workflow-engine* engine]
            (f))
          (finally
            (.close ^java.io.Closeable (:tenon/db engine)))))
      (finally
        (doseq [suffix ["" "-wal" "-shm"]]
          (.delete (java.io.File. (str path suffix))))))))
