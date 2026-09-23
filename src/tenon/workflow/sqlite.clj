(ns tenon.workflow.sqlite
  "The SQLite-backed storage implementation."
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [tenon.workflow.db :as db])
  (:import [java.security MessageDigest]
           [java.time LocalDateTime]
           [javax.sql DataSource]))

(def ^:private now-ms
  "SQL expression for the current UTC time with millisecond precision,
   \"YYYY-MM-DD HH:MM:SS.SSS\" - unlike CURRENT_TIMESTAMP, which stops at
   seconds. Same format, so both sort correctly against each other."
  "strftime('%Y-%m-%d %H:%M:%f', 'now')")

(defn- ->local-date-time
  "Parses a timestamp as now-ms (or, in rows written before changed_at had
   millisecond precision, CURRENT_TIMESTAMP) formats it."
  ^LocalDateTime [^String s]
  (LocalDateTime/parse (.replace s " " "T")))

(defn- dedup-key [wf-def arguments-edn-str]
  (->> (.getBytes (str wf-def "\u0000" arguments-edn-str) "UTF-8")
       (.digest (MessageDigest/getInstance "SHA-256"))
       (map #(format "%02x" (bit-and % 0xff)))
       (apply str)))

(def ^:private with-created-and-state
  "SELECT w.id, w.wf_def, w.parent_workflow_id,
          (SELECT changed_at FROM workflow_events e
           WHERE e.workflow_id = w.id ORDER BY e.id ASC LIMIT 1) AS created_at,
          (SELECT state FROM workflow_events e
           WHERE e.workflow_id = w.id ORDER BY e.id DESC LIMIT 1) AS state
     FROM workflow w")

(extend-protocol db/Storage
  DataSource
  (init-db! [this]
    (doto this
      (jdbc/execute! ["CREATE TABLE IF NOT EXISTS workflow (
                                id TEXT PRIMARY KEY,
                                wf_def TEXT NOT NULL,
                                arguments TEXT NOT NULL,
                                parent_workflow_id TEXT REFERENCES workflow(id),
                                metadata TEXT,
                                dedup_key TEXT NOT NULL)"])
      (jdbc/execute! ["CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_dedup_key
                            ON workflow(dedup_key)"])
      (jdbc/execute! [(str "CREATE TABLE IF NOT EXISTS workflow_events (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                workflow_id TEXT NOT NULL REFERENCES workflow(id),
                                changed_at TEXT NOT NULL DEFAULT (" now-ms "),
                                state TEXT NOT NULL CHECK (state IN ('STARTED', 'DONE', 'ERROR')),
                                payload TEXT)")])
      (jdbc/execute! ["CREATE INDEX IF NOT EXISTS idx_workflow_events_workflow_id
                            ON workflow_events(workflow_id)"])
      (jdbc/execute! ["CREATE INDEX IF NOT EXISTS idx_workflow_parent_workflow_id
                            ON workflow(parent_workflow_id)"])))

  (insert-workflow! [this id wf-def arguments-edn-str parent-workflow-id metadata-edn-str]
    (-> (jdbc/execute! this ["INSERT INTO workflow (id, wf_def, arguments, parent_workflow_id, metadata, dedup_key)
                                      VALUES (?, ?, ?, ?, ?, ?)
                                      ON CONFLICT (dedup_key) DO NOTHING"
                             id wf-def arguments-edn-str parent-workflow-id metadata-edn-str
                             (dedup-key wf-def arguments-edn-str)])
        first :next.jdbc/update-count pos? (#{true}) (and id)))

  (insert-event! [this workflow-id state payload-edn-str]
    (doto this
      ;; changed_at given explicitly (not left to the column DEFAULT), so
      ;; databases created before it had millisecond precision get it too.
      (jdbc/execute! [(str "INSERT INTO workflow_events (workflow_id, state, payload, changed_at)
                            VALUES (?, ?, ?, " now-ms ")")
                      workflow-id state payload-edn-str])))

  (current-time [this]
    (->local-date-time
     (:now (jdbc/execute-one! this [(str "SELECT " now-ms " AS now")]
                              {:builder-fn rs/as-unqualified-lower-maps}))))

  (get-workflow [this id]
    (jdbc/execute-one! this ["SELECT * FROM workflow WHERE id = ?" id]
                       {:builder-fn rs/as-unqualified-lower-maps}))

  (latest-event [this workflow-id]
    (some-> (jdbc/execute-one! this ["SELECT * FROM workflow_events WHERE workflow_id = ? ORDER BY id DESC LIMIT 1" workflow-id]
                               {:builder-fn rs/as-unqualified-lower-maps})
            (update :changed_at ->local-date-time)))

  (find-by-wf-def-and-arguments [this wf-def arguments-edn-str]
    (jdbc/execute-one! this ["SELECT * FROM workflow WHERE dedup_key = ?" (dedup-key wf-def arguments-edn-str)]
                       {:builder-fn rs/as-unqualified-lower-maps}))

  (top-level-workflows
    ([this]
     (db/top-level-workflows this nil))
    ([this {:keys [state wf-def top-level-only? limit before] :or {top-level-only? true}}]
     (let [conditions (cond-> []
                         top-level-only? (conj "parent_workflow_id IS NULL")
                         state (conj "state = ?")
                         wf-def (conj "wf_def = ?")
                         before (conj "(created_at < ? OR (created_at = ? AND id < ?))"))
           params (cond-> []
                    state (conj state)
                    wf-def (conj wf-def)
                    before (into [(:created-at before) (:created-at before) (:id before)])
                    limit (conj (inc limit)))]
       (jdbc/execute! this
         (into [(str "SELECT * FROM (" with-created-and-state ")"
                     (when (seq conditions) (str " WHERE " (str/join " AND " conditions)))
                     " ORDER BY created_at DESC, id DESC"
                     (when limit " LIMIT ?"))]
               params)
         {:builder-fn rs/as-unqualified-lower-maps}))))

  (top-level-wf-defs [this top-level-only?]
    (mapv :wf_def (jdbc/execute! this
                    [(str "SELECT DISTINCT wf_def FROM workflow"
                          (when top-level-only? " WHERE parent_workflow_id IS NULL")
                          " ORDER BY wf_def")]
                    {:builder-fn rs/as-unqualified-lower-maps})))

  (full-timeline [this workflow-id]
    (jdbc/execute! this
      ["WITH RECURSIVE descendants(id, depth) AS (
          SELECT id, 0 FROM workflow WHERE id = ?
          UNION ALL
          SELECT w.id, d.depth + 1
          FROM workflow w JOIN descendants d ON w.parent_workflow_id = d.id
        )
        SELECT e.id, e.workflow_id, e.changed_at, e.state, e.payload, w.wf_def, d.depth
        FROM workflow_events e
        JOIN workflow w ON w.id = e.workflow_id
        JOIN descendants d ON d.id = e.workflow_id
        ORDER BY e.changed_at ASC, e.id ASC"
       workflow-id]
      {:builder-fn rs/as-unqualified-lower-maps}))

  (pending-workflows [this]
    (->> (jdbc/execute! this ["SELECT id FROM workflow"] {:builder-fn rs/as-unqualified-lower-maps})
         (keep (fn [{:keys [id]}]
                 (when (= "STARTED" (:state (db/latest-event this id)))
                   (db/get-workflow this id))))
         vec)))

(defn datasource
  "A javax.sql.DataSource for the SQLite file at db-path (creating it if
   needed) - just the connection, no schema. Pass it to
   tenon.workflow.db/init-db! to also ensure the schema exists (a plain
   Storage method now - dispatches on this being a DataSource, same as
   every other Storage method)."
  [db-path]
  (jdbc/get-datasource (str "jdbc:sqlite:" db-path)))
