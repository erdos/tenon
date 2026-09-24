(ns tenon.workflow.sqlite
  "The SQLite-backed storage implementation."
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [tenon.workflow.db :as db])
  (:import [java.security MessageDigest]
           [javax.sql DataSource]))

(def ^:private now-ms
  "SQL expression for the current UTC time in epoch milliseconds. SQLite
   keeps 'now' fixed for the duration of a statement, so every use within
   one statement yields the same value."
  "CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER)")

(defn- expires-at
  "SQL expression for the lease end timeout-ms after now-ms, NULL when the
   bound timeout-ms is. Takes two parameters, both timeout-ms."
  []
  (str "CASE WHEN ? IS NULL THEN NULL ELSE " now-ms " + ? END"))

(defn- idempotence-key ^bytes [wf-def params-edn-str]
  (.digest (MessageDigest/getInstance "SHA-256")
           (.getBytes (str wf-def "\u0000" params-edn-str) "UTF-8")))

(def ^:private schema
  ["CREATE TABLE IF NOT EXISTS workflow (
      invocation_id        INTEGER PRIMARY KEY AUTOINCREMENT,
      idempotence_key      BLOB    NOT NULL UNIQUE,
      wf_def               TEXT    NOT NULL,
      params               TEXT    NOT NULL,
      parent_invocation_id INTEGER,
      state                TEXT    NOT NULL CHECK (state IN ('STARTED', 'DONE', 'ERROR')),
      state_changed_at     INTEGER NOT NULL,
      expires_at           INTEGER,
      metadata             TEXT,
      result               TEXT,
      CHECK (state = 'STARTED' OR expires_at IS NULL)
    ) STRICT"
   ;; parent_invocation_id has no foreign key: it names the parent's
   ;; invocation at the time the child started, which a restart of the
   ;; parent moves into workflow_history.
   "CREATE INDEX IF NOT EXISTS idx_workflow_parent
      ON workflow(parent_invocation_id) WHERE parent_invocation_id IS NOT NULL"
   "CREATE INDEX IF NOT EXISTS idx_workflow_top_level
      ON workflow(invocation_id) WHERE parent_invocation_id IS NULL"
   "CREATE INDEX IF NOT EXISTS idx_workflow_wf_def
      ON workflow(wf_def, invocation_id)"
   "CREATE INDEX IF NOT EXISTS idx_workflow_state
      ON workflow(state, invocation_id)"
   "CREATE TABLE IF NOT EXISTS workflow_history (
      id               INTEGER PRIMARY KEY,
      idempotence_key  BLOB    NOT NULL REFERENCES workflow(idempotence_key),
      wf_invocation_id INTEGER NOT NULL,
      state            TEXT    NOT NULL,
      state_changed_at INTEGER NOT NULL,
      data             TEXT,
      parent_invocation_id INTEGER
    ) STRICT"
   "CREATE INDEX IF NOT EXISTS idx_workflow_history_key
      ON workflow_history(idempotence_key, id)"
   "CREATE INDEX IF NOT EXISTS idx_workflow_history_invocation
      ON workflow_history(wf_invocation_id)"
   "CREATE INDEX IF NOT EXISTS idx_workflow_history_parent
      ON workflow_history(parent_invocation_id) WHERE parent_invocation_id IS NOT NULL"
   ;; Logs the replaced state - lease-only updates (neither state nor
   ;; invocation_id changes) are not state changes.
   "CREATE TRIGGER IF NOT EXISTS workflow_log_history AFTER UPDATE ON workflow
    WHEN OLD.state IS NOT NEW.state OR OLD.invocation_id IS NOT NEW.invocation_id
    BEGIN
      INSERT INTO workflow_history (idempotence_key, wf_invocation_id, state, state_changed_at, data,
                                    parent_invocation_id)
      VALUES (OLD.idempotence_key, OLD.invocation_id, OLD.state, OLD.state_changed_at,
              CASE OLD.state WHEN 'STARTED' THEN OLD.metadata ELSE OLD.result END,
              OLD.parent_invocation_id);
    END"
   ;; AUTOINCREMENT only advances sqlite_sequence on INSERT - without this,
   ;; an invocation_id assigned by a restart could be handed out again.
   "CREATE TRIGGER IF NOT EXISTS workflow_bump_sequence AFTER UPDATE OF invocation_id ON workflow
    WHEN NEW.invocation_id > OLD.invocation_id
    BEGIN
      UPDATE sqlite_sequence SET seq = max(seq, NEW.invocation_id) WHERE name = 'workflow';
    END"
   "CREATE VIEW IF NOT EXISTS workflow_full_history AS
      SELECT id, idempotence_key, wf_invocation_id AS invocation_id,
             state, state_changed_at, data, parent_invocation_id
        FROM workflow_history
      UNION ALL
      SELECT NULL, idempotence_key, invocation_id, state, state_changed_at,
             CASE state WHEN 'STARTED' THEN metadata ELSE result END, parent_invocation_id
        FROM workflow"])

(def ^:private key-of-invocation
  "SQL expression for the idempotence_key of the workflow the bound
   invocation id - current or past - belongs to. Takes two parameters,
   both the invocation id."
  "COALESCE((SELECT idempotence_key FROM workflow WHERE invocation_id = ?),
            (SELECT idempotence_key FROM workflow_history WHERE wf_invocation_id = ? LIMIT 1))")

(def ^:private select-workflow-with-created-at
  "SELECT w.*,
          COALESCE((SELECT h.state_changed_at FROM workflow_history h
                     WHERE h.idempotence_key = w.idempotence_key ORDER BY h.id LIMIT 1),
                   w.state_changed_at) AS created_at
     FROM workflow w")

(def ^:private opts {:builder-fn rs/as-unqualified-lower-maps})

(extend-protocol db/Storage
  DataSource
  (init-db! [this]
    (jdbc/with-transaction [tx this]
      (doseq [ddl schema]
        (jdbc/execute! tx [ddl])))
    this)

  (insert-workflow! [this wf-def params-edn-str parent-invocation-id metadata-edn-str timeout-ms]
    (:invocation_id
     (jdbc/execute-one! this [(str "INSERT INTO workflow (idempotence_key, wf_def, params, parent_invocation_id,
                                                          metadata, state, state_changed_at, expires_at)
                                    VALUES (?, ?, ?, ?, ?, 'STARTED', " now-ms ", " (expires-at) ")
                                    ON CONFLICT (idempotence_key) DO NOTHING
                                    RETURNING invocation_id")
                               (idempotence-key wf-def params-edn-str) wf-def params-edn-str
                               parent-invocation-id metadata-edn-str timeout-ms timeout-ms]
                        opts)))

  (finish! [this invocation-id state result-edn-str]
    (-> (jdbc/execute-one! this [(str "UPDATE workflow
                                          SET state = ?, result = ?, expires_at = NULL,
                                              state_changed_at = " now-ms "
                                        WHERE invocation_id = ? AND state = 'STARTED'")
                                 state result-edn-str invocation-id])
        :next.jdbc/update-count pos?))

  (time-out! [this invocation-id result-edn-str]
    (-> (jdbc/execute-one! this [(str "UPDATE workflow
                                          SET state = 'ERROR', result = ?, expires_at = NULL,
                                              state_changed_at = " now-ms "
                                        WHERE invocation_id = ? AND state = 'STARTED'
                                          AND expires_at <= " now-ms)
                                 result-edn-str invocation-id])
        :next.jdbc/update-count pos?))

  (restart! [this invocation-id metadata-edn-str timeout-ms]
    (:invocation_id
     (jdbc/execute-one! this [(str "UPDATE workflow
                                       SET invocation_id = (SELECT seq + 1 FROM sqlite_sequence WHERE name = 'workflow'),
                                           state = 'STARTED', result = NULL, metadata = ?,
                                           state_changed_at = " now-ms ", expires_at = " (expires-at) "
                                     WHERE invocation_id = ? AND state IN ('DONE', 'ERROR')
                                     RETURNING invocation_id")
                               metadata-edn-str timeout-ms timeout-ms invocation-id]
                        opts)))

  (record-reuse! [this invocation-id parent-invocation-id metadata-edn-str]
    (jdbc/execute-one! this [(str "INSERT INTO workflow_history (idempotence_key, wf_invocation_id, state,
                                                                 state_changed_at, data, parent_invocation_id)
                                   VALUES (" key-of-invocation ", ?, 'REUSED', " now-ms ", ?, ?)")
                             invocation-id invocation-id invocation-id metadata-edn-str parent-invocation-id])
    nil)

  (get-workflow [this invocation-id]
    (some-> (jdbc/execute-one! this [(str "SELECT *, expires_at <= " now-ms " IS 1 AS expired
                                             FROM workflow
                                            WHERE idempotence_key = " key-of-invocation)
                                     invocation-id invocation-id]
                               opts)
            (update :expired pos?)))

  (find-by-wf-def-and-params [this wf-def params-edn-str]
    (jdbc/execute-one! this ["SELECT * FROM workflow WHERE idempotence_key = ?"
                             (idempotence-key wf-def params-edn-str)]
                       opts))

  (top-level-workflows
    ([this]
     (db/top-level-workflows this nil))
    ([this {:keys [state wf-def top-level-only? limit before] :or {top-level-only? true}}]
     (let [conditions (cond-> []
                        top-level-only? (conj "w.parent_invocation_id IS NULL")
                        state (conj "w.state = ?")
                        wf-def (conj "w.wf_def = ?")
                        before (conj "w.invocation_id < ?"))
           params (cond-> []
                    state (conj state)
                    wf-def (conj wf-def)
                    before (conj before)
                    limit (conj (inc limit)))]
       (jdbc/execute! this
         (into [(str select-workflow-with-created-at
                     (when (seq conditions) (str " WHERE " (str/join " AND " conditions)))
                     " ORDER BY w.invocation_id DESC"
                     (when limit " LIMIT ?"))]
               params)
         opts))))

  (top-level-wf-defs [this top-level-only?]
    (mapv :wf_def (jdbc/execute! this
                    [(str "SELECT DISTINCT wf_def FROM workflow"
                          (when top-level-only? " WHERE parent_invocation_id IS NULL")
                          " ORDER BY wf_def")]
                    opts)))

  (full-timeline [this invocation-id]
    ;; A child references whichever invocation of its parent it started
    ;; under, so descending the tree goes through every invocation_id
    ;; (current and past) of each workflow - one recursive step for each.
    ;; A REUSED history row links its workflow as a child of the reusing
    ;; parent too, so the same two steps are repeated over workflow_history.
    ;; UNION (not UNION ALL) drops the duplicate (key, depth) rows reached
    ;; via several past invocations. Both steps and the final select join
    ;; the tables directly rather than workflow_full_history: SQLite
    ;; can't push the join key into that view, so it would scan both
    ;; tables in full. CROSS JOIN makes SQLite start from the (small) tree
    ;; instead of scanning workflow_history for rows matching it.
    (jdbc/execute! this
      [(str "WITH RECURSIVE tree(idempotence_key, depth) AS (
          SELECT idempotence_key, 0 FROM workflow
           WHERE idempotence_key = " key-of-invocation "
          UNION
          SELECT c.idempotence_key, t.depth + 1
            FROM tree t
            JOIN workflow p ON p.idempotence_key = t.idempotence_key
            JOIN workflow c ON c.parent_invocation_id = p.invocation_id
          UNION
          SELECT c.idempotence_key, t.depth + 1
            FROM tree t
            JOIN workflow_history h ON h.idempotence_key = t.idempotence_key
            JOIN workflow c ON c.parent_invocation_id = h.wf_invocation_id
          UNION
          SELECT c.idempotence_key, t.depth + 1
            FROM tree t
            JOIN workflow p ON p.idempotence_key = t.idempotence_key
            JOIN workflow_history c ON c.parent_invocation_id = p.invocation_id
          UNION
          SELECT c.idempotence_key, t.depth + 1
            FROM tree t
            JOIN workflow_history h ON h.idempotence_key = t.idempotence_key
            JOIN workflow_history c ON c.parent_invocation_id = h.wf_invocation_id
        )
        SELECT invocation_id, current_invocation_id, wf_def, state, state_changed_at, data, depth
          FROM (SELECT h.wf_invocation_id AS invocation_id, w.invocation_id AS current_invocation_id,
                       w.wf_def, h.state, h.state_changed_at, h.data, t.depth, h.id AS history_id
                  FROM tree t
                  CROSS JOIN workflow w ON w.idempotence_key = t.idempotence_key
                  CROSS JOIN workflow_history h ON h.idempotence_key = t.idempotence_key
                UNION ALL
                SELECT w.invocation_id, w.invocation_id, w.wf_def, w.state, w.state_changed_at,
                       CASE w.state WHEN 'STARTED' THEN w.metadata ELSE w.result END, t.depth, NULL
                  FROM tree t
                  CROSS JOIN workflow w ON w.idempotence_key = t.idempotence_key)
         ORDER BY state_changed_at ASC, history_id IS NULL ASC, history_id ASC")
       invocation-id invocation-id]
      opts)))

(defn datasource
  "A javax.sql.DataSource for the SQLite file at db-path (creating it if
   needed) - just the connection, no schema. Pass it to
   tenon.workflow.db/init-db! to also ensure the schema exists (a plain
   Storage method now - dispatches on this being a DataSource, same as
   every other Storage method). Every connection enforces foreign keys and
   waits up to 5s for a concurrent writer's lock, and the file is in WAL
   mode so readers don't block writers."
  [db-path]
  (jdbc/get-datasource (str "jdbc:sqlite:" db-path
                            "?foreign_keys=true&journal_mode=WAL&busy_timeout=5000")))
