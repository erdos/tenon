(ns tenon.workflow.sqlite
  "The SQLite-backed storage implementation.

   An append-only event log plus a table of pointers into it.

   - event: every state a workflow entered, plus REUSED. Its rowid is the
     global order, and a STARTED event's id is the invocation id - so new
     workflows and restarts draw from one increasing sequence, and any
     invocation id, current or past, is a primary key lookup away from
     its workflow. Payloads (metadata, results) are stored here, once.
   - workflow: one narrow row of pointers per workflow, keyed by its first
     invocation id: its current invocation, the event of its current
     state, its latest event and the latest call it made. Writes are
     compare-and-set against these pointers.
   - idempotence: SHA-256 of (wf_def, params) -> workflow, the uniqueness
     guarantee.
   - wf_def: the distinct wf_def names, so events carry a small integer
     instead of the name, and the dropdown lists a handful of rows.

   Instead of indexing events by workflow or by caller, events form
   linked lists, threaded through ids the writer already has at hand:
   prev_id links a workflow's states (newest first, starting at
   workflow.state_id) and, separately, its reuses (starting at
   workflow.last_reuse_id); prev_call_id links the calls a workflow made
   (starting at workflow.last_call_id). Walking them costs one primary key
   lookup per event - what fetching each row through an index costs anyway
   - but nothing is stored or maintained for them beyond the pointers.

   Every write is a single statement that appends one event; triggers
   move the pointers along. SQLite runs one writer at a time, so a
   statement's checks and writes are atomic, and ids are handed out in
   commit order."
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [tenon.workflow.db :as db]
            [tenon.workflow.sqlite.pool :as pool])
  (:import [java.security MessageDigest]
           [tenon.workflow.sqlite.pool Pool]))

(def ^:private now-ms
  "SQL expression for the current UTC time in epoch milliseconds. SQLite
   keeps 'now' fixed for the duration of a statement, so every use within
   one statement yields the same value."
  "CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER)")

(defn- idempotence-key ^bytes [wf-def params-edn-str]
  (.digest (MessageDigest/getInstance "SHA-256")
           (.getBytes (str wf-def "\u0000" params-edn-str) "UTF-8")))

(def ^:private schema
  ["CREATE TABLE IF NOT EXISTS wf_def (
      id        INTEGER PRIMARY KEY,
      name      TEXT    NOT NULL UNIQUE,
      top_level INTEGER NOT NULL DEFAULT 0  -- called at the top level at least once
    ) STRICT"
   ;; Calls - a workflow's first STARTED event and every REUSED event -
   ;; carry the called wf_def_id, which also tells them from the rest.
   ;; parent_invocation_id is only stored on calls: the caller. The other
   ;; events of a workflow share its first event's.
   "CREATE TABLE IF NOT EXISTS event (
      id                   INTEGER PRIMARY KEY,
      workflow_id          INTEGER NOT NULL,
      state                TEXT    NOT NULL CHECK (state IN ('STARTED', 'DONE', 'ERROR', 'REUSED')),
      at                   INTEGER NOT NULL,
      data                 TEXT,     -- metadata (STARTED, REUSED) or result (DONE, ERROR)
      invocation_id        INTEGER,  -- DONE, ERROR, REUSED: the invocation finished or reused
      expires_at           INTEGER,  -- STARTED: the lease
      parent_invocation_id INTEGER,  -- calls: the caller's invocation
      wf_def_id            INTEGER,  -- calls: the called wf_def
      prev_id              INTEGER,  -- the workflow's previous state event, or for REUSED its previous reuse
      prev_call_id         INTEGER,  -- calls: the previous call made by the caller's workflow
      params               TEXT,     -- the workflow's first event: its params, last as they may be large
      CHECK ((invocation_id IS NULL) = (state = 'STARTED')),
      CHECK (expires_at IS NULL OR state = 'STARTED'),
      CHECK ((wf_def_id IS NULL) = (state <> 'REUSED' AND id <> workflow_id))
    ) STRICT"
   ;; A wf_def's calls in call order, top-level ones first: the listing's
   ;; wf_def filter is one or two ranges of it.
   "CREATE INDEX IF NOT EXISTS event_call
      ON event(wf_def_id, parent_invocation_id IS NULL) WHERE wf_def_id IS NOT NULL"
   "CREATE TABLE IF NOT EXISTS workflow (
      id            INTEGER PRIMARY KEY,  -- its first invocation id
      invocation_id INTEGER NOT NULL,     -- the current invocation (its STARTED event)
      state         TEXT    NOT NULL CHECK (state IN ('STARTED', 'DONE', 'ERROR')), -- a copy of the current state event's, for filters
      state_id      INTEGER NOT NULL,     -- the current state's event: invocation_id while STARTED
      last_reuse_id INTEGER,              -- its latest REUSED event, head of their prev_id list
      last_call_id  INTEGER               -- the latest call it made, head of the prev_call_id list
    ) STRICT"
   ;; The few running workflows.
   "CREATE INDEX IF NOT EXISTS workflow_started ON workflow(id) WHERE state = 'STARTED'"
   "CREATE TABLE IF NOT EXISTS idempotence (
      key         BLOB    PRIMARY KEY,
      workflow_id INTEGER NOT NULL
    ) WITHOUT ROWID, STRICT"
   ;; A workflow's first event creates its row ...
   "CREATE TRIGGER IF NOT EXISTS event_first AFTER INSERT ON event
    WHEN NEW.id = NEW.workflow_id
    BEGIN
      INSERT INTO workflow (id, invocation_id, state, state_id) VALUES (NEW.id, NEW.id, 'STARTED', NEW.id);
    END"
   ;; ... and every later one moves its pointers.
   "CREATE TRIGGER IF NOT EXISTS event_next AFTER INSERT ON event
    WHEN NEW.id <> NEW.workflow_id
    BEGIN
      UPDATE workflow
         SET invocation_id = iif(NEW.state = 'STARTED', NEW.id, invocation_id),
             state = iif(NEW.state = 'REUSED', state, NEW.state),
             state_id = iif(NEW.state = 'REUSED', state_id, NEW.id),
             last_reuse_id = iif(NEW.state = 'REUSED', NEW.id, last_reuse_id)
       WHERE id = NEW.workflow_id;
    END"
   "CREATE TRIGGER IF NOT EXISTS event_call AFTER INSERT ON event
    WHEN NEW.wf_def_id IS NOT NULL
    BEGIN
      UPDATE workflow SET last_call_id = NEW.id
       WHERE id = (SELECT workflow_id FROM event WHERE id = NEW.parent_invocation_id);
      UPDATE wf_def SET top_level = 1
       WHERE id = NEW.wf_def_id AND NEW.parent_invocation_id IS NULL AND top_level = 0;
    END"
   ;; A workflow's current state, all fields: the pointers resolved.
   "CREATE VIEW IF NOT EXISTS workflow_state AS
    SELECT w.id AS workflow_id, w.invocation_id, d.name, f.params, f.parent_invocation_id,
           w.state, s.at AS state_changed_at, s.expires_at, i.data AS metadata,
           iif(w.state = 'STARTED', NULL, s.data) AS result
      FROM workflow w
      CROSS JOIN event s ON s.id = w.state_id
      CROSS JOIN event i ON i.id = w.invocation_id
      CROSS JOIN event f ON f.id = w.id
      CROSS JOIN wf_def d ON d.id = f.wf_def_id"
   ;; insert-workflow!: the only write that touches more than the log -
   ;; an INSERT into this view checks and claims the idempotence key and
   ;; logs the first event (whose id the INSERT computes, RETURNING it).
   ;; RAISE(IGNORE) drops the row, so a taken key returns nothing.
   "CREATE VIEW IF NOT EXISTS new_workflow (key, id, wf_def, params, parent_invocation_id, metadata, at, expires_at)
      AS SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL WHERE 0"
   "CREATE TRIGGER IF NOT EXISTS new_workflow INSTEAD OF INSERT ON new_workflow
    BEGIN
      SELECT RAISE(IGNORE) FROM idempotence WHERE key = NEW.key;
      INSERT INTO idempotence (key, workflow_id) VALUES (NEW.key, NEW.id);
      INSERT OR IGNORE INTO wf_def (name) VALUES (NEW.wf_def);
      INSERT INTO event (id, workflow_id, state, at, data, expires_at, parent_invocation_id, wf_def_id,
                         prev_call_id, params)
      VALUES (NEW.id, NEW.id, 'STARTED', NEW.at, NEW.metadata, NEW.expires_at, NEW.parent_invocation_id,
              (SELECT id FROM wf_def WHERE name = NEW.wf_def),
              (SELECT w.last_call_id FROM event p JOIN workflow w ON w.id = p.workflow_id
                WHERE p.id = NEW.parent_invocation_id),
              NEW.params);
    END"])

(def ^:private state-columns
  "The columns of a workflow_state row (aliased s) returned by Storage."
  "s.invocation_id, s.name, s.params, s.parent_invocation_id, s.state,
   s.state_changed_at, s.expires_at, s.metadata, s.result")

(def ^:private plain-opts
  "Rows keyed by their lower-case column labels, as in the SQL."
  {:builder-fn rs/as-unqualified-lower-maps})

(def ^:private opts
  "Rows of Storage methods: column labels become kebab-case keywords
   qualified with :workflow, e.g. state_changed_at -> :workflow/state-changed-at."
  {:builder-fn (fn [rs opts]
                 (rs/as-modified-maps rs (assoc opts
                                                :label-fn #(str/replace (str/lower-case %) \_ \-)
                                                :qualifier-fn (constantly "workflow"))))})

(defn- write!
  "Runs a write statement, returning its first row (RETURNING) if any."
  [ds sql-params]
  (pool/write! ds #(jdbc/execute-one! % sql-params plain-opts)))

(defn- finish!
  "Appends a state-ended event for invocation-id - if it is still the
   current state of its workflow (so STARTED, and neither finished,
   timed out nor restarted) and, when expired-only?, its lease is over."
  [ds invocation-id state result-edn-str expired-only?]
  (some? (write! ds [(str "INSERT INTO event (workflow_id, state, at, data, invocation_id, prev_id)
                                      SELECT w.id, ?, " now-ms ", ?, w.invocation_id, w.state_id
                                        FROM event i
                                        JOIN workflow w ON w.id = i.workflow_id
                                       WHERE i.id = ? AND w.state_id = i.id"
                                     (when expired-only? (str " AND i.expires_at <= " now-ms))
                                     " RETURNING id")
                     state result-edn-str invocation-id])))

(def ^:private call-columns
  (str "c.id AS call_id, c.at AS created_at, c.parent_invocation_id, c.state = 'REUSED' AS reused, "
       "s.invocation_id, s.name, s.params, s.state, s.state_changed_at, s.expires_at, s.metadata, s.result"))

(defn- calls-sql
  "SQL and params for top-level-workflows. Calls are found
   - STARTED: through the few running workflows - each one's first call,
     and its reuses from its own event list;
   - a wf_def: from its range of event_call - one for top-level calls only,
     two merged otherwise;
   - else: walking the log newest first."
  [{:keys [state wf-def top-level-only? limit before] :or {top-level-only? true}}]
  (let [limit (when limit (inc limit))
        where (fn [& conditions] (str/join " AND " (remove str/blank? conditions)))
        order (str " ORDER BY c.id DESC" (when limit " LIMIT ?"))
        def-id "(SELECT id FROM wf_def WHERE name = ?)"]
    (cond
      (= "STARTED" state)
      ;; A running workflow's first call is in workflow_started's order,
      ;; so a page of them is a range of it. Its reuses are in its reuse
      ;; list, newest first - and only those newer than the page's oldest
      ;; first call can make it onto the page, so each list is cut there.
      (let [filters (fn [id] (where (when top-level-only? "c.parent_invocation_id IS NULL")
                                    (when wf-def (str "c.wf_def_id = " def-id))
                                    (when before (str id " < ?"))))
            filter-params (cond-> [] wf-def (conj wf-def) before (conj before))]
        [(str "WITH RECURSIVE
                 first(id) AS (
                   SELECT r.id FROM workflow r INDEXED BY workflow_started CROSS JOIN event c ON c.id = r.id
                    WHERE " (where "r.state = 'STARTED'" (filters "r.id")) "
                    ORDER BY r.id DESC" (when limit " LIMIT ?") "),
                 bound(id) AS (" (if limit "SELECT iif(count(*) < ?, 0, min(id)) FROM first" "SELECT 0") "),
                 reuse(id) AS (
                   SELECT w.last_reuse_id FROM bound CROSS JOIN workflow w INDEXED BY workflow_started
                    WHERE w.state = 'STARTED' AND w.last_reuse_id >= bound.id
                   UNION ALL
                   SELECT e.prev_id FROM reuse r CROSS JOIN event e ON e.id = r.id CROSS JOIN bound
                    WHERE e.prev_id >= bound.id)
               SELECT * FROM (
                 SELECT " call-columns "
                   FROM first x
                   CROSS JOIN event c ON c.id = x.id
                   CROSS JOIN workflow_state s ON s.workflow_id = x.id
                 UNION ALL
                 SELECT " call-columns "
                   FROM reuse x
                   CROSS JOIN event c ON c.id = x.id
                   CROSS JOIN workflow_state s ON s.workflow_id = c.workflow_id
                  WHERE " (where "c.state = 'REUSED'" (filters "c.id")) ")
               ORDER BY call_id DESC" (when limit " LIMIT ?"))
         (cond-> filter-params
           limit (conj limit limit)
           true (into filter-params)
           limit (conj limit))])

      wf-def
      (let [branch (fn [top-level]
                     (str "SELECT " call-columns "
                             FROM event c INDEXED BY event_call
                             CROSS JOIN workflow_state s ON s.workflow_id = c.workflow_id
                            WHERE " (where (str "c.wf_def_id = " def-id)
                                           (str "(c.parent_invocation_id IS NULL) = " top-level)
                                           (when state "s.state = ?")
                                           (when before "c.id < ?"))
                          order))
            params (cond-> [wf-def] state (conj state) before (conj before) limit (conj limit))]
        (if top-level-only?
          [(branch 1) params]
          [(str "SELECT * FROM (SELECT * FROM (" (branch 1) ") UNION ALL SELECT * FROM (" (branch 0) "))
                 ORDER BY call_id DESC" (when limit " LIMIT ?"))
           (cond-> (into params params) limit (conj limit))]))

      :else
      [(str "SELECT " call-columns "
               FROM event c NOT INDEXED
               CROSS JOIN workflow_state s ON s.workflow_id = c.workflow_id
              WHERE " (where "c.wf_def_id IS NOT NULL"
                             (when top-level-only? "c.parent_invocation_id IS NULL")
                             (when state "s.state = ?")
                             (when before "c.id < ?"))
            order)
       (cond-> [] state (conj state) before (conj before) limit (conj limit))])))

(extend-protocol db/Storage
  Pool
  (init-db! [this]
    (jdbc/with-transaction [tx this]
      (doseq [ddl schema]
        (jdbc/execute! tx [ddl])))
    this)

  (insert-workflow! [this wf-def params-edn-str parent-invocation-id metadata-edn-str timeout-ms]
    (:id (write! this [(str "INSERT INTO new_workflow (key, id, wf_def, params, parent_invocation_id,
                                                                  metadata, at, expires_at)
                                        SELECT ?, coalesce(max(id), 0) + 1, ?, ?, ?, ?, " now-ms ", " now-ms " + ?
                                          FROM event
                                        RETURNING id")
                                   (idempotence-key wf-def params-edn-str) wf-def params-edn-str
                       parent-invocation-id metadata-edn-str timeout-ms])))

  (finish! [this invocation-id state result-edn-str]
    (finish! this invocation-id state result-edn-str false))

  (time-out! [this invocation-id result-edn-str]
    (finish! this invocation-id "ERROR" result-edn-str true))

  (restart! [this invocation-id metadata-edn-str timeout-ms]
    (:id (write! this [(str "INSERT INTO event (workflow_id, state, at, data, expires_at, prev_id)
                                        SELECT w.id, 'STARTED', " now-ms ", ?, " now-ms " + ?, w.state_id
                                          FROM event i
                                          JOIN workflow w ON w.id = i.workflow_id
                                         WHERE i.id = ? AND w.invocation_id = i.id AND w.state_id <> i.id
                                        RETURNING id")
                       metadata-edn-str timeout-ms invocation-id])))

  (record-reuse! [this invocation-id parent-invocation-id metadata-edn-str]
    (write! this [(str "INSERT INTO event (workflow_id, state, at, data, invocation_id,
                                                      parent_invocation_id, wf_def_id, prev_id, prev_call_id)
                                   SELECT w.id, 'REUSED', " now-ms ", ?, i.id, ?, f.wf_def_id, w.last_reuse_id,
                                          (SELECT pw.last_call_id FROM event p JOIN workflow pw ON pw.id = p.workflow_id
                                            WHERE p.id = ?)
                                     FROM event i
                                     JOIN workflow w ON w.id = i.workflow_id
                                     JOIN event f ON f.id = w.id
                                    WHERE i.id = ? AND i.state = 'STARTED'")
                             metadata-edn-str parent-invocation-id parent-invocation-id invocation-id])
    nil)

  (get-workflow [this invocation-id]
    (some-> (jdbc/execute-one! this [(str "SELECT " state-columns ", s.expires_at <= " now-ms " IS 1 AS expired
                                             FROM event i
                                             CROSS JOIN workflow_state s ON s.workflow_id = i.workflow_id
                                            WHERE i.id = ? AND i.state = 'STARTED'")
                                     invocation-id]
                               opts)
            (update :workflow/expired pos?)))

  (find-by-wf-def-and-params [this wf-def params-edn-str]
    (jdbc/execute-one! this [(str "SELECT " state-columns "
                                     FROM idempotence k
                                     CROSS JOIN workflow_state s ON s.workflow_id = k.workflow_id
                                    WHERE k.key = ?")
                             (idempotence-key wf-def params-edn-str)]
                       opts))

  (top-level-workflows
    ([this]
     (db/top-level-workflows this nil))
    ([this filters]
     (let [[sql params] (calls-sql filters)]
       (mapv #(update % :workflow/reused pos?) (jdbc/execute! this (into [sql] params) opts)))))

  (top-level-wf-defs [this top-level-only?]
    (mapv :name (jdbc/execute! this ["SELECT name FROM wf_def WHERE top_level OR NOT ? ORDER BY name"
                                     (boolean top-level-only?)]
                               plain-opts)))

  (full-timeline [this invocation-id]
    ;; The tree is found by walking call lists: the root's calls, and the
    ;; calls of every workflow first started by one of them (a workflow
    ;; has one first call, so it is reached once). A REUSED call is a leaf
    ;; - the reused workflow didn't run there - listed a level below its
    ;; caller. Each tree workflow's states come from its state list; of
    ;; the reuses of tree workflows only the root's made from outside the
    ;; tree are added (at depth 0) - from inside they are calls, listed
    ;; already, and other workflows' outside reuses aren't part of it.
    (jdbc/execute! this
      ["WITH RECURSIVE
          root(id) AS (SELECT workflow_id FROM event WHERE id = ?1 AND state = 'STARTED'),
          call(id, depth) AS (
            SELECT w.last_call_id, 1 FROM root CROSS JOIN workflow w ON w.id = root.id
             WHERE w.last_call_id IS NOT NULL
            UNION ALL
            SELECT e.prev_call_id, c.depth FROM call c CROSS JOIN event e ON e.id = c.id
             WHERE e.prev_call_id IS NOT NULL
            UNION ALL
            SELECT w.last_call_id, c.depth + 1 FROM call c
             CROSS JOIN event e ON e.id = c.id
             CROSS JOIN workflow w ON w.id = e.workflow_id
             WHERE e.state = 'STARTED' AND w.last_call_id IS NOT NULL
          ),
          tree(id, depth) AS (
            SELECT id, 0 FROM root
            UNION ALL
            SELECT e.workflow_id, c.depth FROM call c CROSS JOIN event e ON e.id = c.id WHERE e.state = 'STARTED'
          ),
          own(id, depth) AS (
            SELECT w.state_id, t.depth FROM tree t CROSS JOIN workflow w ON w.id = t.id
            UNION ALL
            SELECT w.last_reuse_id, 0 FROM root CROSS JOIN workflow w ON w.id = root.id
             WHERE w.last_reuse_id IS NOT NULL
            UNION ALL
            SELECT e.prev_id, o.depth FROM own o CROSS JOIN event e ON e.id = o.id WHERE e.prev_id IS NOT NULL
          ),
          entry(id, depth) AS (
            SELECT o.id, o.depth FROM own o CROSS JOIN event e ON e.id = o.id
             WHERE e.state <> 'REUSED'
                OR NOT EXISTS (SELECT 1 FROM event p JOIN tree t ON t.id = p.workflow_id
                                WHERE p.id = e.parent_invocation_id)
            UNION ALL
            SELECT c.id, c.depth FROM call c CROSS JOIN event e ON e.id = c.id WHERE e.state = 'REUSED'
          )
        SELECT coalesce(e.invocation_id, e.id) AS invocation_id, w.invocation_id AS current_invocation_id,
               iif(e.state = 'REUSED', e.parent_invocation_id, f.parent_invocation_id) AS parent_invocation_id,
               d.name, e.state, e.at AS state_changed_at, e.data, x.depth
          FROM entry x
          CROSS JOIN event e ON e.id = x.id
          CROSS JOIN workflow w ON w.id = e.workflow_id
          CROSS JOIN event f ON f.id = w.id
          CROSS JOIN wf_def d ON d.id = f.wf_def_id
         ORDER BY e.id"
       invocation-id]
      opts)))

(defn datasource
  "A connection pool (tenon.workflow.sqlite.pool) for the SQLite file at
   db-path (creating it if needed) - just the connections, no schema. Pass
   it to tenon.workflow.db/init-db! to also ensure the schema exists (a
   plain Storage method now - dispatches on this being a pool, same as
   every other Storage method). Every connection waits up to 5s for a
   concurrent writer's lock, keeps its prepared statements and maps the
   file into memory, and the file is in WAL mode so readers don't block
   writers. Commits are durable (synchronous stays FULL). The pool is Closeable:
   close it (e.g. with-open or .close) once it's no longer needed."
  ^java.io.Closeable [db-path]
  ;; Reads are primary key lookups scattered over the event table, so
  ;; they are served from a memory map of the file (shared by every
  ;; connection, through the OS page cache) rather than copied into each
  ;; connection's small default page cache.
  (pool/open (str "jdbc:sqlite:" db-path "?journal_mode=WAL&busy_timeout=5000"
                  "&mmap_size=1073741824&cache_size=-16384&temp_store=MEMORY")
             10))
