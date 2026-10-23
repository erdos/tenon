(ns tenon.workflow.sqlite
  "The SQLite-backed storage implementation.

   One append-only history table, and a current-state table that triggers
   keep in step with it.

   - event: the history - every state a workflow entered, plus REUSED. It
     is only ever appended to. Its rowid is the global order, and a STARTED
     event's id is the invocation id - so new workflows and restarts draw
     from one increasing sequence, and any invocation id, current or past,
     is a primary key lookup away from its workflow. Payloads (metadata,
     results) are stored here, once. States are small integers (see
     `states`): 0 and 1, the most common, take no space at all.
   - workflow: one row per workflow, keyed by its first invocation id,
     maintained by triggers on event: its identity (wf_def, params, the
     64-bit hash of both, its caller), its current invocation, state and
     lease, and the heads of its event lists. It holds everything a read
     needs but the payloads, so a workflow's current state is this row
     plus its current events. The lease is the one value written in place
     (extend-lease!): renewing it is not a state change, and is not logged.
   - wf_def: the distinct wf_def names, so rows carry a small integer
     instead of the name, and the dropdown lists a handful of rows.

   Idempotence is an index on the 64-bit hash of (wf_def, params): the
   lookup compares the params too, so a hash collision costs a row read,
   never a wrong answer. Uniqueness is guaranteed by the insert itself,
   which checks and inserts in one statement.

   Instead of indexing events by workflow or by caller, events form
   linked lists, threaded through ids the writer already has at hand:
   prev_id links a workflow's states (newest first, starting at
   workflow.state_id) and, separately, its reuses (starting at
   workflow.last_reuse_id); prev_call_id links the calls a workflow made
   (starting at workflow.last_call_id).

   Every write is a single statement that appends one event (a new
   workflow goes through the new_workflow view, whose trigger also writes
   its workflow row); triggers move the pointers along. SQLite runs one
   writer at a time, so a statement's checks and writes are atomic, and
   ids are handed out in commit order."
  (:require [clojure.string :as str]
            [tenon.workflow.db :as db]
            [tenon.workflow.sqlite.jdbc :refer [int-at int-or-nil-at row-fn text-at]]
            [tenon.workflow.sqlite.pool :as pool])
  (:import [java.sql ResultSet]
           [java.util.concurrent ConcurrentHashMap]
           [tenon.workflow.sqlite.pool Pool]))

(def ^:private states
  "State names by their code in the database."
  ["STARTED" "DONE" "ERROR" "TIMEOUT" "REUSED"])

(def ^:private state-code (zipmap states (range)))

(def ^:private placeholders
  "What sql fills in for each placeholder - in this order, so a value may
   use the placeholders after it."
  (concat
   [;; the columns of a workflow's current state (w and state-joins), as
    ;; read by read-state
    ["{{state-columns}}" "{{current-columns}}, w.parent_invocation_id"]
    ;; the columns of a call (c) and of the called workflow's current
    ;; state, as read by read-call
    ["{{call-columns}}" "{{current-columns}}, c.id AS call_id, c.occurred_at, c.parent_invocation_id, c.state"]
    ;; the columns of a workflow's current state but its parent, as read
    ;; by current-row-fn
    ["{{current-columns}}" "w.invocation_id, d.name, w.params, w.state, s.occurred_at, w.expires_at, i.data,
                            iif(w.state = {{STARTED}}, NULL, s.data)"]
    ;; the joins resolving the workflow row aliased w to its current
    ;; state: s its state event, i its invocation's (STARTED) event, d its
    ;; wf_def
    ["{{state-joins}}" "CROSS JOIN event s ON s.id = w.state_id
                        CROSS JOIN event i ON i.id = w.invocation_id
                        CROSS JOIN wf_def d ON d.id = w.wf_def_id"]
    ;; the columns of an event (aliased e) the timeline's CTEs carry along
    ["{{event-carried}}" "e.id, e.workflow_id, e.state, e.occurred_at, e.data, e.invocation_id,
                          e.parent_invocation_id"]
    ;; the current UTC time in epoch milliseconds - fixed for the duration
    ;; of a statement, so every use within one yields the same value
    ["{{now}}" "CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER)"]]
   ;; the states' codes: {{STARTED}}, {{DONE}}, ...
   (map-indexed (fn [code state] [(str "{{" state "}}") (str code)]) states)))

(defn- sql
  "The SQL of template, its placeholders filled in (see placeholders)."
  ^String [^String template]
  (reduce (fn [^String s [^String k ^String v]] (.replace s k v)) template placeholders))

(defn- idempotence-key
  "The 64-bit idempotence key of (wf-def, params) - see
   tenon.workflow.db/idempotence-key. A var of its own, so tests can make
   keys collide."
  ^long [^String wf-def ^String params]
  (db/idempotence-key wf-def params))

(def ^:private schema
  (mapv
   sql
   ["CREATE TABLE IF NOT EXISTS wf_def (
      id        INTEGER PRIMARY KEY,
      name      TEXT    NOT NULL UNIQUE,
      top_level INTEGER NOT NULL DEFAULT 0  -- called at the top level at least once
    ) STRICT"
   ;; Calls - a workflow's first STARTED event and every REUSED event -
   ;; carry the called wf_def_id, which also tells them from the rest.
   "CREATE TABLE IF NOT EXISTS event (
      id                   INTEGER PRIMARY KEY,
      workflow_id          INTEGER NOT NULL,
      state                INTEGER NOT NULL CHECK (state BETWEEN {{STARTED}} AND {{REUSED}}), -- see states
      occurred_at          INTEGER NOT NULL,
      data                 TEXT,     -- metadata (STARTED, REUSED) or result (DONE, ERROR, TIMEOUT)
      invocation_id        INTEGER,  -- DONE, ERROR, TIMEOUT, REUSED: the invocation finished or reused
      expires_at           INTEGER,  -- STARTED: the lease it was started with
      parent_invocation_id INTEGER,  -- calls: the caller's invocation
      wf_def_id            INTEGER,  -- calls: the called wf_def
      prev_id              INTEGER,  -- the workflow's previous state event, or for REUSED its previous reuse
      prev_call_id         INTEGER,  -- calls: the previous call made by the caller's workflow
      CHECK ((invocation_id IS NULL) = (state = {{STARTED}})),
      CHECK (expires_at IS NULL OR state = {{STARTED}}),
      CHECK ((wf_def_id IS NULL) = (state <> {{REUSED}} AND id <> workflow_id))
    ) STRICT"
   ;; A wf_def's calls in call order, top-level ones first: the listing's
   ;; wf_def filter is one or two ranges of it.
   "CREATE INDEX IF NOT EXISTS event_call
      ON event(wf_def_id, parent_invocation_id IS NULL) WHERE wf_def_id IS NOT NULL"
   ;; The top-level calls, in call order: the default listing. (Its key is
   ;; always NULL - it is the rowids that matter.)
   "CREATE INDEX IF NOT EXISTS event_top_call
      ON event(parent_invocation_id) WHERE wf_def_id IS NOT NULL AND parent_invocation_id IS NULL"
   "CREATE TABLE IF NOT EXISTS workflow (
      id                   INTEGER PRIMARY KEY,  -- its first invocation id
      key                  INTEGER NOT NULL,     -- idempotence-key of (wf_def, params)
      wf_def_id            INTEGER NOT NULL,
      parent_invocation_id INTEGER,              -- the caller of its first invocation
      invocation_id        INTEGER NOT NULL,     -- the current invocation (its STARTED event)
      state                INTEGER NOT NULL,     -- the current state's code
      state_id             INTEGER NOT NULL,     -- the current state's event: invocation_id while STARTED
      expires_at           INTEGER,              -- while STARTED: the lease
      last_reuse_id        INTEGER,              -- its latest REUSED event, head of their prev_id list
      last_call_id         INTEGER,              -- the latest call it made, head of the prev_call_id list
      params               TEXT    NOT NULL      -- last, as it may be large
    ) STRICT"
   "CREATE INDEX IF NOT EXISTS workflow_key ON workflow(key)"
   ;; The few workflows that are not DONE - the running ones and the failed
   ;; ones - by state, then id. A query using it has to say state <> DONE too,
   ;; for SQLite to see that the index holds all it needs.
   "CREATE INDEX IF NOT EXISTS workflow_not_done ON workflow(state, id) WHERE state <> {{DONE}}"
   ;; A later state moves the workflow's state pointers ...
   "CREATE TRIGGER IF NOT EXISTS event_state AFTER INSERT ON event
    WHEN NEW.state <> {{REUSED}} AND NEW.id <> NEW.workflow_id
    BEGIN
      UPDATE workflow
         SET invocation_id = iif(NEW.state = {{STARTED}}, NEW.id, invocation_id),
             state = NEW.state,
             state_id = NEW.id,
             expires_at = NEW.expires_at
       WHERE id = NEW.workflow_id;
    END"
   ;; ... a reuse its reuse list, and the caller's call list.
   "CREATE TRIGGER IF NOT EXISTS event_reuse AFTER INSERT ON event
    WHEN NEW.state = {{REUSED}}
    BEGIN
      UPDATE workflow SET last_reuse_id = NEW.id WHERE id = NEW.workflow_id;
      UPDATE workflow SET last_call_id = NEW.id
       WHERE id = (SELECT workflow_id FROM event WHERE id = NEW.parent_invocation_id);
      UPDATE wf_def SET top_level = 1
       WHERE id = NEW.wf_def_id AND NEW.parent_invocation_id IS NULL AND top_level = 0;
    END"
   ;; insert-workflow!: an INSERT into this view checks the idempotence
   ;; key, logs the first event (whose id the INSERT computes, RETURNING
   ;; it) and creates the workflow row. RAISE(IGNORE) drops the row, so a
   ;; taken key returns nothing.
   "CREATE VIEW IF NOT EXISTS new_workflow (key, id, wf_def, params, parent_invocation_id, metadata, occurred_at, expires_at)
      AS SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL WHERE 0"
   "CREATE TRIGGER IF NOT EXISTS new_workflow INSTEAD OF INSERT ON new_workflow
    BEGIN
      SELECT RAISE(IGNORE) FROM workflow w JOIN wf_def d ON d.id = w.wf_def_id
       WHERE w.key = NEW.key AND w.params = NEW.params AND d.name = NEW.wf_def;
      INSERT INTO wf_def (name, top_level) VALUES (NEW.wf_def, NEW.parent_invocation_id IS NULL)
        ON CONFLICT (name) DO UPDATE SET top_level = 1 WHERE excluded.top_level AND NOT top_level;
      INSERT INTO event (id, workflow_id, state, occurred_at, data, expires_at, parent_invocation_id, wf_def_id,
                         prev_call_id)
      VALUES (NEW.id, NEW.id, {{STARTED}}, NEW.occurred_at, NEW.metadata, NEW.expires_at, NEW.parent_invocation_id,
              (SELECT id FROM wf_def WHERE name = NEW.wf_def),
              (SELECT w.last_call_id FROM event p JOIN workflow w ON w.id = p.workflow_id
                WHERE p.id = NEW.parent_invocation_id));
      INSERT INTO workflow (id, key, wf_def_id, parent_invocation_id, invocation_id, state, state_id, expires_at,
                            params)
      VALUES (NEW.id, NEW.key, (SELECT id FROM wf_def WHERE name = NEW.wf_def), NEW.parent_invocation_id,
              NEW.id, {{STARTED}}, NEW.id, NEW.expires_at, NEW.params);
      UPDATE workflow SET last_call_id = NEW.id
       WHERE id = (SELECT workflow_id FROM event WHERE id = NEW.parent_invocation_id);
    END"]))

(defn- state-at ^Object [^ResultSet r ^long i]
  (states (.getInt r i)))

(defmacro ^:private current-row-fn [& kvs]
  `(row-fn :workflow/invocation-id (int-at %)
           :workflow/name (text-at %)
           :workflow/params (text-at %)
           :workflow/state (state-at %)
           :workflow/state-changed-at (int-at %)
           :workflow/expires-at (int-or-nil-at %)
           :workflow/metadata (text-at %)
           :workflow/result (text-at %)
           ~@kvs))

(def ^:private read-state
  "Rows of state-columns."
  (current-row-fn :workflow/parent-invocation-id (int-or-nil-at %)))

(defn- read-id [^ResultSet r]
  (.getLong r 1))

(defn- write!
  "Runs a write statement, returning the vector of its RETURNING ids."
  [ds sql params]
  (pool/write! ds #(pool/query % sql params read-id)))

(def ^:private finish-sql
  (sql "INSERT INTO event (workflow_id, state, occurred_at, data, invocation_id, prev_id)
       SELECT w.id, ?, {{now}}, ?, w.invocation_id, w.state_id
         FROM event i
         JOIN workflow w ON w.id = i.workflow_id
        WHERE i.id = ? AND w.state_id = i.id AND (? OR w.expires_at <= {{now}})
       RETURNING id"))

(defn- finish!
  "Appends a state-ended event for invocation-id - if it is still the
   current state of its workflow (so STARTED, and neither finished,
   timed out nor restarted) and, when expired-only?, its lease is over."
  [ds invocation-id state result-edn-str expired-only?]
  (boolean (seq (write! ds finish-sql [(state-code state) result-edn-str invocation-id (not expired-only?)]))))

(def ^:private read-call
  (current-row-fn :event/call-id (int-at %)
                  :event/created-at (int-at %)
                  :workflow/parent-invocation-id (int-or-nil-at %)
                  :event/reused (= "REUSED" (state-at %))))

(defn- calls-sql
  "A fn building the SQL, and the params, for top-level-workflows. Calls
   are found
   - STARTED, ERROR or TIMEOUT: through the few workflows in that state
     (workflow_not_done) - each one's first call, and its reuses from its
     own event list;
   - a wf_def: from its range of event_call - one for top-level calls only,
     two merged otherwise (the ids first, so only the page is joined);
   - else: from event_top_call for top-level calls only, else walking the
     log newest first."
  [{:keys [state wf-def top-level-only? limit before] :or {top-level-only? true}}]
  (let [limit (when limit (inc limit))
        code (some-> state state-code)
        and-all (fn ^String [& conditions] (apply str (interleave (repeat " AND ") (remove nil? conditions))))
        ^String limit-sql (if limit " LIMIT ?" "")
        def-filter "c.wf_def_id = (SELECT id FROM wf_def WHERE name = ?)"]
    (cond
      (#{"STARTED" "ERROR" "TIMEOUT"} state)
      ;; A first call of a workflow in the state is in workflow_not_done's
      ;; order, so a page of them is a range of it. Its reuses are in its
      ;; reuse list, newest first - and only those newer than the page's
      ;; oldest first call can make it onto the page, so each list is cut
      ;; there.
      (let [filters (fn ^String [^String id]
                      ^String (and-all (when top-level-only? "c.parent_invocation_id IS NULL")
                               (when wf-def def-filter)
                               (when before (.replace "{{id}} < ?" "{{id}}" id))))
            filter-params (cond-> [] wf-def (conj wf-def) before (conj before))]
        [#(-> "WITH RECURSIVE
                 first(id) AS (
                   SELECT r.id FROM workflow r INDEXED BY workflow_not_done CROSS JOIN event c ON c.id = r.id
                    WHERE r.state = {{state}} AND r.state <> {{DONE}}{{first-filters}}
                    ORDER BY r.id DESC{{limit}}),
                 bound(id) AS ({{bound}}),
                 reuse(id) AS (
                   SELECT w.last_reuse_id FROM bound CROSS JOIN workflow w INDEXED BY workflow_not_done
                    WHERE w.state = {{state}} AND w.state <> {{DONE}} AND w.last_reuse_id >= bound.id
                   UNION ALL
                   SELECT e.prev_id FROM reuse r CROSS JOIN event e ON e.id = r.id CROSS JOIN bound
                    WHERE e.prev_id >= bound.id)
               SELECT * FROM (
                 SELECT {{call-columns}}
                   FROM first x
                   CROSS JOIN event c ON c.id = x.id
                   CROSS JOIN workflow w ON w.id = x.id {{state-joins}}
                 UNION ALL
                 SELECT {{call-columns}}
                   FROM reuse x
                   CROSS JOIN event c ON c.id = x.id
                   CROSS JOIN workflow w ON w.id = c.workflow_id {{state-joins}}
                  WHERE c.state = {{REUSED}}{{reuse-filters}})
               ORDER BY call_id DESC{{limit}}"
              (.replace "{{state}}" ^String (str code))
              (.replace "{{first-filters}}" ^String (filters "r.id"))
              (.replace "{{reuse-filters}}" ^String (filters "c.id"))
              (.replace "{{bound}}" ^String (if limit "SELECT iif(count(*) < ?, 0, min(id)) FROM first" "SELECT 0"))
              (.replace "{{limit}}" limit-sql)
              sql)
         (cond-> filter-params
           limit (conj limit limit)
           true (into filter-params)
           limit (conj limit))])

      wf-def
      (let [conditions (fn ^String [top-level]
                         (-> "{{def-filter}} AND (c.parent_invocation_id IS NULL) = {{top-level}}{{filters}}"
                             (.replace "{{def-filter}}" def-filter)
                             (.replace "{{top-level}}" ^String (str top-level))
                             (.replace "{{filters}}" ^String (and-all (when state "w.state = ?") (when before "c.id < ?")))))
            params (cond-> [wf-def] state (conj code) before (conj before) limit (conj limit))]
        (if top-level-only?
          [#(-> "SELECT {{call-columns}}
                   FROM event c INDEXED BY event_call
                   CROSS JOIN workflow w ON w.id = c.workflow_id {{state-joins}}
                  WHERE {{conditions}}
                  ORDER BY c.id DESC{{limit}}"
                (.replace "{{conditions}}" ^String (conditions 1))
                (.replace "{{limit}}" limit-sql)
                sql)
           params]
          (let [ids (fn ^String [top-level]
                      (-> "SELECT c.id FROM event c INDEXED BY event_call{{workflow}}
                            WHERE {{conditions}}
                            ORDER BY c.id DESC{{limit}}"
                          (.replace "{{workflow}}" ^String (if state " CROSS JOIN workflow w ON w.id = c.workflow_id" ""))
                          (.replace "{{conditions}}" ^String (conditions top-level))
                          (.replace "{{limit}}" limit-sql)))]
            [#(-> "SELECT {{call-columns}}
                     FROM (SELECT id FROM (SELECT * FROM ({{top-level-ids}}) UNION ALL SELECT * FROM ({{nested-ids}}))
                            ORDER BY id DESC{{limit}}) x
                     CROSS JOIN event c ON c.id = x.id
                     CROSS JOIN workflow w ON w.id = c.workflow_id {{state-joins}}
                    ORDER BY c.id DESC"
                  (.replace "{{top-level-ids}}" ^String (ids 1))
                  (.replace "{{nested-ids}}" ^String (ids 0))
                  (.replace "{{limit}}" limit-sql)
                  sql)
             (cond-> (into params params) limit (conj limit))])))

      :else
      [#(-> "SELECT {{call-columns}}
               FROM event c {{index}}
               CROSS JOIN workflow w ON w.id = c.workflow_id {{state-joins}}
              WHERE c.wf_def_id IS NOT NULL{{filters}}
              ORDER BY c.id DESC{{limit}}"
            (.replace "{{index}}" ^String (if top-level-only? "INDEXED BY event_top_call" "NOT INDEXED"))
            (.replace "{{filters}}" ^String (and-all (when top-level-only? "c.parent_invocation_id IS NULL")
                                             (when state "w.state = ?")
                                             (when before "c.id < ?")))
            (.replace "{{limit}}" limit-sql)
            sql)
       (cond-> [] state (conj code) before (conj before) limit (conj limit))])))

(def ^:private calls-sql-cache
  "The SQL of top-level-workflows by the shape of its filters."
  (ConcurrentHashMap.))

(defn- calls-query
  "SQL and params for top-level-workflows - the SQL built once per shape
   of filters (which of them are given, and the state if it is one of
   those found through workflow_not_done)."
  [{:keys [state wf-def top-level-only? limit before] :or {top-level-only? true} :as filters}]
  (let [[sql params] (calls-sql filters)
        shape [(if (#{"STARTED" "ERROR" "TIMEOUT"} state) state (some? state))
               (some? wf-def) (boolean top-level-only?) (some? limit) (some? before)]]
    [(or (.get ^ConcurrentHashMap calls-sql-cache shape)
         (let [sql (sql)] (.put ^ConcurrentHashMap calls-sql-cache shape sql) sql))
     params]))

(defn- init-schema!
  "Creates the schema in a single transaction that takes the write lock up
   front. A deferred one reads first, and cannot then upgrade to a write if
   another process committed meanwhile (SQLite fails it at once, whatever
   the busy timeout) - while this way, of several processes starting at
   once on a new database one does the work and the others, having waited
   for the lock, find it done."
  [pool]
  (pool/write!
   pool
   (fn [c]
     (pool/execute! c "BEGIN IMMEDIATE")
     (try
       (doseq [ddl schema]
         (pool/execute! c ddl))
       (pool/execute! c "COMMIT")
       (catch Throwable e
         (try (pool/execute! c "ROLLBACK") (catch Throwable _))
         (throw e))))))

(def ^:private insert-sql
  (sql "INSERT INTO new_workflow (key, id, wf_def, params, parent_invocation_id, metadata, occurred_at, expires_at)
       SELECT ?, coalesce(max(id), 0) + 1, ?, ?, ?, ?, {{now}}, {{now}} + ?
         FROM event
       RETURNING id"))

(def ^:private restart-sql
  (sql "INSERT INTO event (workflow_id, state, occurred_at, data, expires_at, prev_id)
       SELECT w.id, {{STARTED}}, {{now}}, ?, {{now}} + ?, w.state_id
         FROM event i
         JOIN workflow w ON w.id = i.workflow_id
        WHERE i.id = ? AND w.invocation_id = i.id AND w.state <> {{STARTED}}
       RETURNING id"))

(def ^:private reuse-sql
  (sql "INSERT INTO event (workflow_id, state, occurred_at, data, invocation_id,
                          parent_invocation_id, wf_def_id, prev_id, prev_call_id)
       SELECT w.id, {{REUSED}}, {{now}}, ?, i.id, ?, w.wf_def_id, w.last_reuse_id,
              (SELECT pw.last_call_id FROM event p JOIN workflow pw ON pw.id = p.workflow_id
                WHERE p.id = ?)
         FROM event i
         JOIN workflow w ON w.id = i.workflow_id
        WHERE i.id = ? AND i.state = {{STARTED}}
       RETURNING id"))

(def ^:private get-workflow-sql
  (sql "SELECT {{state-columns}}, w.expires_at <= {{now}} IS 1
         FROM event x
         CROSS JOIN workflow w ON w.id = x.workflow_id {{state-joins}}
        WHERE x.id = ? AND x.state = {{STARTED}}"))

(def ^:private find-sql
  (sql "SELECT {{state-columns}}
         FROM workflow w INDEXED BY workflow_key {{state-joins}}
        WHERE w.key = ?"))

(def ^:private list-expired-sql
  (sql "SELECT {{state-columns}}
         FROM workflow w INDEXED BY workflow_not_done {{state-joins}}
        WHERE w.state = {{STARTED}} AND w.state <> {{DONE}} AND w.expires_at <= {{now}}
        ORDER BY w.id"))

(def ^:private timeline-sql
  (sql "WITH RECURSIVE
        root(id) AS (SELECT workflow_id FROM event WHERE id = ?1 AND state = {{STARTED}}),
        call(id, workflow_id, state, occurred_at, data, invocation_id, parent_invocation_id, depth, prev) AS (
          SELECT {{event-carried}}, 1, e.prev_call_id
            FROM root CROSS JOIN workflow w ON w.id = root.id CROSS JOIN event e ON e.id = w.last_call_id
          UNION ALL
          SELECT {{event-carried}}, c.depth, e.prev_call_id
            FROM call c CROSS JOIN event e ON e.id = c.prev
          UNION ALL
          SELECT {{event-carried}}, c.depth + 1, e.prev_call_id
            FROM call c CROSS JOIN workflow w ON w.id = c.workflow_id CROSS JOIN event e ON e.id = w.last_call_id
           WHERE c.state = {{STARTED}}
        ),
        tree(id, depth) AS (
          SELECT id, 0 FROM root
          UNION ALL
          SELECT workflow_id, depth FROM call WHERE state = {{STARTED}}
        ),
        own(id, workflow_id, state, occurred_at, data, invocation_id, parent_invocation_id, depth, prev) AS (
          SELECT {{event-carried}}, t.depth, e.prev_id
            FROM tree t CROSS JOIN workflow w ON w.id = t.id CROSS JOIN event e ON e.id = w.state_id
          UNION ALL
          SELECT {{event-carried}}, 0, e.prev_id
            FROM root CROSS JOIN workflow w ON w.id = root.id CROSS JOIN event e ON e.id = w.last_reuse_id
          UNION ALL
          SELECT {{event-carried}}, o.depth, e.prev_id
            FROM own o CROSS JOIN event e ON e.id = o.prev
        ),
        entry AS (
          SELECT * FROM own
           WHERE state <> {{REUSED}}
              OR NOT EXISTS (SELECT 1 FROM event p JOIN tree t ON t.id = p.workflow_id
                              WHERE p.id = own.parent_invocation_id)
          UNION ALL
          SELECT * FROM call WHERE state = {{REUSED}}
        )
      SELECT coalesce(x.invocation_id, x.id), w.invocation_id,
             iif(x.state = {{REUSED}}, x.parent_invocation_id, w.parent_invocation_id),
             d.name, x.state, x.occurred_at, x.data, x.depth
        FROM entry x
        CROSS JOIN workflow w ON w.id = x.workflow_id
        CROSS JOIN wf_def d ON d.id = w.wf_def_id
       ORDER BY x.id"))

(extend-protocol db/Storage
  Pool
  (init-db! [this]
    (init-schema! this)
    this)

  (insert-workflow! [this wf-def params-edn-str parent-invocation-id metadata-edn-str timeout-ms]
    (first (write! this insert-sql
                   [(idempotence-key wf-def params-edn-str) wf-def params-edn-str
                    parent-invocation-id metadata-edn-str timeout-ms])))

  (finish! [this invocation-id state result-edn-str]
    (finish! this invocation-id state result-edn-str false))

  (time-out! [this invocation-id result-edn-str]
    (finish! this invocation-id "TIMEOUT" result-edn-str true))

  (restart! [this invocation-id metadata-edn-str timeout-ms]
    (first (write! this restart-sql
                   [metadata-edn-str timeout-ms invocation-id])))

  (record-reuse! [this invocation-id parent-invocation-id metadata-edn-str]
    (write! this reuse-sql
            [metadata-edn-str parent-invocation-id parent-invocation-id invocation-id])
    nil)

  (get-workflow [this invocation-id]
    (first (pool/query this get-workflow-sql [invocation-id]
                       (current-row-fn :workflow/parent-invocation-id (int-or-nil-at %)
                                       :workflow/expired (.getBoolean %)))))

  (find-by-wf-def-and-params [this wf-def params-edn-str]
    ;; The rows with the key - all but never more than one - are told apart
    ;; by their name and params here, rather than by binding both to the
    ;; statement too.
    (some #(when (and (= params-edn-str (:workflow/params %)) (= wf-def (:workflow/name %))) %)
          (pool/query this find-sql [(idempotence-key wf-def params-edn-str)] read-state)))

  (top-level-workflows
    ([this]
     (db/top-level-workflows this nil))
    ([this filters]
     (let [[sql params] (calls-query filters)]
       (pool/query this sql params read-call))))

  (top-level-wf-defs [this top-level-only?]
    (pool/query this "SELECT name FROM wf_def WHERE top_level OR NOT ? ORDER BY name" [(boolean top-level-only?)]
                #(text-at % 1)))

  (full-timeline [this invocation-id]
    ;; The tree is found by walking call lists: the root's calls, and the
    ;; calls of every workflow first started by one of them (a workflow
    ;; has one first call, so it is reached once). A REUSED call is a leaf
    ;; - the reused workflow didn't run there - listed a level below its
    ;; caller. Each tree workflow's states come from its state list; of
    ;; the reuses of tree workflows only the root's made from outside the
    ;; tree are added (at depth 0) - from inside they are calls, listed
    ;; already, and other workflows' outside reuses aren't part of it.
    ;; Every event is looked up once: the CTE rows carry the columns
    ;; needed of it.
    (pool/query this timeline-sql
      [invocation-id]
      (row-fn :workflow/invocation-id (int-at %)
              :workflow/current-invocation-id (int-at %)
              :workflow/parent-invocation-id (int-or-nil-at %)
              :workflow/name (text-at %)
              :workflow/state (state-at %)
              :workflow/state-changed-at (int-at %)
              :event/data (text-at %)
              :workflow/depth (int-at %)))))

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

(def ^:private extend-lease-sql
  ;; The ids go in as a JSON array, twice: the workflows they belong to are
  ;; looked up by them, and then only those whose current invocation is
  ;; one of them are extended.
  (sql "UPDATE workflow SET expires_at = {{now}} + ?
        WHERE id IN (SELECT e.workflow_id FROM json_each(?) j CROSS JOIN event e ON e.id = j.value)
          AND invocation_id IN (SELECT value FROM json_each(?))
          AND state = {{STARTED}} AND expires_at IS NOT NULL
       RETURNING invocation_id"))

(extend-protocol db/Timeouts
  Pool
  (extend-lease! [this invocation-ids timeout-ms]
    (if (empty? invocation-ids)
      #{}
      ;; The ids go in as JSON arrays, so that the statement is the same
      ;; for any number of them (and cached once, whatever the batch size).
      (->> (pool/write! this #(pool/query % extend-lease-sql
                                          (let [ids (str "[" (str/join "," (map long invocation-ids)) "]")]
                                            [timeout-ms ids ids])
                                          read-id))
           (into #{}))))

  (list-expired [this]
    (pool/query this list-expired-sql []
                (current-row-fn :workflow/parent-invocation-id (int-or-nil-at %)
                                :workflow/expired Boolean/TRUE))))
