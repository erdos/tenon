(ns tenon.workflow.postgres
  "The PostgreSQL-backed storage implementation.

   The data model is the SQLite one (see tenon.workflow.sqlite): an
   append-only history table, and a current-state table kept in step with
   it.

   - event: the history - every state a workflow entered, plus REUSED. It
     is only ever appended to. Its ids come from one sequence, and a
     STARTED event's id is the invocation id, so any invocation id,
     current or past, is a primary key lookup away from its workflow.
     Payloads (metadata, results) are stored here, once - and so is a
     workflow's identity, on its first event: its wf_def, params, caller
     and the 64-bit idempotence key of wf_def and params.
   - workflow: one row per workflow, keyed by its first invocation id: what
     changes - its current invocation, state and lease, and the heads of
     its event lists. Nothing else: Postgres writes a whole new row version
     on every update, and this row is updated on every state change, lease
     renewal, reuse and call made. It leaves room in its pages
     (fillfactor) so that updates not touching an indexed column - all but
     the state changes - are HOT: the new version goes on the same page,
     and no index is written. A state change writes an entry in the
     primary key, and in workflow_started or workflow_failed if it is to
     one of their states - the identity's index entries are on the event,
     written once.
   - wf_def: the distinct wf_def names, so rows carry a small integer
     instead of the name, and the dropdown lists a handful of rows.

   As in SQLite, events are not indexed by workflow or by caller: they
   form linked lists, threaded through ids the writer already has at hand
   - prev_id links a workflow's states (newest first, from
   workflow.state_id) and, separately, its reuses (from
   workflow.last_reuse_id); prev_call_id links the calls a workflow made
   (from workflow.last_call_id). The only indexes are the primary keys,
   the idempotence keys, the running and the failed workflows, and the
   calls (first starts and reuses) the listing pages through.

   Writers run concurrently, so where SQLite relies on running one writer
   at a time, these rely on row locks. A write that moves a list head
   locks the row holding it, so it reads the head it links to and moves it
   atomically. Compare-and-set writes (finish!, time-out!, restart!) lock
   the workflow row, and Postgres re-checks their condition on its latest
   version. Rows are locked in id order, so writers can't deadlock.
   Inserts of one idempotence key are serialized by a transaction-scoped
   advisory lock on the key - in the database's advisory lock space,
   shared with the application's - so the second one sees the first's row
   (the check compares wf_def and params too, so a key collision costs a
   row read, never a wrong answer). Every write is a single statement - a
   plain one, or a call of one of the schema's functions - in a
   transaction of its own: one round trip, one commit, and concurrent
   commits share their WAL flushes.

   Reads follow those lists - and every other pointer, e.g. from a page
   of calls to their workflows - by primary key lookups the planner has
   no choice about (see by-id): what SQLite's CROSS JOINs and INDEXED BY
   pin down. Listings are built from a page of ids, found from a range of
   an index, and only then is each row looked up; the values the right
   index depends on - the state, wf_def and page size - are part of the
   SQL, so even a generic plan is made for them.

   Everything lives in one schema - `tenon` unless the JDBC URL names one
   with currentSchema, or the :schema option does - created by init-db!.
   Connections run at READ COMMITTED, which the writes rely on.

   Timestamps come from the database server's clock: the start of the
   statement, in UTC epoch milliseconds."
  (:require [next.jdbc.protocols :as p]
            [tenon.workflow.db :as db]
            [tenon.workflow.sqlite.jdbc :refer [int-at int-or-nil-at row-fn]])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
           [java.io Closeable]
           [java.sql PreparedStatement ResultSet Types]
           [java.util.concurrent ConcurrentHashMap]
           [javax.sql DataSource]))

(def ^:private states
  "State names by their code in the database."
  ["STARTED" "DONE" "ERROR" "TIMEOUT" "REUSED"])

(def ^:private state-code (zipmap states (range)))

(def ^:private placeholders
  "What sql fills in for each placeholder - in this order, so a value may
   use the placeholders after it. {{s}}, the schema, comes last."
  (concat
   [;; the columns of a workflow's current state (w and state-joins), as
    ;; read by read-state
    ["{{state-columns}}" "{{current-columns}}, f.parent_invocation_id"]
    ;; the columns of a call (c) and of the called workflow's current
    ;; state, as read by read-call
    ["{{call-columns}}" "{{current-columns}}, c.id AS call_id, c.occurred_at AS called_at,
                         c.parent_invocation_id AS caller_id, c.state AS call_state"]
    ;; the columns of a workflow's current state but its parent, as read
    ;; by current-row-fn
    ["{{current-columns}}" "w.invocation_id, d.name, f.params, w.state, s.occurred_at AS state_changed_at,
                            w.expires_at, i.data AS metadata,
                            CASE WHEN w.state = {{STARTED}} THEN NULL ELSE s.data END AS result"]
    ;; the joins resolving the workflow row aliased w to its current
    ;; state: f its first event (its identity), then current-joins
    ["{{state-joins}}" "JOIN {{s}}.event f ON f.id = w.id {{current-joins}}"]
    ;; the joins resolving the workflow row aliased w, and its first event
    ;; f, to its current state: s its state event, i its invocation's
    ;; (STARTED) event, d its wf_def
    ["{{current-joins}}" "JOIN {{s}}.event s ON s.id = w.state_id
                          JOIN {{s}}.event i ON i.id = w.invocation_id
                          JOIN {{s}}.wf_def d ON d.id = f.wf_def_id"]
    ;; the columns of an event (aliased e) the timeline's CTEs carry along
    ["{{event-carried}}" "e.id, e.workflow_id, e.state, e.occurred_at, e.data, e.invocation_id,
                          e.parent_invocation_id"]
    ;; the current UTC time in epoch milliseconds: the start of the
    ;; statement, so every use within one yields the same value
    ["{{now}}" "(extract(epoch FROM statement_timestamp()) * 1000)::bigint"]]
   ;; the states' codes: {{STARTED}}, {{DONE}}, ...
   (map-indexed (fn [code state] [(str "{{" state "}}") (str code)]) states)))

(defn- sql
  "The SQL of template for schema, its placeholders filled in (see
   placeholders)."
  ^String [^String schema ^String template]
  (.replace ^String (reduce (fn [^String s [^String k ^String v]] (.replace s k v)) template placeholders)
            "{{s}}" schema))

(def ^:private init-lock
  "The advisory lock init-db! holds while creating the schema."
  -6447303651960325383)

(def ^:private schema-ddl
  [;; top_level: called at the top level at least once
   "CREATE TABLE IF NOT EXISTS {{s}}.wf_def (
      id        integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
      top_level boolean NOT NULL,
      name      text COLLATE \"C\" NOT NULL UNIQUE
    )"
   ;; Ids are handed out in order (no per-connection CACHE), so a restart's
   ;; invocation id is larger than every id before it.
   "CREATE SEQUENCE IF NOT EXISTS {{s}}.event_id_seq AS bigint"
   ;; Calls - a workflow's first STARTED event and every REUSED event -
   ;; carry the called wf_def_id, which also tells them from the rest.
   ;; Fixed-width columns first, widest first, so none needs padding.
   "CREATE TABLE IF NOT EXISTS {{s}}.event (
      id                   bigint   PRIMARY KEY,
      workflow_id          bigint   NOT NULL,
      occurred_at          bigint   NOT NULL,
      key                  bigint,   -- the workflow's first event: idempotence key of (wf_def, params)
      invocation_id        bigint,   -- DONE, ERROR, TIMEOUT, REUSED: the invocation finished or reused
      expires_at           bigint,   -- STARTED: the lease it was started with
      parent_invocation_id bigint,   -- calls: the caller's invocation
      prev_id              bigint,   -- the workflow's previous state event, or for REUSED its previous reuse
      prev_call_id         bigint,   -- calls: the previous call made by the caller's workflow
      wf_def_id            integer,  -- calls: the called wf_def
      state                smallint NOT NULL CHECK (state BETWEEN {{STARTED}} AND {{REUSED}}), -- see states
      data                 text,     -- metadata (STARTED, REUSED) or result (DONE, ERROR, TIMEOUT)
      params               text,     -- the workflow's first event: its params
      CHECK ((invocation_id IS NULL) = (state = {{STARTED}})),
      CHECK (expires_at IS NULL OR state = {{STARTED}}),
      CHECK ((wf_def_id IS NULL) = (state <> {{REUSED}} AND id <> workflow_id)),
      CHECK ((key IS NULL) = (id <> workflow_id)),
      CHECK ((params IS NULL) = (id <> workflow_id))
    )"
   "CREATE INDEX IF NOT EXISTS event_key ON {{s}}.event (key) WHERE key IS NOT NULL"
   ;; The calls, in call order: top-level ones (the default listing) ...
   "CREATE INDEX IF NOT EXISTS event_top_call ON {{s}}.event (id)
      WHERE wf_def_id IS NOT NULL AND parent_invocation_id IS NULL"
   ;; ... and by wf_def, top-level and nested ones apart: the listing's
   ;; wf_def filter is one range, or two merged.
   "CREATE INDEX IF NOT EXISTS event_def_top_call ON {{s}}.event (wf_def_id, id)
      WHERE wf_def_id IS NOT NULL AND parent_invocation_id IS NULL"
   "CREATE INDEX IF NOT EXISTS event_def_nested_call ON {{s}}.event (wf_def_id, id)
      WHERE wf_def_id IS NOT NULL AND parent_invocation_id IS NOT NULL"
   "CREATE TABLE IF NOT EXISTS {{s}}.workflow (
      id            bigint   PRIMARY KEY,  -- its first invocation id (and event)
      invocation_id bigint   NOT NULL,     -- the current invocation (its STARTED event)
      state_id      bigint   NOT NULL,     -- the current state's event: invocation_id while STARTED
      expires_at    bigint,                -- while STARTED: the lease
      last_reuse_id bigint,                -- its latest REUSED event, head of their prev_id list
      last_call_id  bigint,                -- the latest call it made, head of the prev_call_id list
      state         smallint NOT NULL      -- the current state's code
    ) WITH (fillfactor = 75)"
   ;; The few workflows that are not DONE - the running ones and the failed
   ;; ones - by state, then id.
   ;; The few workflows not DONE: the running ones ...
   "CREATE INDEX IF NOT EXISTS workflow_started ON {{s}}.workflow (id) WHERE state = {{STARTED}}"
   ;; ... and the failed ones, by state. Apart, as every workflow passes
   ;; through STARTED, briefly: that index takes up the space of the most
   ;; workflows ever running at once (and finished since the last vacuum),
   ;; and looks sparse to the planner - this one stays dense.
   "CREATE INDEX IF NOT EXISTS workflow_failed ON {{s}}.workflow (state, id)
      WHERE state IN ({{ERROR}}, {{TIMEOUT}})"
   ;; insert-workflow!: the key's advisory lock makes the check and the
   ;; insert atomic. The wf_def is usually there already: then it is
   ;; looked up, not upserted (an upsert would lock its row, serializing
   ;; every insert of the wf_def).
   "CREATE OR REPLACE FUNCTION {{s}}.insert_workflow(p_key bigint, p_wf_def text, p_params text, p_parent bigint,
                                                     p_metadata text, p_timeout bigint)
    RETURNS bigint LANGUAGE plpgsql AS $$
    DECLARE
      v_now       bigint := {{now}};
      v_def       integer;
      v_top       boolean;
      v_parent_wf bigint;
      v_prev_call bigint;
      v_id        bigint;
    BEGIN
      PERFORM pg_advisory_xact_lock(p_key);
      PERFORM FROM {{s}}.event f JOIN {{s}}.wf_def d ON d.id = f.wf_def_id
       WHERE f.key = p_key AND f.params = p_params AND d.name = p_wf_def;
      IF FOUND THEN
        RETURN NULL;
      END IF;
      SELECT id, top_level INTO v_def, v_top FROM {{s}}.wf_def WHERE name = p_wf_def;
      IF NOT FOUND THEN
        INSERT INTO {{s}}.wf_def (name, top_level) VALUES (p_wf_def, p_parent IS NULL)
          ON CONFLICT (name) DO NOTHING
          RETURNING id, top_level INTO v_def, v_top;
        IF NOT FOUND THEN
          SELECT id, top_level INTO v_def, v_top FROM {{s}}.wf_def WHERE name = p_wf_def;
        END IF;
      END IF;
      IF p_parent IS NULL AND NOT v_top THEN
        UPDATE {{s}}.wf_def SET top_level = true WHERE id = v_def AND NOT top_level;
      END IF;
      IF p_parent IS NOT NULL THEN
        SELECT w.id, w.last_call_id INTO v_parent_wf, v_prev_call
          FROM {{s}}.event p JOIN {{s}}.workflow w ON w.id = p.workflow_id
         WHERE p.id = p_parent
           FOR UPDATE OF w;
      END IF;
      v_id := nextval('{{s}}.event_id_seq');
      INSERT INTO {{s}}.event (id, workflow_id, state, occurred_at, data, key, params, expires_at,
                               parent_invocation_id, wf_def_id, prev_call_id)
      VALUES (v_id, v_id, {{STARTED}}, v_now, p_metadata, p_key, p_params, v_now + p_timeout,
              p_parent, v_def, v_prev_call);
      INSERT INTO {{s}}.workflow (id, invocation_id, state, state_id, expires_at)
      VALUES (v_id, v_id, {{STARTED}}, v_id, v_now + p_timeout);
      IF v_parent_wf IS NOT NULL THEN
        UPDATE {{s}}.workflow SET last_call_id = v_id WHERE id = v_parent_wf;
      END IF;
      RETURN v_id;
    END
    $$"
   ;; restart!: locking the row re-checks that the invocation is still the
   ;; current one, and finished.
   "CREATE OR REPLACE FUNCTION {{s}}.restart_workflow(p_invocation bigint, p_metadata text, p_timeout bigint)
    RETURNS bigint LANGUAGE plpgsql AS $$
    DECLARE
      v_now  bigint := {{now}};
      v_wf   bigint;
      v_prev bigint;
      v_id   bigint;
    BEGIN
      SELECT w.id, w.state_id INTO v_wf, v_prev
        FROM {{s}}.event i JOIN {{s}}.workflow w ON w.id = i.workflow_id
       WHERE i.id = p_invocation AND w.invocation_id = i.id AND w.state <> {{STARTED}}
         FOR UPDATE OF w;
      IF NOT FOUND THEN
        RETURN NULL;
      END IF;
      v_id := nextval('{{s}}.event_id_seq');
      INSERT INTO {{s}}.event (id, workflow_id, state, occurred_at, data, expires_at, prev_id)
      VALUES (v_id, v_wf, {{STARTED}}, v_now, p_metadata, v_now + p_timeout, v_prev);
      UPDATE {{s}}.workflow
         SET invocation_id = v_id, state = {{STARTED}}, state_id = v_id, expires_at = v_now + p_timeout
       WHERE id = v_wf;
      RETURN v_id;
    END
    $$"
   ;; record-reuse!: moves the heads of two lists - the reused workflow's
   ;; reuses and the caller's calls - so it locks both rows, in id order.
   "CREATE OR REPLACE FUNCTION {{s}}.record_reuse(p_invocation bigint, p_parent bigint, p_metadata text)
    RETURNS bigint LANGUAGE plpgsql AS $$
    DECLARE
      v_wf        bigint;
      v_def       integer;
      v_parent_wf bigint;
      v_prev      bigint;
      v_prev_call bigint;
      v_id        bigint;
      r           record;
    BEGIN
      SELECT f.id, f.wf_def_id INTO v_wf, v_def
        FROM {{s}}.event i JOIN {{s}}.event f ON f.id = i.workflow_id
       WHERE i.id = p_invocation AND i.state = {{STARTED}};
      IF NOT FOUND THEN
        RETURN NULL;
      END IF;
      SELECT p.workflow_id INTO v_parent_wf FROM {{s}}.event p WHERE p.id = p_parent;
      FOR r IN SELECT id, last_reuse_id, last_call_id FROM {{s}}.workflow
                WHERE id IN (v_wf, v_parent_wf) ORDER BY id FOR UPDATE
      LOOP
        IF r.id = v_wf THEN
          v_prev := r.last_reuse_id;
        END IF;
        IF r.id = v_parent_wf THEN
          v_prev_call := r.last_call_id;
        END IF;
      END LOOP;
      v_id := nextval('{{s}}.event_id_seq');
      INSERT INTO {{s}}.event (id, workflow_id, state, occurred_at, data, invocation_id, parent_invocation_id,
                               wf_def_id, prev_id, prev_call_id)
      VALUES (v_id, v_wf, {{REUSED}}, {{now}}, p_metadata, p_invocation, p_parent, v_def, v_prev, v_prev_call);
      UPDATE {{s}}.workflow SET last_reuse_id = v_id WHERE id = v_wf;
      IF v_parent_wf IS NOT NULL THEN
        UPDATE {{s}}.workflow SET last_call_id = v_id WHERE id = v_parent_wf;
      END IF;
      IF p_parent IS NULL THEN
        UPDATE {{s}}.wf_def SET top_level = true WHERE id = v_def AND NOT top_level;
      END IF;
      RETURN v_id;
    END
    $$"])

;; Reading rows

(defn- text-at ^Object [^ResultSet r ^long i]
  (.getString r i))

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

(def ^:private read-call
  "Rows of call-columns."
  (current-row-fn :event/call-id (int-at %)
                  :event/created-at (int-at %)
                  :workflow/parent-invocation-id (int-or-nil-at %)
                  :event/reused (= "REUSED" (state-at %))))

(def ^:private read-get-workflow
  (current-row-fn :workflow/parent-invocation-id (int-or-nil-at %)
                  :workflow/expired (.getBoolean %)))

(def ^:private read-expired
  (current-row-fn :workflow/parent-invocation-id (int-or-nil-at %)
                  :workflow/expired Boolean/TRUE))

(def ^:private read-timeline
  (row-fn :workflow/invocation-id (int-at %)
          :workflow/current-invocation-id (int-at %)
          :workflow/parent-invocation-id (int-or-nil-at %)
          :workflow/name (text-at %)
          :workflow/state (state-at %)
          :workflow/state-changed-at (int-at %)
          :event/data (text-at %)
          :workflow/depth (int-at %)))

(defn- read-id [^ResultSet r]
  (.getLong r 1))

(defn- read-nilable-id [^ResultSet r]
  (let [v (.getLong r 1)]
    (when-not (.wasNull r) v)))

;; Running statements

(defn- bind!
  "Sets the parameters of ps to params. Each statement casts its
   parameters to their types, and NULLs are sent typed too - as bigints
   here, text NULLs by bind-text!: pgjdbc prepares a statement anew
   whenever the types of its parameters change."
  [^PreparedStatement ps params]
  (loop [i 1 params (seq params)]
    (when params
      (let [v (first params)]
        (cond
          (nil? v) (.setNull ps i Types/BIGINT)
          (instance? Long v) (.setLong ps i v)
          (string? v) (.setString ps i v)
          (boolean? v) (.setBoolean ps i v)
          :else (.setObject ps i v)))
      (recur (unchecked-inc i) (next params)))))

(defn- bind-text!
  "Sets parameter i of ps to the text v (nil: NULL)."
  [^PreparedStatement ps i v]
  (if (nil? v) (.setNull ps (int i) Types/VARCHAR) (.setString ps (int i) v)))

(defn- query
  "Runs sql with params on a connection of ds, and returns the vector of
   (row-fn result-set) for every row of the result. text-params: the
   indexes (1-based) of the params that are text, so a nil one is a NULL
   of text."
  ([ds sql params row-fn] (query ds sql params #{} row-fn))
  ([^DataSource ds ^String sql params text-params row-fn]
   (with-open [c (.getConnection ds)
               ps (.prepareStatement c sql)]
     (bind! ps params)
     (doseq [i text-params] (bind-text! ps i (nth params (dec i))))
     (with-open [r (.executeQuery ps)]
       (loop [rows (transient [])]
         (if (.next r)
           (recur (conj! rows (row-fn r)))
           (persistent! rows)))))))

;; Following pointers

(defn- by-id
  "SQL joining the row of table whose id is expr, as alias: a primary key
   lookup per row. Written as a lateral subquery with a LIMIT, which the
   planner can't flatten into a join - one it might otherwise run as a
   scan of the whole table, hashed: it estimates the size of a recursive
   CTE, or of a page of ids, generously."
  [table alias expr]
  (str "CROSS JOIN LATERAL (SELECT * FROM {{s}}." table " WHERE id = " expr " LIMIT 1) " alias))

(def ^:private call-details
  "SQL joining the call whose event id is x.id, and the called workflow's
   current state, as the call-columns of d."
  "CROSS JOIN LATERAL (SELECT {{call-columns}}
                        FROM {{s}}.event c
                        JOIN {{s}}.workflow w ON w.id = c.workflow_id {{state-joins}}
                       WHERE c.id = x.id
                       LIMIT 1) d")

(def ^:private state-details
  "SQL joining the current state of the workflow whose id is x.id, as the
   state-columns of d."
  "CROSS JOIN LATERAL (SELECT {{state-columns}}
                        FROM {{s}}.workflow w {{state-joins}}
                       WHERE w.id = x.id
                       LIMIT 1) d")

;; Listing calls

(defn- calls-sql
  "A fn building the SQL, and the params, for top-level-workflows. The
   page of call ids is found first, then each call's details. Calls are
   found
   - STARTED, ERROR or TIMEOUT: through the few workflows in that state
     (workflow_started, workflow_failed) - each one's first call, and its
     reuses from its own event list;
   - a wf_def: from its range of event_def_top_call, and for nested calls
     too of event_def_nested_call, merged;
   - else: from event_top_call for top-level calls only, else walking the
     log newest first."
  [s {:keys [state wf-def-id top-level-only? limit before] :or {top-level-only? true}}]
  (let [limit (when limit (inc (long limit)))
        ;; inlined, like the limit: few values
        code (when state (get state-code state -1))
        and-all (fn ^String [& conditions] (apply str (interleave (repeat " AND ") (remove nil? conditions))))
        ;; The limit is part of the SQL, not a parameter: so the planner
        ;; knows it in a generic plan too, rather than guessing a tenth of
        ;; the rows - and planning for that many.
        ^String limit-sql (if limit (str " LIMIT " limit) "")
        ;; inlined too, so that the planner knows how many calls of the
        ;; wf_def there are: rare ones are better found from their own range
        ;; of event_def_top_call than by walking all calls
        def-filter (str "c.wf_def_id = " wf-def-id)
        page (fn ^String [^String ids]
               (str "SELECT d.* FROM (" ids ") x " call-details " ORDER BY x.id DESC"))]
    (cond
      (#{"STARTED" "ERROR" "TIMEOUT"} state)
      ;; A first call of a workflow in the state is in its index's order,
      ;; so a page of them is a range of it. Its reuses are in its
      ;; reuse list, newest first - and only those newer than the page's
      ;; oldest first call can make it onto the page, so each list is cut
      ;; there.
      (let [filters (fn ^String [^String id]
                      ^String (and-all (when top-level-only? "c.parent_invocation_id IS NULL")
                                       (when wf-def-id def-filter)
                                       (when before (.replace "{{id}} < ?::bigint" "{{id}}" id))))
            filter-params (cond-> [] before (conj before))]
        [#(sql s (-> (page "WITH RECURSIVE
                              firsts(id) AS (
                                SELECT r.id FROM {{s}}.workflow r{{first-join}}
                                 WHERE r.state = {{state}}{{first-filters}}
                                 ORDER BY r.id DESC{{limit}}),
                              bound(id) AS ({{bound}}),
                              reuses(id) AS (
                                SELECT w.last_reuse_id FROM {{s}}.workflow w
                                 WHERE w.state = {{state}} AND w.last_reuse_id >= (SELECT id FROM bound)
                                UNION ALL
                                SELECT e.prev_id FROM reuses r {{event-e}}
                                 WHERE e.prev_id >= (SELECT id FROM bound))
                            SELECT id FROM firsts
                            UNION ALL
                            SELECT r.id FROM reuses r {{event-c}}
                             WHERE c.state = {{REUSED}}{{reuse-filters}}
                            ORDER BY id DESC{{limit}}")
                     (.replace "{{first-join}}" ^String (if (or top-level-only? wf-def-id) (str " " (by-id "event" "c" "r.id")) ""))
                     (.replace "{{event-e}}" ^String (by-id "event" "e" "r.id"))
                     (.replace "{{event-c}}" ^String (by-id "event" "c" "r.id"))
                     (.replace "{{state}}" ^String (str code))
                     (.replace "{{first-filters}}" ^String (filters "r.id"))
                     (.replace "{{reuse-filters}}" ^String (filters "r.id"))
                     (.replace "{{bound}}" ^String (if limit
                                                     (str "SELECT CASE WHEN count(*) < " limit " THEN 0 ELSE min(id) END FROM firsts")
                                                     "SELECT 0::bigint"))
                     (.replace "{{limit}}" limit-sql)))
         (into filter-params filter-params)])

      wf-def-id
      ;; From the wf_def's range of its index, however few its calls at
      ;; that level - which the planner can't tell from the statistics of
      ;; wf_def_id: so the rows are ordered by the index's key, which only
      ;; it has them in, and the wf_def is a range of it rather than an
      ;; equality, which would make ordering by it a no-op.
      (let [ids (fn ^String [top-level?]
                  (-> "(SELECT c.id FROM {{s}}.event c{{workflow}}
                         WHERE c.wf_def_id BETWEEN {{def}} AND {{def}}
                           AND c.parent_invocation_id {{top-level}}{{filters}}
                         ORDER BY c.wf_def_id DESC, c.id DESC{{limit}})"
                      (.replace "{{workflow}}" ^String (if state (str " " (by-id "workflow" "w" "c.workflow_id")) ""))
                      (.replace "{{def}}" (str wf-def-id))
                      (.replace "{{top-level}}" (if top-level? "IS NULL" "IS NOT NULL"))
                      (.replace "{{filters}}" ^String (and-all nil
                                                               (when state (str "w.state = " code))
                                                               (when before "c.id < ?::bigint")))
                      (.replace "{{limit}}" limit-sql)))
            params (cond-> [] before (conj before))]
        (if top-level-only?
          [#(sql s (page (ids true))) params]
          [#(sql s (page (str "SELECT id FROM (" (ids true) " UNION ALL " (ids false) ") u ORDER BY id DESC" limit-sql)))
           (into params params)]))

      :else
      [#(sql s (page (-> "SELECT c.id FROM {{s}}.event c{{workflow}}
                           WHERE c.wf_def_id IS NOT NULL{{filters}}
                           ORDER BY c.id DESC{{limit}}"
                         (.replace "{{workflow}}" ^String (if state (str " " (by-id "workflow" "w" "c.workflow_id")) ""))
                         (.replace "{{filters}}" ^String (and-all nil
                                                                  (when top-level-only? "c.parent_invocation_id IS NULL")
                                                                  (when state (str "w.state = " code))
                                                                  (when before "c.id < ?::bigint")))
                         (.replace "{{limit}}" limit-sql))))
       (cond-> [] before (conj before))])))

(defn- calls-query
  "SQL and params for top-level-workflows - the SQL built once per shape
   of filters (which of them are given, the state, wf_def id and the
   limit), and cached in cache. filters has the :wf-def-id of the
   :wf-def."
  [s ^ConcurrentHashMap cache {:keys [state wf-def-id top-level-only? limit before] :or {top-level-only? true} :as filters}]
  (let [[sql params] (calls-sql s filters)
        shape [state wf-def-id (boolean top-level-only?) (some-> limit long) (some? before)]]
    [(or (.get cache shape)
         (let [sql (sql)]
           ;; bounded, should callers page by ever new limits, or have
           ;; very many wf_defs
           (when (< (.size cache) 256) (.put cache shape sql))
           sql))
     params]))

;; The statements

(def ^:private statements
  {:insert "SELECT {{s}}.insert_workflow(?::bigint, ?::text, ?::text, ?::bigint, ?::text, ?::bigint)"

   :finish
   ;; Compare-and-set on the workflow row: Postgres re-checks the WHERE on
   ;; its latest version once it has the row lock, so a concurrent finish,
   ;; time-out or restart makes this one update nothing.
   "WITH w AS (
      UPDATE {{s}}.workflow w
         SET state = ?::smallint, state_id = nextval('{{s}}.event_id_seq'), expires_at = NULL
        FROM {{s}}.event i
       WHERE i.id = ?::bigint AND w.id = i.workflow_id AND w.state_id = i.id
         AND (?::boolean OR w.expires_at <= {{now}})
      RETURNING w.id, w.state_id, w.invocation_id, w.state)
    INSERT INTO {{s}}.event (id, workflow_id, state, occurred_at, data, invocation_id, prev_id)
    SELECT state_id, id, state, {{now}}, ?::text, invocation_id, invocation_id FROM w
    RETURNING id"

   :restart "SELECT {{s}}.restart_workflow(?::bigint, ?::text, ?::bigint)"

   :reuse "SELECT {{s}}.record_reuse(?::bigint, ?::bigint, ?::text)"

   :get-workflow
   "SELECT {{state-columns}}, coalesce(w.expires_at <= {{now}}, false)
      FROM {{s}}.event x
      JOIN {{s}}.workflow w ON w.id = x.workflow_id {{state-joins}}
     WHERE x.id = ?::bigint AND x.state = {{STARTED}}"

   :find
   "SELECT {{state-columns}}
      FROM {{s}}.event f
      JOIN {{s}}.workflow w ON w.id = f.id {{current-joins}}
     WHERE f.key = ?::bigint"

   :wf-defs "SELECT name FROM {{s}}.wf_def WHERE top_level OR NOT ?::boolean ORDER BY name"

   :wf-def-id "SELECT id FROM {{s}}.wf_def WHERE name = ?::text"

   :timeline
   ;; As in SQLite, but a recursive term may refer to its CTE only once:
   ;; a call's two successors - the caller's previous call, and for a
   ;; first start the first call the started workflow made - come from one
   ;; lateral subquery.
   (str
    "WITH RECURSIVE
     root(id) AS (SELECT workflow_id FROM {{s}}.event WHERE id = ?::bigint AND state = {{STARTED}}),
     calls(id, workflow_id, state, occurred_at, data, invocation_id, parent_invocation_id, depth, prev) AS (
       SELECT {{event-carried}}, 1, e.prev_call_id
         FROM root " (by-id "workflow" "w" "root.id") " " (by-id "event" "e" "w.last_call_id") "
       UNION ALL
       SELECT n.*
         FROM calls c
         CROSS JOIN LATERAL (
           SELECT {{event-carried}}, c.depth, e.prev_call_id
             FROM {{s}}.event e WHERE e.id = c.prev
           UNION ALL
           SELECT {{event-carried}}, c.depth + 1, e.prev_call_id
             FROM {{s}}.workflow w JOIN {{s}}.event e ON e.id = w.last_call_id
            WHERE w.id = c.workflow_id AND c.state = {{STARTED}}) n
     ),
     tree(id, depth) AS (
       SELECT id, 0 FROM root
       UNION ALL
       SELECT workflow_id, depth FROM calls WHERE state = {{STARTED}}
     ),
     own(id, workflow_id, state, occurred_at, data, invocation_id, parent_invocation_id, depth, prev) AS (
       SELECT {{event-carried}}, t.depth, e.prev_id
         FROM tree t " (by-id "workflow" "w" "t.id") " " (by-id "event" "e" "w.state_id") "
       UNION ALL
       SELECT {{event-carried}}, 0, e.prev_id
         FROM root " (by-id "workflow" "w" "root.id") " " (by-id "event" "e" "w.last_reuse_id") "
       UNION ALL
       SELECT {{event-carried}}, o.depth, e.prev_id
         FROM own o " (by-id "event" "e" "o.prev") "
     ),
     entry AS (
       SELECT * FROM own
        WHERE state <> {{REUSED}}
           OR coalesce((SELECT workflow_id FROM {{s}}.event WHERE id = own.parent_invocation_id), -1)
              NOT IN (SELECT id FROM tree)
       UNION ALL
       SELECT * FROM calls WHERE state = {{REUSED}}
     )
     SELECT coalesce(x.invocation_id, x.id), w.invocation_id,
            CASE WHEN x.state = {{REUSED}} THEN x.parent_invocation_id ELSE f.parent_invocation_id END,
            d.name, x.state, x.occurred_at, x.data, x.depth
       FROM entry x " (by-id "workflow" "w" "x.workflow_id") " " (by-id "event" "f" "x.workflow_id") "
       " (by-id "wf_def" "d" "f.wf_def_id") "
      ORDER BY x.id")

   :extend-lease
   ;; The rows are locked in id order first (see the ns doc), each still
   ;; the current, leased invocation of its workflow once locked.
   (str
    "UPDATE {{s}}.workflow w SET expires_at = {{now}} + ?::bigint
       FROM (SELECT w.id
               FROM unnest(?::bigint[]) AS a(id) " (by-id "event" "e" "a.id") "
               JOIN {{s}}.workflow w ON w.id = e.workflow_id
              WHERE w.invocation_id = e.id AND w.state = {{STARTED}} AND w.expires_at IS NOT NULL
              ORDER BY w.id
                FOR UPDATE OF w) l
      WHERE w.id = l.id
     RETURNING w.invocation_id")

   :list-expired
   (str
    "SELECT d.*
       FROM (SELECT id FROM {{s}}.workflow WHERE state = {{STARTED}} AND expires_at <= {{now}}) x
       " state-details "
      ORDER BY x.id")})

(defn- init-schema!
  "Creates the schema in one transaction, holding an advisory lock: of
   several processes starting at once on a new database one does the
   work, and the others, having waited for the lock, find it done."
  [^DataSource ds s]
  (with-open [c (.getConnection ds)]
    (.setAutoCommit c false)
    (try
      (with-open [st (.createStatement c)]
        (.execute st (str "SELECT pg_advisory_xact_lock(" init-lock ")"))
        ;; CREATE SCHEMA IF NOT EXISTS needs the CREATE privilege on the
        ;; database even when the schema exists - which it may well, set
        ;; up by someone who has it.
        (when-not (with-open [r (.executeQuery st (str "SELECT FROM pg_namespace WHERE nspname = '" s "'"))]
                    (.next r))
          (.execute st (str "CREATE SCHEMA " s)))
        (doseq [ddl schema-ddl]
          (.execute st (sql s ddl))))
      (.commit c)
      (catch Throwable e
        (try (.rollback c) (catch Throwable _))
        (throw e))
      (finally
        (.setAutoCommit c true)))))

(defn- finish! [ds sqls invocation-id state result-edn-str expired-only?]
  (boolean (seq (query ds (:finish sqls) [(state-code state) invocation-id (not expired-only?) result-edn-str] #{4}
                       read-id))))

(defn- idempotence-key
  "The 64-bit idempotence key of (wf-def, params) - see
   tenon.workflow.db/idempotence-key. A var of its own, so tests can make
   keys collide."
  ^long [^String wf-def ^String params]
  (db/idempotence-key wf-def params))

(deftype PostgresStorage [^DataSource ds ^String schema sqls ^ConcurrentHashMap calls-cache
                          ^ConcurrentHashMap wf-def-ids owns-ds?]
  Closeable
  (close [_]
    (when owns-ds? (.close ^Closeable ds)))

  ;; next.jdbc can run statements on it, each on a connection of its pool
  ;; (whose search_path is the schema if the URL set currentSchema)
  p/Executable
  (-execute [_ sql-params opts] (p/-execute ds sql-params opts))
  (-execute-one [_ sql-params opts] (p/-execute-one ds sql-params opts))
  (-execute-all [_ sql-params opts] (p/-execute-all ds sql-params opts))

  p/Connectable
  (get-connection [_ opts] (p/get-connection ds opts)))

(defn- ds-of ^DataSource [^PostgresStorage s] (.-ds s))

(defn- sqls-of [^PostgresStorage s] (.-sqls s))

(defn- wf-def-id
  "The id of the wf_def named wf-def, or nil if there is none (yet). Ids
   never change, so they are cached once found."
  [^PostgresStorage s wf-def]
  (let [^ConcurrentHashMap ids (.-wf-def-ids s)]
    (or (.get ids wf-def)
        (when-let [id (first (query (ds-of s) (:wf-def-id (sqls-of s)) [wf-def] read-id))]
          (.put ids wf-def id)
          id))))

;; The protocols are extended to the type rather than implemented inline,
;; so that calls go through their vars - which with-redefs can then
;; replace, as with SQLite.
(extend-type PostgresStorage
  db/Storage
  (init-db! [this]
    (init-schema! (ds-of this) (.-schema this))
    this)

  (insert-workflow! [this wf-def params-edn-str parent-invocation-id metadata-edn-str timeout-ms]
    (first (query (ds-of this) (:insert (sqls-of this))
                  [(idempotence-key wf-def params-edn-str) wf-def params-edn-str parent-invocation-id
                   metadata-edn-str timeout-ms]
                  #{5}
                  read-nilable-id)))

  (finish! [this invocation-id state result-edn-str]
    (finish! (ds-of this) (sqls-of this) invocation-id state result-edn-str false))

  (time-out! [this invocation-id result-edn-str]
    (finish! (ds-of this) (sqls-of this) invocation-id "TIMEOUT" result-edn-str true))

  (restart! [this invocation-id metadata-edn-str timeout-ms]
    (first (query (ds-of this) (:restart (sqls-of this)) [invocation-id metadata-edn-str timeout-ms] #{2}
                  read-nilable-id)))

  (record-reuse! [this invocation-id parent-invocation-id metadata-edn-str]
    (query (ds-of this) (:reuse (sqls-of this)) [invocation-id parent-invocation-id metadata-edn-str] #{3}
           read-nilable-id)
    nil)

  (get-workflow [this invocation-id]
    (first (query (ds-of this) (:get-workflow (sqls-of this)) [invocation-id] read-get-workflow)))

  (find-by-wf-def-and-params [this wf-def params-edn-str]
    ;; The rows with the key - all but never more than one - are told apart
    ;; by their name and params here, as in SQLite.
    (some #(when (and (= params-edn-str (:workflow/params %)) (= wf-def (:workflow/name %))) %)
          (query (ds-of this) (:find (sqls-of this)) [(idempotence-key wf-def params-edn-str)] read-state)))

  (top-level-workflows
    ([this]
     (db/top-level-workflows this nil))
    ([this {:keys [wf-def] :as filters}]
     (let [def-id (some->> wf-def (wf-def-id this))]
       (if (and wf-def (nil? def-id))
         [] ; no such wf_def, no calls of it
         (let [[sql params] (calls-query (.-schema this) (.-calls-cache this) (assoc filters :wf-def-id def-id))]
           (query (ds-of this) sql params read-call))))))

  (top-level-wf-defs [this top-level-only?]
    (query (ds-of this) (:wf-defs (sqls-of this)) [(boolean top-level-only?)] #(text-at % 1)))

  (full-timeline [this invocation-id]
    (query (ds-of this) (:timeline (sqls-of this)) [invocation-id] read-timeline))

  db/Timeouts
  (extend-lease! [this invocation-ids timeout-ms]
    (if (empty? invocation-ids)
      #{}
      (into #{} (query (ds-of this) (:extend-lease (sqls-of this))
                       [timeout-ms (long-array (map long invocation-ids))]
                       read-id))))

  (list-expired [this]
    (query (ds-of this) (:list-expired (sqls-of this)) [] read-expired)))

(defn- url-schema
  "The first schema of the currentSchema parameter of a JDBC URL, if any."
  [url]
  (some-> (re-find #"[?&]currentSchema=([^&,]+)" url) second))

(defn- pool
  "A HikariCP pool of size connections to url."
  ^HikariDataSource [url size]
  (HikariDataSource.
   (doto (HikariConfig.)
     (.setJdbcUrl url)
     (.setMaximumPoolSize size)
     (.setAutoCommit true)
     (.setTransactionIsolation "TRANSACTION_READ_COMMITTED")
     (.setPoolName "tenon"))))

(defn storage
  "A Storage (and db/Timeouts) on PostgreSQL - just the connections, no
   schema: pass it to tenon.workflow.db/init-db! to also ensure the schema
   exists. db is either a JDBC URL (jdbc:postgresql://...), for which it
   opens a HikariCP pool of :pool-size connections (10) - closed when the
   storage is closed - or a javax.sql.DataSource the caller manages (its
   connections must run at READ COMMITTED, the default, and auto-commit).

   Its tables live in the schema :schema, else the one the URL's
   currentSchema parameter names, else \"tenon\" - a lowercase SQL
   identifier.

   The storage is Closeable: close it (e.g. with-open or .close) once it's
   no longer needed. next.jdbc can run statements on it too."
  (^PostgresStorage [db] (storage db {}))
  (^PostgresStorage [db {:keys [schema pool-size] :or {pool-size 10}}]
   (let [owns-ds? (string? db)
         schema (or schema (when owns-ds? (url-schema db)) "tenon")]
     (when-not (re-matches #"[a-z_][a-z0-9_]{0,62}" schema)
       (throw (IllegalArgumentException. (str "not a lowercase SQL identifier: " (pr-str schema)))))
     (PostgresStorage. (if owns-ds? (pool db pool-size) db)
                       schema
                       (update-vals statements #(sql schema %))
                       (ConcurrentHashMap.)
                       (ConcurrentHashMap.)
                       owns-ds?))))
