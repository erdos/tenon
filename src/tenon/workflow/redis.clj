(ns tenon.workflow.redis
  "The Redis-backed storage implementation: db/Storage and db/Timeouts over
   taoensso/carmine, with the same observable behaviour as the SQLite one.

   The data model is the SQLite one, with hashes for tables:

   - {p}:seq: INCR counter handing out event ids - the global order, shared
     by new workflows, restarts and every other event, as the rowid is.
   - {p}:ev:<id>: hash, one event. Fields as the columns of the event table
     (workflow_id, state, occurred_at, data, invocation_id, expires_at,
     parent_invocation_id, wf_def, prev_id, prev_call_id, params); a NULL
     is an absent field.
   - {p}:wf:<id>: hash, the pointers of the workflow whose first event is
     <id> (invocation_id, state, state_id, last_reuse_id, last_call_id).
   - {p}:idem: hash, SHA-256 hex of (wf_def, params) -> workflow id.
   - {p}:defs, {p}:defs:top: sets of wf_def names - all, and those called
     at the top level at least once.
   - What SQLite finds with indexes, here are sorted sets scored by id:
     {p}:calls, {p}:calls:top, {p}:calls:def:<wf_def> and
     {p}:calls:deftop:<wf_def> list the calls (a workflow's first event and
     its REUSED events) newest first, and {p}:state:STARTED/ERROR/TIMEOUT
     the workflows that are in the state. {p}:leases is scored by lease
     end: the STARTED workflows that have a lease.

   Every operation is one Lua script, which Redis runs without interleaving
   anything else: the compare-and-set checks of a write and its writes are
   atomic, as is a read's snapshot - what the single SQL statements, and
   SQLite's single writer, are in the other implementation. The clock is
   Redis' (TIME), so that timestamps never mix in a second one. {p} is the
   prefix in braces, a Redis Cluster hash tag: all keys share a slot.

   What SQLite adds, and Redis only has if told to: durability. A commit
   there is fsynced (synchronous FULL). Run Redis with appendonly yes and
   appendfsync always for the same guarantee - and noeviction, as an
   evicted key is a lost workflow."
  (:require [clojure.string :as str]
            [taoensso.carmine :as car]
            [taoensso.carmine.connections :as conn]
            [tenon.workflow.db :as db])
  (:import [java.io Closeable]
           [java.security MessageDigest]))

;;; Lua

(def ^:private prelude
  "local P = ARGV[1]
   local SEQ = KEYS[1]
   local function ek(id) return P .. ':ev:' .. id end
   local function wk(id) return P .. ':wf:' .. id end
   -- optional arguments travel as '-' (nil) or '+' followed by the value
   local function opt(a)
     if string.sub(a, 1, 1) == '+' then return string.sub(a, 2) end
     return nil
   end
   local function now()
     local t = redis.call('TIME')
     return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
   end
   local function fmt(n) return string.format('%d', n) end
   local function next_id() return fmt(redis.call('INCR', SEQ)) end
   local function put(f, k, v)
     if v then f[#f + 1] = k; f[#f + 1] = v end
   end
   local function add_call(wf_def, id, top)
     redis.call('ZADD', P .. ':calls', id, id)
     redis.call('ZADD', P .. ':calls:def:' .. wf_def, id, id)
     if top then
       redis.call('ZADD', P .. ':calls:top', id, id)
       redis.call('ZADD', P .. ':calls:deftop:' .. wf_def, id, id)
       redis.call('SADD', P .. ':defs:top', wf_def)
     end
   end
   -- The workflow_state row of a workflow: the pointers resolved, nil as false.
   local function state_row(wid)
     local w = redis.call('HMGET', wk(wid), 'invocation_id', 'state', 'state_id')
     local f = redis.call('HMGET', ek(wid), 'wf_def', 'params', 'parent_invocation_id')
     local s = redis.call('HMGET', ek(w[3]), 'occurred_at', 'expires_at', 'data')
     local m = redis.call('HGET', ek(w[1]), 'data')
     local result = false
     if w[2] ~= 'STARTED' then result = s[3] end
     return {w[1], f[1], f[2], f[3], w[2], s[1], s[2], m, result}
   end
   -- The last call made by the workflow of the event parent, or false;
   -- and the workflow itself.
   local function last_call_of(parent)
     if not parent then return false, false end
     local pw = redis.call('HGET', ek(parent), 'workflow_id')
     if not pw then return false, false end
     return redis.call('HGET', wk(pw), 'last_call_id'), pw
   end
   -- Records the end of the lease of wid, or that it has none.
   local function set_lease(wid, expires)
     if expires then
       redis.call('ZADD', P .. ':leases', expires, wid)
     else
       redis.call('ZREM', P .. ':leases', wid)
     end
   end
   -- Moves wid between the state indexes.
   local function index_state(wid, state)
     redis.call('ZREM', P .. ':state:STARTED', wid)
     redis.call('ZREM', P .. ':state:ERROR', wid)
     redis.call('ZREM', P .. ':state:TIMEOUT', wid)
     if state ~= 'DONE' then redis.call('ZADD', P .. ':state:' .. state, wid, wid) end
   end
   ")

(def ^:private insert-lua
  "local key, wf_def, params = ARGV[2], ARGV[3], ARGV[4]
   local parent, metadata, timeout = opt(ARGV[5]), opt(ARGV[6]), opt(ARGV[7])
   if redis.call('HEXISTS', P .. ':idem', key) == 1 then return false end
   local t = now()
   local id = next_id()
   redis.call('HSET', P .. ':idem', key, id)
   redis.call('SADD', P .. ':defs', wf_def)
   local expires = false
   if timeout then expires = t + tonumber(timeout) end
   local prev_call, pw = last_call_of(parent)
   local f = {'workflow_id', id, 'state', 'STARTED', 'occurred_at', fmt(t), 'wf_def', wf_def, 'params', params}
   put(f, 'data', metadata)
   put(f, 'expires_at', expires and fmt(expires))
   put(f, 'parent_invocation_id', parent)
   put(f, 'prev_call_id', prev_call)
   redis.call('HSET', ek(id), unpack(f))
   redis.call('HSET', wk(id), 'invocation_id', id, 'state', 'STARTED', 'state_id', id)
   redis.call('ZADD', P .. ':state:STARTED', id, id)
   if expires then set_lease(id, expires) end
   if pw then redis.call('HSET', wk(pw), 'last_call_id', id) end
   add_call(wf_def, id, not parent)
   return id")

(def ^:private finish-lua
  ;; ARGV: id, state, result, expired-only
  "local id, state, result, expired_only = ARGV[2], ARGV[3], opt(ARGV[4]), ARGV[5]
   local e = redis.call('HMGET', ek(id), 'workflow_id', 'state', 'expires_at')
   local wid = e[1]
   if not wid or e[2] ~= 'STARTED' then return 0 end
   if redis.call('HGET', wk(wid), 'state_id') ~= id then return 0 end
   local t = now()
   if expired_only == '1' and (not e[3] or tonumber(e[3]) > t) then return 0 end
   local nid = next_id()
   local f = {'workflow_id', wid, 'state', state, 'occurred_at', fmt(t), 'invocation_id', id, 'prev_id', id}
   put(f, 'data', result)
   redis.call('HSET', ek(nid), unpack(f))
   redis.call('HSET', wk(wid), 'state', state, 'state_id', nid)
   index_state(wid, state)
   set_lease(wid, false)
   return 1")

(def ^:private restart-lua
  ;; ARGV: id, metadata, timeout
  "local id, metadata, timeout = ARGV[2], opt(ARGV[3]), opt(ARGV[4])
   local wid = redis.call('HGET', ek(id), 'workflow_id')
   if not wid then return false end
   local w = redis.call('HMGET', wk(wid), 'invocation_id', 'state_id')
   if w[1] ~= id or w[2] == id then return false end
   local t = now()
   local nid = next_id()
   local expires = false
   if timeout then expires = t + tonumber(timeout) end
   local f = {'workflow_id', wid, 'state', 'STARTED', 'occurred_at', fmt(t), 'prev_id', w[2]}
   put(f, 'data', metadata)
   put(f, 'expires_at', expires and fmt(expires))
   redis.call('HSET', ek(nid), unpack(f))
   redis.call('HSET', wk(wid), 'invocation_id', nid, 'state', 'STARTED', 'state_id', nid)
   index_state(wid, 'STARTED')
   set_lease(wid, expires)
   return nid")

(def ^:private reuse-lua
  ;; ARGV: id, parent, metadata
  "local id, parent, metadata = ARGV[2], opt(ARGV[3]), opt(ARGV[4])
   local e = redis.call('HMGET', ek(id), 'workflow_id', 'state')
   local wid = e[1]
   if not wid or e[2] ~= 'STARTED' then return 0 end
   local wf_def = redis.call('HGET', ek(wid), 'wf_def')
   local prev_call, pw = last_call_of(parent)
   local nid = next_id()
   local f = {'workflow_id', wid, 'state', 'REUSED', 'occurred_at', fmt(now()), 'invocation_id', id, 'wf_def', wf_def}
   put(f, 'data', metadata)
   put(f, 'parent_invocation_id', parent)
   put(f, 'prev_id', redis.call('HGET', wk(wid), 'last_reuse_id'))
   put(f, 'prev_call_id', prev_call)
   redis.call('HSET', ek(nid), unpack(f))
   redis.call('HSET', wk(wid), 'last_reuse_id', nid)
   if pw then redis.call('HSET', wk(pw), 'last_call_id', nid) end
   add_call(wf_def, nid, not parent)
   return 1")

(def ^:private get-lua
  "local e = redis.call('HMGET', ek(ARGV[2]), 'workflow_id', 'state')
   if not e[1] or e[2] ~= 'STARTED' then return false end
   local row = state_row(e[1])
   row[10] = 0
   if row[7] and tonumber(row[7]) <= now() then row[10] = 1 end
   return row")

(def ^:private find-lua
  "local wid = redis.call('HGET', P .. ':idem', ARGV[2])
   if not wid then return false end
   return state_row(wid)")

(def ^:private calls-lua
  ;; ARGV: state, wf_def, top-level-only?, limit, before
  "local state, wf_def, top_only, limit, before = opt(ARGV[2]), opt(ARGV[3]), ARGV[4] == '1', opt(ARGV[5]), opt(ARGV[6])
   if limit then limit = tonumber(limit) + 1 end
   local max = '+inf'
   if before then max = '(' .. before end
   -- Ids of a sorted set, newest first, from max down, that accept(id): up to limit.
   local function scan(key, accept)
     local out, from = {}, max
     while true do
       local chunk = redis.call('ZREVRANGEBYSCORE', key, from, '-inf', 'LIMIT', 0, 256)
       if #chunk == 0 then return out end
       for i = 1, #chunk do
         if accept(chunk[i]) then
           out[#out + 1] = chunk[i]
           if limit and #out >= limit then return out end
         end
       end
       from = '(' .. chunk[#chunk]
     end
   end
   local function matches(parent, def)
     return not (top_only and parent) and not (wf_def and def ~= wf_def)
   end
   local ids
   if state == 'STARTED' or state == 'ERROR' or state == 'TIMEOUT' then
     -- The calls of the (few) workflows in the state: each one's first call,
     -- and its reuses from its own list. Only reuses newer than the page's
     -- oldest first call can make it onto the page.
     local zkey = P .. ':state:' .. state
     ids = scan(zkey, function(wid)
       local f = redis.call('HMGET', ek(wid), 'parent_invocation_id', 'wf_def')
       return matches(f[1], f[2])
     end)
     local bound = 0
     if limit and #ids >= limit then bound = tonumber(ids[#ids]) end
     for _, wid in ipairs(redis.call('ZRANGE', zkey, 0, -1)) do
       local r = redis.call('HGET', wk(wid), 'last_reuse_id')
       while r and tonumber(r) >= bound do
         local e = redis.call('HMGET', ek(r), 'parent_invocation_id', 'wf_def', 'prev_id')
         if (not before or tonumber(r) < tonumber(before)) and matches(e[1], e[2]) then
           ids[#ids + 1] = r
         end
         r = e[3]
       end
     end
     table.sort(ids, function(a, b) return tonumber(a) > tonumber(b) end)
     if limit then
       for i = #ids, limit + 1, -1 do ids[i] = nil end
     end
   else
     local key
     if wf_def then
       key = P .. (top_only and ':calls:deftop:' or ':calls:def:') .. wf_def
     else
       key = P .. (top_only and ':calls:top' or ':calls')
     end
     ids = scan(key, function(cid)
       if not state then return true end
       local wid = redis.call('HGET', ek(cid), 'workflow_id')
       return redis.call('HGET', wk(wid), 'state') == state
     end)
   end
   local rows = {}
   for i = 1, #ids do
     local c = redis.call('HMGET', ek(ids[i]), 'workflow_id', 'occurred_at', 'parent_invocation_id', 'state')
     local r = state_row(c[1])
     rows[i] = {ids[i], c[2], c[3], c[4] == 'REUSED' and 1 or 0, r[1], r[2], r[3], r[5], r[6], r[7], r[8], r[9]}
   end
   return rows")

(def ^:private timeline-lua
  ;; The tree is found by walking call lists, as in the SQLite version: the
  ;; calls of the root, and of every workflow first started by one of them.
  ;; A REUSED call is a leaf, a level below its caller. Each tree workflow's
  ;; states come from its state list; of the root's reuses only those made
  ;; from outside the tree are added, at depth 0.
  "local e0 = redis.call('HMGET', ek(ARGV[2]), 'workflow_id', 'state')
   if not e0[1] or e0[2] ~= 'STARTED' then return {} end
   local root = e0[1]
   local tree = {[root] = true}
   local queue = {{root, 0}}
   local entries = {}
   local qi = 1
   while qi <= #queue do
     local wid, depth = queue[qi][1], queue[qi][2]
     qi = qi + 1
     local w = redis.call('HMGET', wk(wid), 'state_id', 'last_call_id')
     local s = w[1]
     while s do
       entries[#entries + 1] = {s, depth}
       s = redis.call('HGET', ek(s), 'prev_id')
     end
     local c = w[2]
     while c do
       local ce = redis.call('HMGET', ek(c), 'state', 'prev_call_id')
       if ce[1] == 'STARTED' then
         tree[c] = true
         queue[#queue + 1] = {c, depth + 1}
       else
         entries[#entries + 1] = {c, depth + 1}
       end
       c = ce[2]
     end
   end
   local r = redis.call('HGET', wk(root), 'last_reuse_id')
   while r do
     local re = redis.call('HMGET', ek(r), 'parent_invocation_id', 'prev_id')
     local inside = false
     if re[1] then
       local pw = redis.call('HGET', ek(re[1]), 'workflow_id')
       inside = (pw and tree[pw]) or false
     end
     if not inside then entries[#entries + 1] = {r, 0} end
     r = re[2]
   end
   table.sort(entries, function(a, b) return tonumber(a[1]) < tonumber(b[1]) end)
   local rows = {}
   for i = 1, #entries do
     local id = entries[i][1]
     local e = redis.call('HMGET', ek(id), 'workflow_id', 'state', 'occurred_at', 'data', 'invocation_id', 'parent_invocation_id')
     local w = redis.call('HGET', wk(e[1]), 'invocation_id')
     local f = redis.call('HMGET', ek(e[1]), 'wf_def', 'parent_invocation_id')
     local parent = f[2]
     if e[2] == 'REUSED' then parent = e[6] end
     rows[i] = {e[5] or id, w, parent, f[1], e[2], e[3], e[4], entries[i][2]}
   end
   return rows")

(def ^:private extend-lease-lua
  ;; ARGV: comma-separated ids, timeout
  "local expires = now() + tonumber(ARGV[3])
   local done = {}
   for id in string.gmatch(ARGV[2], '[^,]+') do
     local e = redis.call('HMGET', ek(id), 'workflow_id', 'state', 'expires_at')
     local wid = e[1]
     if wid and e[2] == 'STARTED' and e[3] and redis.call('HGET', wk(wid), 'state_id') == id then
       redis.call('HSET', ek(id), 'expires_at', fmt(expires))
       set_lease(wid, expires)
       done[#done + 1] = id
     end
   end
   return done")

(def ^:private list-expired-lua
  "local wids = redis.call('ZRANGEBYSCORE', P .. ':leases', '-inf', now())
   table.sort(wids, function(a, b) return tonumber(a) < tonumber(b) end)
   local rows = {}
   for i = 1, #wids do
     local row = state_row(wids[i])
     row[10] = 1
     rows[i] = row
   end
   return rows")

(defn- script [body] (str prelude body))

(def ^:private scripts
  {:insert (script insert-lua) :finish (script finish-lua) :restart (script restart-lua)
   :reuse (script reuse-lua) :get (script get-lua) :find (script find-lua)
   :calls (script calls-lua) :timeline (script timeline-lua)
   :extend-lease (script extend-lease-lua) :list-expired (script list-expired-lua)})

;;; Client

(deftype RedisStorage [conn ^String prefix ^Closeable pool]
  Closeable
  (close [_] (.close pool)))

(defn- run
  "Evaluates the script named script-name against storage, with args."
  [^RedisStorage storage script-name & args]
  (let [p (.-prefix storage)]
    (car/wcar (.-conn storage)
      (apply car/eval* (scripts script-name) 1 (str p ":seq") p args))))

(defn- opt-arg
  "An optional argument: nil, or the value after a plus."
  [v]
  (if (nil? v) "-" (str "+" v)))

(defn- hex ^String [^bytes bs]
  (let [sb (StringBuilder.)]
    (doseq [b bs] (.append sb (format "%02x" (bit-and (long b) 0xff))))
    (str sb)))

(defn- idempotence-key [wf-def params-edn-str]
  (hex (.digest (MessageDigest/getInstance "SHA-256")
                (.getBytes (str wf-def "\u0000" params-edn-str) "UTF-8"))))

(defn- ->long [v] (when (some? v) (if (string? v) (parse-long v) (long v))))

(defn- state-row
  "The map of a workflow's row, as returned by the Storage methods."
  [[invocation-id wf-def params parent state state-changed-at expires-at metadata result]]
  {:workflow/invocation-id invocation-id
   :workflow/name wf-def
   :workflow/params params
   :workflow/parent-invocation-id parent
   :workflow/state state
   :workflow/state-changed-at state-changed-at
   :workflow/expires-at expires-at
   :workflow/metadata metadata
   :workflow/result result})

(defn- workflow-row [[invocation-id wf-def params parent state state-changed-at expires-at metadata result expired]]
  (cond-> (state-row [(->long invocation-id) wf-def params (->long parent) state (->long state-changed-at)
                      (->long expires-at) metadata result])
    (some? expired) (assoc :workflow/expired (= 1 expired))))

(defn- call-row [[call-id created-at parent reused invocation-id wf-def params state state-changed-at expires-at metadata result]]
  (assoc (state-row [(->long invocation-id) wf-def params (->long parent) state (->long state-changed-at)
                     (->long expires-at) metadata result])
         :event/call-id (->long call-id)
         :event/created-at (->long created-at)
         :event/reused (= 1 reused)))

(defn- timeline-row [[invocation-id current-invocation-id parent wf-def state state-changed-at data depth]]
  {:workflow/invocation-id (->long invocation-id)
   :workflow/current-invocation-id (->long current-invocation-id)
   :workflow/parent-invocation-id (->long parent)
   :workflow/name wf-def
   :workflow/state state
   :workflow/state-changed-at (->long state-changed-at)
   :event/data data
   :workflow/depth (long depth)})

(def ^:private finish-states #{"DONE" "ERROR" "TIMEOUT"})

(defn- finish! [storage invocation-id state result-edn-str expired-only?]
  (when-not (finish-states state)
    (throw (IllegalArgumentException. (str "Not a state a STARTED invocation can end in: " (pr-str state)))))
  (= 1 (run storage :finish (str invocation-id) state (opt-arg result-edn-str) (if expired-only? "1" "0"))))

(extend-type RedisStorage
  db/Storage
  (init-db! [this]
    this)

  (insert-workflow! [this wf-def params-edn-str parent-invocation-id metadata-edn-str timeout-ms]
    (->long (run this :insert (idempotence-key wf-def params-edn-str) wf-def params-edn-str
                 (opt-arg parent-invocation-id) (opt-arg metadata-edn-str) (opt-arg timeout-ms))))

  (finish! [this invocation-id state result-edn-str]
    (finish! this invocation-id state result-edn-str false))

  (time-out! [this invocation-id result-edn-str]
    (finish! this invocation-id "TIMEOUT" result-edn-str true))

  (restart! [this invocation-id metadata-edn-str timeout-ms]
    (->long (run this :restart (str invocation-id) (opt-arg metadata-edn-str) (opt-arg timeout-ms))))

  (record-reuse! [this invocation-id parent-invocation-id metadata-edn-str]
    (run this :reuse (str invocation-id) (opt-arg parent-invocation-id) (opt-arg metadata-edn-str))
    nil)

  (get-workflow [this invocation-id]
    (some-> (run this :get (str invocation-id)) workflow-row))

  (find-by-wf-def-and-params [this wf-def params-edn-str]
    (some-> (run this :find (idempotence-key wf-def params-edn-str)) workflow-row))

  (top-level-workflows
    ([this]
     (db/top-level-workflows this nil))
    ([this {:keys [state wf-def top-level-only? limit before] :or {top-level-only? true}}]
     (mapv call-row (run this :calls (opt-arg state) (opt-arg wf-def) (if top-level-only? "1" "0")
                         (opt-arg limit) (opt-arg before)))))

  (top-level-wf-defs [^RedisStorage this top-level-only?]
    (let [p (.-prefix this)]
      (vec (sort (car/wcar (.-conn this) (car/smembers (str p (if top-level-only? ":defs:top" ":defs"))))))))

  (full-timeline [this invocation-id]
    (mapv timeline-row (run this :timeline (str invocation-id))))

  db/Timeouts
  (extend-lease! [this invocation-ids timeout-ms]
    (if (empty? invocation-ids)
      #{}
      (into #{} (map ->long)
            (run this :extend-lease (str/join "," (map long invocation-ids)) (long timeout-ms)))))

  (list-expired [this]
    (mapv (comp #(assoc % :workflow/expired true) workflow-row) (run this :list-expired))))

(defn datasource
  "A Storage and Timeouts over the Redis at uri (default redis://localhost:6379),
   with its own connection pool of up to pool-size (32) connections. All keys
   start with {prefix} (default \"tenon\"), so that several storages can share
   a server. Pass it to tenon.workflow.db/init-db! (a no-op - Redis has no
   schema). Closeable: close it once it's no longer needed."
  ^java.io.Closeable
  ([] (datasource {}))
  ([{:keys [uri prefix pool-size] :or {uri "redis://localhost:6379" prefix "tenon" pool-size 32}}]
   ;; carmine memoizes pools by options: an :id makes this one ours alone
   (let [pool (conn/conn-pool {:id (random-uuid) :max-total-per-key pool-size :max-idle-per-key pool-size})]
     (->RedisStorage {:pool pool :spec {:uri uri}} (str "{" prefix "}") pool))))

(defn flush!
  "Deletes every key of storage. For tests and benchmarks - there's no undo."
  [^RedisStorage storage]
  (let [pattern (str (str/replace (.-prefix storage) #"[\\*?\[\]]" #(str "\\" %)) ":*")]
    (loop [cursor "0"]
      (let [[next-cursor ks] (car/wcar (.-conn storage) (car/scan cursor "MATCH" pattern "COUNT" 1000))]
        (when (seq ks)
          (car/wcar (.-conn storage) (apply car/del ks)))
        (when-not (= "0" next-cursor)
          (recur next-cursor))))))
