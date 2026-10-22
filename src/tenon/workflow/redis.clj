(ns tenon.workflow.redis
  "The Redis-backed storage implementation: db/Storage and db/Timeouts over
   taoensso/carmine. Same contract as the SQLite one; its own data model,
   built for Redis rather than ported from tables:

   - {p}:seq: INCR counter handing out event ids - the global order. Every
     event (STARTED, DONE, ERROR, TIMEOUT, REUSED) takes one, and a STARTED
     event's id is the invocation id.
   - {p}:wf:<id>: hash, a workflow (<id> is its first invocation id) with
     everything its row shows, resolved: invocation_id, name, params,
     parent (the first call's), created_at, state, state_at, expires_at,
     metadata, result, last_reuse. A point read or a list row is one HMGET.
   - {p}:hist:<id>: list of the workflow's events in order (MessagePack
     [id, state, at, data, invocation_id]) - read by full-timeline only.
   - {p}:inv: hash, restarted invocation id -> workflow id. (A first
     invocation id is the workflow's own, found by its hash existing.)
   - {p}:idem: hash, SHA-256 hex of (wf_def, params) -> workflow id.
   - {p}:ru: hash, REUSED event id -> MessagePack [workflow, at, data,
     invocation, caller]; {p}:reuses:<id> lists a workflow's, by id.
   - {p}:kids:<id>: sorted set of the call ids a workflow made.
   - Sorted sets scored by id, newest first by ZREVRANGEBYSCORE, are the
     indexes: {p}:calls, {p}:calls:top, {p}:calls:def:<wf_def> and
     {p}:calls:deftop:<wf_def> list calls (a workflow's first event, and
     its REUSED events); {p}:state:STARTED/ERROR/TIMEOUT the workflows in a
     state, and {p}:state:<S>:reused those of them that were reused, scored
     by their latest reuse - so listing a rare state touches only them.
     {p}:leases is scored by lease end. {p}:defs and {p}:defs:top are sets
     of wf_def names - all, and those called at the top level.

   Every operation is one Lua script, which Redis runs without interleaving
   anything else: the compare-and-set checks of a write and its writes are
   atomic, as is a read's snapshot. The clock is Redis' (TIME), so that
   timestamps never mix in a second one. {p} is the prefix in braces, a
   Redis Cluster hash tag: all keys share a slot.

   What SQLite adds, and Redis only has if told to: durability. A commit
   there is fsynced (synchronous FULL). Run Redis with appendonly yes and
   appendfsync always for the same guarantee - and noeviction, as an
   evicted key is a lost workflow. Strings stored must not begin with a NUL
   byte (carmine's marker) - EDN never does."
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
   local IDEM, INV, RU = P .. ':idem', P .. ':inv', P .. ':ru'
   local function wk(id) return P .. ':wf:' .. id end
   local function hk(id) return P .. ':hist:' .. id end
   local function kk(id) return P .. ':kids:' .. id end
   local function rk(id) return P .. ':reuses:' .. id end
   local function sk(state) return P .. ':state:' .. state end
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
   local function num(v) if v then return tonumber(v) end return false end
   local function str(v) if v then return fmt(v) end return false end
   local function next_id() return fmt(redis.call('INCR', SEQ)) end
   local function put(f, k, v)
     if v then f[#f + 1] = k; f[#f + 1] = v end
   end
   -- The workflow that the STARTED event id belongs to, or false.
   local function resolve(id)
     if redis.call('EXISTS', wk(id)) == 1 then return id end
     return redis.call('HGET', INV, id)
   end
   local ROW = {'invocation_id', 'name', 'params', 'parent', 'state', 'state_at',
                'expires_at', 'metadata', 'result', 'created_at'}
   local function row(wid) return redis.call('HMGET', wk(wid), unpack(ROW)) end
   local function hist_add(wid, id, state, at, data, inv)
     redis.call('RPUSH', hk(wid), cmsgpack.pack({tonumber(id), state, at, data or false, num(inv)}))
   end
   local function add_call(name, id, top)
     redis.call('ZADD', P .. ':calls', id, id)
     redis.call('ZADD', P .. ':calls:def:' .. name, id, id)
     if top then
       redis.call('ZADD', P .. ':calls:top', id, id)
       redis.call('ZADD', P .. ':calls:deftop:' .. name, id, id)
       redis.call('SADD', P .. ':defs:top', name)
     end
   end
   -- A call made by the workflow of the event parent.
   local function add_kid(parent, id)
     if parent then
       local pw = resolve(parent)
       if pw then redis.call('ZADD', kk(pw), id, id) end
     end
   end
   local function set_lease(wid, expires)
     if expires then redis.call('ZADD', P .. ':leases', expires, wid)
     else redis.call('ZREM', P .. ':leases', wid) end
   end
   -- Moves wid from the indexes of state old (nil: none) to those of new.
   local function index_state(wid, old, new)
     local last_reuse = redis.call('HGET', wk(wid), 'last_reuse')
     if old and old ~= 'DONE' then
       redis.call('ZREM', sk(old), wid)
       redis.call('ZREM', sk(old) .. ':reused', wid)
     end
     if new ~= 'DONE' then
       redis.call('ZADD', sk(new), wid, wid)
       if last_reuse then redis.call('ZADD', sk(new) .. ':reused', last_reuse, wid) end
     end
   end
   ")

(def ^:private insert-lua
  "local key, wf_def, params = ARGV[2], ARGV[3], ARGV[4]
   local parent, metadata, timeout = opt(ARGV[5]), opt(ARGV[6]), opt(ARGV[7])
   if redis.call('HEXISTS', IDEM, key) == 1 then return false end
   local t = now()
   local id = next_id()
   local expires = false
   if timeout then expires = t + tonumber(timeout) end
   redis.call('HSET', IDEM, key, id)
   redis.call('SADD', P .. ':defs', wf_def)
   local f = {'invocation_id', id, 'name', wf_def, 'params', params, 'state', 'STARTED',
              'state_at', fmt(t), 'created_at', fmt(t)}
   put(f, 'parent', parent)
   put(f, 'metadata', metadata)
   put(f, 'expires_at', expires and fmt(expires))
   redis.call('HSET', wk(id), unpack(f))
   hist_add(id, id, 'STARTED', t, metadata, nil)
   index_state(id, nil, 'STARTED')
   if expires then set_lease(id, expires) end
   add_kid(parent, id)
   add_call(wf_def, id, not parent)
   return id")

(def ^:private finish-lua
  ;; ARGV: id, state, result, expired-only
  "local id, state, result, expired_only = ARGV[2], ARGV[3], opt(ARGV[4]), ARGV[5]
   local wid = resolve(id)
   if not wid then return 0 end
   local w = redis.call('HMGET', wk(wid), 'invocation_id', 'state', 'expires_at')
   if w[1] ~= id or w[2] ~= 'STARTED' then return 0 end
   local t = now()
   if expired_only == '1' and (not w[3] or tonumber(w[3]) > t) then return 0 end
   local nid = next_id()
   local f = {'state', state, 'state_at', fmt(t)}
   put(f, 'result', result)
   redis.call('HDEL', wk(wid), 'expires_at')
   redis.call('HSET', wk(wid), unpack(f))
   hist_add(wid, nid, state, t, result, id)
   index_state(wid, 'STARTED', state)
   set_lease(wid, false)
   return 1")

(def ^:private restart-lua
  ;; ARGV: id, metadata, timeout
  "local id, metadata, timeout = ARGV[2], opt(ARGV[3]), opt(ARGV[4])
   local wid = resolve(id)
   if not wid then return false end
   local w = redis.call('HMGET', wk(wid), 'invocation_id', 'state')
   if w[1] ~= id or w[2] == 'STARTED' then return false end
   local t = now()
   local nid = next_id()
   local expires = false
   if timeout then expires = t + tonumber(timeout) end
   local f = {'invocation_id', nid, 'state', 'STARTED', 'state_at', fmt(t)}
   put(f, 'metadata', metadata)
   put(f, 'expires_at', expires and fmt(expires))
   redis.call('HDEL', wk(wid), 'result', 'metadata', 'expires_at')
   redis.call('HSET', wk(wid), unpack(f))
   redis.call('HSET', INV, nid, wid)
   hist_add(wid, nid, 'STARTED', t, metadata, nil)
   index_state(wid, w[2], 'STARTED')
   set_lease(wid, expires)
   return nid")

(def ^:private reuse-lua
  ;; ARGV: id, parent, metadata
  "local id, parent, metadata = ARGV[2], opt(ARGV[3]), opt(ARGV[4])
   local wid = resolve(id)
   if not wid then return 0 end
   local w = redis.call('HMGET', wk(wid), 'name', 'state')
   local nid = next_id()
   redis.call('HSET', RU, nid, cmsgpack.pack({tonumber(wid), now(), metadata or false, tonumber(id), num(parent)}))
   redis.call('ZADD', rk(wid), nid, nid)
   redis.call('HSET', wk(wid), 'last_reuse', nid)
   if w[2] ~= 'DONE' then redis.call('ZADD', sk(w[2]) .. ':reused', nid, wid) end
   add_kid(parent, nid)
   add_call(w[1], nid, not parent)
   return 1")

(def ^:private get-lua
  "local wid = resolve(ARGV[2])
   if not wid then return false end
   local r = row(wid)
   r[11] = 0
   if r[7] and tonumber(r[7]) <= now() then r[11] = 1 end
   return r")

(def ^:private find-lua
  "local wid = redis.call('HGET', IDEM, ARGV[2])
   if not wid then return false end
   return row(wid)")

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
   local ids
   if state == 'STARTED' or state == 'ERROR' or state == 'TIMEOUT' then
     -- The (few) workflows in the state: each one's first call, and the
     -- calls that reused it. Only reuses newer than the page's oldest first
     -- call can make it onto the page.
     ids = scan(sk(state), function(wid)
       local f = redis.call('HMGET', wk(wid), 'parent', 'name')
       return not (top_only and f[1]) and not (wf_def and f[2] ~= wf_def)
     end)
     local bound = '0'
     if limit and #ids >= limit then bound = ids[#ids] end
     for _, wid in ipairs(redis.call('ZRANGEBYSCORE', sk(state) .. ':reused', bound, '+inf')) do
       if not wf_def or redis.call('HGET', wk(wid), 'name') == wf_def then
         for _, r in ipairs(redis.call('ZREVRANGEBYSCORE', rk(wid), max, bound)) do
           if not top_only or not cmsgpack.unpack(redis.call('HGET', RU, r))[5] then
             ids[#ids + 1] = r
           end
         end
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
       local st = redis.call('HGET', wk(cid), 'state')
       if not st then
         st = redis.call('HGET', wk(fmt(cmsgpack.unpack(redis.call('HGET', RU, cid))[1])), 'state')
       end
       return st == state
     end)
   end
   local rows = {}
   for i = 1, #ids do
     local r = row(ids[i])
     if r[5] then
       rows[i] = {ids[i], r[10], r[4], 0, r[1], r[2], r[3], r[5], r[6], r[7], r[8], r[9]}
     else
       local ru = cmsgpack.unpack(redis.call('HGET', RU, ids[i]))
       r = row(fmt(ru[1]))
       rows[i] = {ids[i], fmt(ru[2]), str(ru[5]), 1, r[1], r[2], r[3], r[5], r[6], r[7], r[8], r[9]}
     end
   end
   return rows")

(def ^:private timeline-lua
  ;; The tree is found by walking the calls each workflow made, from the
  ;; root: a call is either the first start of a workflow, which joins the
  ;; tree a level down, or a REUSED event, a leaf a level below its caller.
  ;; Each tree workflow's events come from its history; of the root's
  ;; reuses only those made from outside the tree are added, at depth 0.
  "local root = resolve(ARGV[2])
   if not root then return {} end
   local tree = {[root] = true}
   local queue = {{root, 0}}
   local entries = {}
   local function reuse_entry(cid, depth)
     local ru = cmsgpack.unpack(redis.call('HGET', RU, cid))
     entries[#entries + 1] = {id = tonumber(cid), state = 'REUSED', at = ru[2], data = ru[3], inv = ru[4],
                              wid = fmt(ru[1]), parent = ru[5], depth = depth}
   end
   local qi = 1
   while qi <= #queue do
     local wid, depth = queue[qi][1], queue[qi][2]
     qi = qi + 1
     for _, packed in ipairs(redis.call('LRANGE', hk(wid), 0, -1)) do
       local e = cmsgpack.unpack(packed)
       entries[#entries + 1] = {id = e[1], state = e[2], at = e[3], data = e[4], inv = e[5], wid = wid, depth = depth}
     end
     for _, cid in ipairs(redis.call('ZRANGE', kk(wid), 0, -1)) do
       if redis.call('EXISTS', wk(cid)) == 1 then
         tree[cid] = true
         queue[#queue + 1] = {cid, depth + 1}
       else
         reuse_entry(cid, depth + 1)
       end
     end
   end
   for _, cid in ipairs(redis.call('ZRANGE', rk(root), 0, -1)) do
     local parent = cmsgpack.unpack(redis.call('HGET', RU, cid))[5]
     local pw = parent and resolve(fmt(parent))
     if not (pw and tree[pw]) then reuse_entry(cid, 0) end
   end
   table.sort(entries, function(a, b) return a.id < b.id end)
   local infos, rows = {}, {}
   for i = 1, #entries do
     local e = entries[i]
     local w = infos[e.wid]
     if not w then
       w = redis.call('HMGET', wk(e.wid), 'invocation_id', 'name', 'parent')
       infos[e.wid] = w
     end
     local parent = w[3]
     if e.state == 'REUSED' then parent = str(e.parent) end
     rows[i] = {e.inv and fmt(e.inv) or fmt(e.id), w[1], parent, w[2], e.state, fmt(e.at), e.data, e.depth}
   end
   return rows")

(def ^:private extend-lease-lua
  ;; ARGV: comma-separated ids, timeout
  "local expires = now() + tonumber(ARGV[3])
   local done = {}
   for id in string.gmatch(ARGV[2], '[^,]+') do
     local wid = resolve(id)
     if wid then
       local w = redis.call('HMGET', wk(wid), 'invocation_id', 'state', 'expires_at')
       if w[1] == id and w[2] == 'STARTED' and w[3] then
         redis.call('HSET', wk(wid), 'expires_at', fmt(expires))
         set_lease(wid, expires)
         done[#done + 1] = id
       end
     end
   end
   return done")

(def ^:private list-expired-lua
  "local wids = redis.call('ZRANGEBYSCORE', P .. ':leases', '-inf', now())
   table.sort(wids, function(a, b) return tonumber(a) < tonumber(b) end)
   local rows = {}
   for i = 1, #wids do
     local r = row(wids[i])
     r[11] = 1
     rows[i] = r
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

(defn- workflow-row [[invocation-id wf-def params parent state state-changed-at expires-at metadata result _created-at expired]]
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
