(ns tenon.workflow.redis
  "The Redis-backed storage implementation: db/Storage and db/Timeouts over
   taoensso/carmine. Same contract as the SQLite one; its own data model,
   built for Redis rather than ported from tables:

   - Ids are UUIDv7 strings: the time in ms, a counter and random bits, so
     that they sort, as strings, in the order they were made (and stay
     unique, and in order, if the clock goes back). Every event (STARTED,
     DONE, ERROR, TIMEOUT, REUSED) takes one, and a STARTED event's id is
     the invocation id. {p}:seq holds the last one's ms and counter.
   - {p}:wf:<id>: string, a workflow (<id> is its first invocation id) with
     everything its row shows, resolved, as one packed blob: the fields
     lease end, state, invocation id, name, params, parent (the first
     call's), state_at, metadata, result and created_at, each as <byte
     length>:<bytes>, or - for none. A point read is a GET, a list row is
     the blob itself, and neither is taken apart on the server.
   - {p}:hist:<id>: list of the workflow's events in order (MessagePack
     [id, state, at, data, invocation_id]) - read by full-timeline only.
   - {p}:inv: hash, restarted invocation id -> workflow id. (A first
     invocation id is the workflow's own, found by its blob existing.)
   - {p}:idem: hash, SHA-256 hex of (wf_def, params) -> workflow id.
   - {p}:ru: hash, REUSED event id -> MessagePack [workflow, at, data,
     invocation, caller]; {p}:reuses:<id> lists a workflow's, by id, and
     {p}:lr maps a workflow id to its latest.
   - {p}:kids:<id>: sorted set of the call ids a workflow made.
   - Sorted sets with all scores 0 - so ordered by id - read newest first
     by ZREVRANGEBYLEX are the indexes: {p}:calls, {p}:calls:top, {p}:calls:def:<wf_def> and
     {p}:calls:deftop:<wf_def> list calls (a workflow's first event, and
     its REUSED events); {p}:state:STARTED/ERROR/TIMEOUT the workflows in a
     state, and {p}:state:<S>:reused those of them that were reused, scored
     by the ms of their latest reuse - so listing a rare state touches only
     them.
     {p}:leases is scored by lease end. {p}:defs and {p}:defs:top are sets
     of wf_def names - all, and those called at the top level.

   Writes and the multi-key reads are Lua scripts, which Redis runs without
   interleaving anything else: the compare-and-set checks of a write and its
   writes are atomic, as is a read's snapshot. The clock is Redis' (TIME),
   so that timestamps never mix in a second one. {p} is the prefix in
   braces, a Redis Cluster hash tag: all keys share a slot.

   The round trip dominates a Redis call, so concurrent callers share
   them: a caller queues its commands, and whichever caller finds no one
   sending sends everyone's queued commands in one pipeline (flat
   combining) - a lone caller sends its own at once, as without it.

   What SQLite adds, and Redis only has if told to: durability. A commit
   there is fsynced (synchronous FULL). Run Redis with appendonly yes and
   appendfsync always for the same guarantee - and noeviction, as an
   evicted key is a lost workflow. Strings stored must not begin with a NUL
   byte (carmine's marker) - EDN never does."
  (:require [clojure.string :as str]
            [taoensso.carmine :as car]
            [taoensso.carmine.connections :as conn]
            [tenon.workflow.db :as db])
  (:import [java.io Closeable InputStream OutputStream]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.util.concurrent ConcurrentLinkedQueue]
           [java.util.concurrent.atomic AtomicInteger AtomicReference]
           [java.util.concurrent.locks LockSupport]))

;;; Lua

(def ^:private prelude
  "local P = ARGV[1]
   local SEQ = KEYS[1]
   local RND = ARGV[#ARGV]
   local IDEM, INV, RU, LR = P .. ':idem', P .. ':inv', P .. ':ru', P .. ':lr'
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
   -- A new id, a UUIDv7: the time in ms, a counter that keeps ids made in one
   -- ms (or after the clock went back) in order, and random bits from RND.
   -- Ids sort as strings in the order they were made.
   local function new_id(t)
     local last, ms, c = redis.call('GET', SEQ), t, 0
     if last then
       local lms, lc = string.match(last, '^(%d+),(%d+)$')
       lms, lc = tonumber(lms), tonumber(lc)
       if lms >= t then
         ms, c = lms, lc + 1
         if c > 4095 then ms, c = lms + 1, 0 end
       end
     end
     redis.call('SET', SEQ, fmt(ms) .. ',' .. fmt(c))
     local hex = string.format('%012x', ms)
     return string.format('%s-%s-7%03x-%x%s-%s', string.sub(hex, 1, 8), string.sub(hex, 9, 12), c,
                          8 + tonumber(string.sub(RND, 1, 1), 16) % 4, string.sub(RND, 2, 4), string.sub(RND, 5, 16))
   end
   -- The time in ms in an id, and whether id a is before b.
   local function id_ms(id) return tonumber(string.sub(id, 1, 8) .. string.sub(id, 10, 13), 16) end
   local function id_before(a, b)
     -- the locale may collate strings otherwise than bytewise
     for _, r in ipairs({{1, 8}, {10, 13}, {15, 18}, {20, 23}, {25, 36}}) do
       local x, y = tonumber(string.sub(a, r[1], r[2]), 16), tonumber(string.sub(b, r[1], r[2]), 16)
       if x ~= y then return x < y end
     end
     return false
   end
   -- A field as <byte length>:<bytes>, - for none; and the first k fields (all) of a packed string.
   local function enc(v) if v then return #v .. ':' .. v end return '-' end
   local function dec(s, k)
     local out, i, n = {}, 1, #s
     while i <= n and (not k or #out < k) do
       if string.byte(s, i) == 45 then
         out[#out + 1] = false
         i = i + 1
       else
         local j = string.find(s, ':', i, true)
         local len = tonumber(string.sub(s, i, j - 1))
         out[#out + 1] = string.sub(s, j + 1, j + len)
         i = j + len + 1
       end
     end
     return out
   end
   -- A workflow blob's fields: 1 lease end, 2 state, 3 invocation id, 4 name, 5 params,
   -- 6 parent, 7 state_at, 8 metadata, 9 result, 10 created_at.
   local function mk(h)
     local p = {}
     for i = 1, 10 do p[i] = enc(h[i]) end
     return table.concat(p)
   end
   -- The workflow that the STARTED event id belongs to, or false.
   local function resolve(id)
     if redis.call('EXISTS', wk(id)) == 1 then return id end
     return redis.call('HGET', INV, id)
   end
   local function hist_add(wid, id, state, at, data, inv)
     redis.call('RPUSH', hk(wid), cmsgpack.pack({id, state, at, data or false, inv or false}))
   end
   local function add_call(name, id, top)
     redis.call('ZADD', P .. ':calls', 0, id)
     redis.call('ZADD', P .. ':calls:def:' .. name, 0, id)
     if top then
       redis.call('ZADD', P .. ':calls:top', 0, id)
       redis.call('ZADD', P .. ':calls:deftop:' .. name, 0, id)
       redis.call('SADD', P .. ':defs:top', name)
     end
   end
   -- A call made by the workflow of the event parent.
   local function add_kid(parent, id)
     if parent then
       local pw = resolve(parent)
       if pw then redis.call('ZADD', kk(pw), 0, id) end
     end
   end
   local function set_lease(wid, expires)
     if expires then redis.call('ZADD', P .. ':leases', expires, wid)
     else redis.call('ZREM', P .. ':leases', wid) end
   end
   -- Moves wid from the indexes of state old (nil: none) to those of new.
   local function index_state(wid, old, new)
     if old and old ~= 'DONE' then
       redis.call('ZREM', sk(old), wid)
       redis.call('ZREM', sk(old) .. ':reused', wid)
     end
     if new ~= 'DONE' then
       redis.call('ZADD', sk(new), 0, wid)
       local last_reuse = redis.call('HGET', LR, wid)
       if last_reuse then redis.call('ZADD', sk(new) .. ':reused', id_ms(last_reuse), wid) end
     end
   end
   ")

(def ^:private insert-lua
  "local key, wf_def, params = ARGV[2], ARGV[3], ARGV[4]
   local parent, metadata, timeout = opt(ARGV[5]), opt(ARGV[6]), opt(ARGV[7])
   if redis.call('HEXISTS', IDEM, key) == 1 then return false end
   local t = now()
   local id = new_id(t)
   local expires = false
   if timeout then expires = t + tonumber(timeout) end
   redis.call('HSET', IDEM, key, id)
   redis.call('SADD', P .. ':defs', wf_def)
   redis.call('SET', wk(id), mk({expires and fmt(expires), 'STARTED', id, wf_def, params, parent or false,
                                 fmt(t), metadata or false, false, fmt(t)}))
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
   local h = dec(redis.call('GET', wk(wid)))
   if h[3] ~= id or h[2] ~= 'STARTED' then return 0 end
   local t = now()
   if expired_only == '1' and (not h[1] or tonumber(h[1]) > t) then return 0 end
   local nid = new_id(t)
   h[1], h[2], h[7], h[9] = false, state, fmt(t), result or false
   redis.call('SET', wk(wid), mk(h))
   hist_add(wid, nid, state, t, result, id)
   index_state(wid, 'STARTED', state)
   set_lease(wid, false)
   return 1")

(def ^:private restart-lua
  ;; ARGV: id, metadata, timeout
  "local id, metadata, timeout = ARGV[2], opt(ARGV[3]), opt(ARGV[4])
   local wid = resolve(id)
   if not wid then return false end
   local h = dec(redis.call('GET', wk(wid)))
   if h[3] ~= id or h[2] == 'STARTED' then return false end
   local old = h[2]
   local t = now()
   local nid = new_id(t)
   local expires = false
   if timeout then expires = t + tonumber(timeout) end
   h[1], h[2], h[3], h[7], h[8], h[9] = expires and fmt(expires), 'STARTED', nid, fmt(t), metadata or false, false
   redis.call('SET', wk(wid), mk(h))
   redis.call('HSET', INV, nid, wid)
   hist_add(wid, nid, 'STARTED', t, metadata, nil)
   index_state(wid, old, 'STARTED')
   set_lease(wid, expires)
   return nid")

(def ^:private reuse-lua
  ;; ARGV: id, parent, metadata
  "local id, parent, metadata = ARGV[2], opt(ARGV[3]), opt(ARGV[4])
   local wid = resolve(id)
   if not wid then return 0 end
   local h = dec(redis.call('GET', wk(wid)), 4)
   local t = now()
   local nid = new_id(t)
   redis.call('HSET', RU, nid, cmsgpack.pack({wid, t, metadata or false, id, parent or false}))
   redis.call('ZADD', rk(wid), 0, nid)
   redis.call('HSET', LR, wid, nid)
   if h[2] ~= 'DONE' then redis.call('ZADD', sk(h[2]) .. ':reused', id_ms(nid), wid) end
   add_kid(parent, nid)
   add_call(h[4], nid, not parent)
   return 1")

(def ^:private get-lua
  "local wid = resolve(ARGV[2])
   if not wid then return false end
   return redis.call('GET', wk(wid))")

(def ^:private find-lua
  "local wid = redis.call('HGET', IDEM, ARGV[2])
   if not wid then return false end
   return redis.call('GET', wk(wid))")

(def ^:private calls-lua
  ;; ARGV: state, wf_def, top-level-only?, limit, before
  "local state, wf_def, top_only, limit, before = opt(ARGV[2]), opt(ARGV[3]), ARGV[4] == '1', opt(ARGV[5]), opt(ARGV[6])
   if limit then limit = tonumber(limit) + 1 end
   local max = '+'
   if before then max = '(' .. before end
   -- Ids of a lexicographic set, newest first, from max down, that accept(id): up to limit.
   local function scan(key, accept)
     local out, from = {}, max
     while true do
       local chunk = redis.call('ZREVRANGEBYLEX', key, from, '-', 'LIMIT', 0, 256)
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
   -- The blob of the workflow that call cid belongs to, and its reuse entry if it is one.
   local function call_blob(cid)
     local b = redis.call('GET', wk(cid))
     if b then return b, nil end
     local ru = cmsgpack.unpack(redis.call('HGET', RU, cid))
     return redis.call('GET', wk(ru[1])), ru
   end
   local ids
   if state == 'STARTED' or state == 'ERROR' or state == 'TIMEOUT' then
     -- The (few) workflows in the state: each one's first call, and the
     -- calls that reused it. Only reuses newer than the page's oldest first
     -- call can make it onto the page.
     ids = scan(sk(state), function(wid)
       local h = dec(redis.call('GET', wk(wid)), 6)
       return not (top_only and h[6]) and not (wf_def and h[4] ~= wf_def)
     end)
     local bound = nil
     if limit and #ids >= limit then bound = ids[#ids] end
     local reused = redis.call('ZRANGEBYSCORE', sk(state) .. ':reused', bound and id_ms(bound) or '-inf', '+inf')
     for _, wid in ipairs(reused) do
       if not wf_def or dec(redis.call('GET', wk(wid)), 4)[4] == wf_def then
         for _, r in ipairs(redis.call('ZREVRANGEBYLEX', rk(wid), max, bound and ('[' .. bound) or '-')) do
           if not top_only or not cmsgpack.unpack(redis.call('HGET', RU, r))[5] then
             ids[#ids + 1] = r
           end
         end
       end
     end
     table.sort(ids, function(a, b) return id_before(b, a) end)
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
       return dec((call_blob(cid)), 2)[2] == state
     end)
   end
   -- A row: call id, 0 and the workflow blob; or for a reuse call id, 1, when,
   -- the caller and the blob.
   local out = {}
   for i = 1, #ids do
     local b, ru = call_blob(ids[i])
     if ru then
       out[i] = enc(ids[i]) .. '1:1' .. enc(fmt(ru[2])) .. enc(ru[5]) .. enc(b)
     else
       out[i] = enc(ids[i]) .. '1:0' .. enc(b)
     end
   end
   return table.concat(out)")

(def ^:private timeline-lua
  ;; The tree is found by walking the calls each workflow made, from the
  ;; root: a call is either the first start of a workflow, which joins the
  ;; tree a level down, or a REUSED event, a leaf a level below its caller.
  ;; Each tree workflow's events come from its history; of the root's
  ;; reuses only those made from outside the tree are added, at depth 0.
  "local root = resolve(ARGV[2])
   if not root then return '' end
   local tree = {[root] = true}
   local queue = {{root, 0}}
   local entries = {}
   local function reuse_entry(cid, depth)
     local ru = cmsgpack.unpack(redis.call('HGET', RU, cid))
     entries[#entries + 1] = {id = cid, state = 'REUSED', at = ru[2], data = ru[3], inv = ru[4],
                              wid = ru[1], parent = ru[5], depth = depth}
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
     local pw = parent and resolve(parent)
     if not (pw and tree[pw]) then reuse_entry(cid, 0) end
   end
   table.sort(entries, function(a, b) return id_before(a.id, b.id) end)
   local infos, out = {}, {}
   for i = 1, #entries do
     local e = entries[i]
     local h = infos[e.wid]
     if not h then
       h = dec(redis.call('GET', wk(e.wid)), 6)
       infos[e.wid] = h
     end
     local parent = h[6]
     if e.state == 'REUSED' then parent = e.parent end
     out[i] = enc(e.inv or e.id) .. enc(h[3]) .. enc(parent) .. enc(h[4]) .. enc(e.state)
              .. enc(fmt(e.at)) .. enc(e.data) .. enc(fmt(e.depth))
   end
   return table.concat(out)")

(def ^:private extend-lease-lua
  ;; ARGV: comma-separated ids, timeout
  "local expires = now() + tonumber(ARGV[3])
   local done = {}
   for id in string.gmatch(ARGV[2], '[^,]+') do
     local wid = resolve(id)
     if wid then
       local b = redis.call('GET', wk(wid))
       local h = dec(b, 3)
       if h[3] == id and h[2] == 'STARTED' and h[1] then
         local colon = string.find(b, ':', 1, true)
         local rest = string.sub(b, colon + tonumber(string.sub(b, 1, colon - 1)) + 1)
         redis.call('SET', wk(wid), enc(fmt(expires)) .. rest)
         set_lease(wid, expires)
         done[#done + 1] = id
       end
     end
   end
   return table.concat(done, ',')")

(def ^:private list-expired-lua
  "local wids = redis.call('ZRANGEBYSCORE', P .. ':leases', '-inf', now())
   table.sort(wids, id_before)
   local out = {}
   for i = 1, #wids do out[i] = enc(redis.call('GET', wk(wids[i]))) end
   return table.concat(out)")

(def ^:private scripts
  (into {} (map (fn [[k body]] [k (str prelude body)]))
        {:insert insert-lua :finish finish-lua :restart restart-lua :reuse reuse-lua :get get-lua
         :find find-lua :calls calls-lua :timeline timeline-lua :extend-lease extend-lease-lua
         :list-expired list-expired-lua}))

(def ^:private script-shas
  (into {} (map (fn [[k script]] [k (car/script-hash script)])) scripts))

;;; Sending: concurrent callers share a pipeline

(deftype RedisStorage [conn-opts ^String prefix ^Closeable pool
                       ^ConcurrentLinkedQueue queue
                       ^AtomicInteger sending ^long max-senders ^AtomicInteger waiting]
  Closeable
  (close [_] (.close pool)))

(deftype Request [cmds ^AtomicReference outcome ^Thread thread])

(defn- load-scripts! [^RedisStorage storage]
  (car/wcar (.-conn-opts storage)
    (doseq [[_ script] scripts] (car/redis-call ["SCRIPT" "LOAD" script]))))

;; RESP, by hand: carmine's wcar costs more per call than Redis does, so the
;; connection - carmine's: pool, URI, auth, TLS, timeouts - is written to
;; and read directly.

(def ^:private crlf (byte-array [13 10]))

(defn- write-command! [^OutputStream out cmd]
  (.write out 42)
  (.write out (.getBytes (str (count cmd)) StandardCharsets/US_ASCII))
  (.write out ^bytes crlf)
  (doseq [arg cmd]
    (let [bs (.getBytes (str arg) StandardCharsets/UTF_8)]
      (.write out 36)
      (.write out (.getBytes (str (alength bs)) StandardCharsets/US_ASCII))
      (.write out ^bytes crlf)
      (.write out bs)
      (.write out ^bytes crlf))))

(defn- read-line! ^String [^InputStream in]
  (let [sb (StringBuilder.)]
    (loop []
      (let [c (.read in)]
        (cond (neg? c) (throw (java.io.EOFException. "Redis closed the connection"))
              (== c 13) (do (.read in) (str sb))
              :else (do (.append sb (char c)) (recur)))))))

(defn- read-reply!
  "A reply: byte array (bulk string), long, string (status), vector, nil, or
   an ExceptionInfo for an error."
  [^InputStream in]
  (let [t (.read in)]
    (case (char t)
      \+ (read-line! in)
      \- (ex-info (read-line! in) {})
      \: (parse-long (read-line! in))
      \$ (let [n (parse-long (read-line! in))]
           (when-not (neg? n)
             (let [b (byte-array n)]
               (loop [off 0]
                 (when (< off n)
                   (let [r (.read in b off (- n off))]
                     (when (neg? r) (throw (java.io.EOFException. "Redis closed the connection")))
                     (recur (+ off r)))))
               (.read in) (.read in)
               b)))
      \* (let [n (parse-long (read-line! in))]
           (when-not (neg? n)
             (loop [i 0 acc (transient [])]
               (if (< i n) (recur (inc i) (conj! acc (read-reply! in))) (persistent! acc)))))
      (throw (java.io.IOException. (str "Unexpected reply type " t))))))

(defn- send-batch!
  "Sends the commands of requests in one pipeline, and gives each its
   replies - byte arrays for strings - or the exception."
  [^RedisStorage storage requests]
  (let [[pool conn] (try (conn/pooled-conn (.-conn-opts storage)) (catch Throwable e [nil e]))
        replies
        (if (instance? Throwable conn)
          conn
          (try (let [^OutputStream out (:out conn)
                     ^InputStream in (:in conn)]
                 (doseq [^Request r requests, cmd (.-cmds r)] (write-command! out cmd))
                 (.flush out)
                 (let [replies (loop [rs (seq requests) acc (transient [])]
                                 (if rs
                                   (recur (next rs) (reduce (fn [acc _] (conj! acc (read-reply! in))) acc (.-cmds ^Request (first rs))))
                                   (persistent! acc)))]
                   (conn/release-conn pool conn)
                   replies))
               (catch Throwable e
                 (conn/release-conn pool conn e)
                 e)))]
    (if (instance? Throwable replies)
      (doseq [^Request r requests] (.set (.-outcome r) replies))
      (loop [rs (seq requests), replies replies]
        (when rs
          (let [^Request r (first rs)
                n (count (.-cmds r))
                mine (subvec replies 0 n)
                failed (first (filter #(instance? Throwable %) mine))]
            (.set (.-outcome r) (or failed mine))
            (recur (next rs) (subvec replies n))))))
    (doseq [^Request r requests] (LockSupport/unpark (.-thread r)))))

(defn- send-until-done! [^RedisStorage storage ^Request mine]
  (let [^ConcurrentLinkedQueue queue (.-queue storage)]
    (loop []
      (when (nil? (.get (.-outcome mine)))
        (let [batch (loop [acc (transient [])]
                      (if-let [r (when (< (count acc) 1024) (.poll queue))]
                        (recur (conj! acc r))
                        (persistent! acc)))]
          (when (seq batch)
            (send-batch! storage batch)
            (recur)))))))

(def ^:private spin-ns
  "How long a caller waiting for its reply spins before it parks - when
   there are cores to spare."
  30000)

(def ^:private spare-cores
  (max 0 (- (.availableProcessors (Runtime/getRuntime)) 2)))

(defn- take-sender-slot! [^RedisStorage storage]
  (let [^AtomicInteger sending (.-sending storage)]
    (loop []
      (let [n (.get sending)]
        (and (< n (.-max-senders storage))
             (or (.compareAndSet sending n (inc n)) (recur)))))))

(defn- wait-for-reply!
  "Spins for a moment, if cores are free, then parks."
  [^RedisStorage storage ^Request req]
  (let [^AtomicInteger waiting (.-waiting storage)
        n (.incrementAndGet waiting)]
    (try
      (when (<= n spare-cores)
        (let [deadline (+ (System/nanoTime) spin-ns)]
          (loop []
            (when (and (nil? (.get (.-outcome req))) (< (System/nanoTime) deadline))
              (Thread/onSpinWait)
              (recur)))))
      (when (nil? (.get (.-outcome req)))
        (LockSupport/parkNanos storage 200000))
      (finally (.decrementAndGet waiting)))))

(defn- exec!
  "Sends cmds - vectors of strings - and returns their replies, strings as
   byte arrays."
  [^RedisStorage storage cmds]
  (let [^ConcurrentLinkedQueue queue (.-queue storage)
        ^Request req (->Request cmds (AtomicReference.) (Thread/currentThread))]
    (.add queue req)
    (loop []
      (let [outcome (.get (.-outcome req))]
        (cond
          (some? outcome)
          (if (instance? Throwable outcome)
            (if (str/includes? (str (ex-message outcome)) "NOSCRIPT")
              (do (load-scripts! storage) (exec! storage cmds))
              (throw outcome))
            outcome)

          (take-sender-slot! storage)
          (do (try (send-until-done! storage req)
                   (finally
                     (.decrementAndGet ^AtomicInteger (.-sending storage))
                     (when-let [^Request next-up (.peek queue)]
                       (LockSupport/unpark (.-thread next-up)))))
              (recur))

          :else
          (do (wait-for-reply! storage req) (recur)))))))

(def ^:private ^java.security.SecureRandom secure-random (java.security.SecureRandom.))

(defn- random-hex
  "16 random hex digits: the random bits of an id."
  []
  (format "%016x" (.nextLong secure-random)))

(defn- script-cmd [^RedisStorage storage script-name & args]
  (let [p (.-prefix storage)]
    (-> ["EVALSHA" (script-shas script-name) "1" (str p ":seq") p]
        (into args)
        (conj (random-hex)))))

(defn- run1
  "The reply of one script."
  [storage script-name & args]
  (first (exec! storage [(apply script-cmd storage script-name args)])))

;;; Decoding

(defn- ->str ^String [^bytes b] (when b (String. b StandardCharsets/UTF_8)))

(defn- read-len!
  "The length prefix at pos; moves pos after the colon."
  ^long [^bytes b ^ints pos]
  (let [start (aget pos 0)]
    (loop [i start n 0]
      (let [c (aget b i)]
        (if (== c 58)
          (do (aset pos 0 (inc i)) n)
          (recur (inc i) (+ (* n 10) (- c 48))))))))

(defn- read-str! [^bytes b ^ints pos]
  (if (== 45 (aget b (aget pos 0)))
    (do (aset pos 0 (inc (aget pos 0))) nil)
    (let [n (read-len! b pos)
          i (aget pos 0)]
      (aset pos 0 (+ i n))
      (String. b i n StandardCharsets/UTF_8))))

(defn- read-long! [^bytes b ^ints pos]
  (if (== 45 (aget b (aget pos 0)))
    (do (aset pos 0 (inc (aget pos 0))) nil)
    (let [n (read-len! b pos)
          i (aget pos 0)]
      (aset pos 0 (+ i n))
      (loop [j i acc 0]
        (if (< j (+ i n))
          (recur (inc j) (+ (* acc 10) (- (aget b j) 48)))
          acc)))))

(defn- read-row!
  "A workflow blob (after its length prefix) as a Storage row, with its
   lease end and creation time as the second and third values."
  [^bytes b ^ints pos]
  (let [expires-at (read-long! b pos)
        state (read-str! b pos)
        invocation-id (read-str! b pos)
        wf-def (read-str! b pos)
        params (read-str! b pos)
        parent (read-str! b pos)
        state-changed-at (read-long! b pos)
        metadata (read-str! b pos)
        result (read-str! b pos)
        created-at (read-long! b pos)]
    [{:workflow/invocation-id invocation-id
      :workflow/name wf-def
      :workflow/params params
      :workflow/parent-invocation-id parent
      :workflow/state state
      :workflow/state-changed-at state-changed-at
      :workflow/expires-at expires-at
      :workflow/metadata metadata
      :workflow/result result}
     created-at]))

(defn- blob-row [^bytes b]
  (first (read-row! b (int-array 1))))

(defn- blobs->rows
  "The rows of a sequence of length-prefixed blobs."
  [^bytes b]
  (let [pos (int-array 1)]
    (loop [rows (transient [])]
      (if (< (aget pos 0) (alength b))
        (do (read-len! b pos)
            (recur (conj! rows (first (read-row! b pos)))))
        (persistent! rows)))))

(defn- calls->rows [^bytes b]
  (let [pos (int-array 1)]
    (loop [rows (transient [])]
      (if (< (aget pos 0) (alength b))
        (let [call-id (read-str! b pos)
              reused? (= "1" (read-str! b pos))
              created-at (when reused? (read-long! b pos))
              caller (when reused? (read-str! b pos))
              _ (read-len! b pos)
              [row first-created-at] (read-row! b pos)]
          (recur (conj! rows (assoc row
                                    :workflow/parent-invocation-id (if reused? caller (:workflow/parent-invocation-id row))
                                    :event/call-id call-id
                                    :event/created-at (if reused? created-at first-created-at)
                                    :event/reused reused?))))
        (persistent! rows)))))

(defn- timeline->rows [^bytes b]
  (let [pos (int-array 1)]
    (loop [rows (transient [])]
      (if (< (aget pos 0) (alength b))
        (recur (conj! rows {:workflow/invocation-id (read-str! b pos)
                            :workflow/current-invocation-id (read-str! b pos)
                            :workflow/parent-invocation-id (read-str! b pos)
                            :workflow/name (read-str! b pos)
                            :workflow/state (read-str! b pos)
                            :workflow/state-changed-at (read-long! b pos)
                            :event/data (read-str! b pos)
                            :workflow/depth (read-long! b pos)}))
        (persistent! rows)))))

;;; Storage

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

(def ^:private finish-states #{"DONE" "ERROR" "TIMEOUT"})

(defn- finish! [storage invocation-id state result-edn-str expired-only?]
  (when-not (finish-states state)
    (throw (IllegalArgumentException. (str "Not a state a STARTED invocation can end in: " (pr-str state)))))
  (= 1 (run1 storage :finish (str invocation-id) state (opt-arg result-edn-str) (if expired-only? "1" "0"))))

(defn- redis-now
  "The Redis server's clock, in epoch milliseconds."
  [storage]
  (let [[sec usec] (exec! storage [["TIME"]])]
    (+ (* 1000 (parse-long (->str (first sec)))) (quot (parse-long (->str (second sec))) 1000))))

(defn- reply-id [^bytes b] (->str b))

(extend-type RedisStorage
  db/Storage
  (init-db! [this]
    (load-scripts! this)
    this)

  (insert-workflow! [this wf-def params-edn-str parent-invocation-id metadata-edn-str timeout-ms]
    (reply-id (run1 this :insert (idempotence-key wf-def params-edn-str) wf-def params-edn-str
                      (opt-arg parent-invocation-id) (opt-arg metadata-edn-str) (opt-arg timeout-ms))))

  (finish! [this invocation-id state result-edn-str]
    (finish! this invocation-id state result-edn-str false))

  (time-out! [this invocation-id result-edn-str]
    (finish! this invocation-id "TIMEOUT" result-edn-str true))

  (restart! [this invocation-id metadata-edn-str timeout-ms]
    (reply-id (run1 this :restart (str invocation-id) (opt-arg metadata-edn-str) (opt-arg timeout-ms))))

  (record-reuse! [this invocation-id parent-invocation-id metadata-edn-str]
    (run1 this :reuse (str invocation-id) (opt-arg parent-invocation-id) (opt-arg metadata-edn-str))
    nil)

  (get-workflow [^RedisStorage this invocation-id]
    ;; a first invocation id is the key of its workflow; any other one asks the script
    (when-let [blob (or (first (exec! this [["GET" (str (.-prefix this) ":wf:" invocation-id)]]))
                        (run1 this :get (str invocation-id)))]
      (let [row (blob-row blob)
            lease-end (:workflow/expires-at row)]
        (assoc row :workflow/expired (boolean (and lease-end (<= lease-end (redis-now this))))))))

  (find-by-wf-def-and-params [this wf-def params-edn-str]
    (some-> (run1 this :find (idempotence-key wf-def params-edn-str)) blob-row))

  (top-level-workflows
    ([this]
     (db/top-level-workflows this nil))
    ([this {:keys [state wf-def top-level-only? limit before] :or {top-level-only? true}}]
     (calls->rows (run1 this :calls (opt-arg state) (opt-arg wf-def) (if top-level-only? "1" "0")
                        (opt-arg limit) (opt-arg before)))))

  (top-level-wf-defs [^RedisStorage this top-level-only?]
    (->> (first (exec! this [["SMEMBERS" (str (.-prefix this) (if top-level-only? ":defs:top" ":defs"))]]))
         (mapv ->str)
         sort
         vec))

  (full-timeline [this invocation-id]
    (timeline->rows (run1 this :timeline (str invocation-id))))

  db/Timeouts
  (extend-lease! [this invocation-ids timeout-ms]
    (if (empty? invocation-ids)
      #{}
      (let [done (->str (run1 this :extend-lease (str/join "," (map str invocation-ids)) (long timeout-ms)))]
        (into #{} (remove str/blank?) (str/split done #",")))))

  (list-expired [this]
    (mapv #(assoc % :workflow/expired true) (blobs->rows (run1 this :list-expired)))))

(defn datasource
  "A Storage and Timeouts over the Redis at uri (default redis://localhost:6379),
   with its own connection pool, from which up to senders (2) callers at a
   time send everyone's queued commands in a pipeline. All keys
   start with {prefix} (default \"tenon\"), so that several storages can share
   a server. Pass it to tenon.workflow.db/init-db! (which loads the scripts -
   Redis has no schema). Closeable: close it once it's no longer needed."
  ^java.io.Closeable
  ([] (datasource {}))
  ([{:keys [uri prefix senders] :or {uri "redis://localhost:6379" prefix "tenon" senders 2}}]
   ;; carmine memoizes pools by options: an :id makes this one ours alone
   (let [pool (conn/conn-pool {:id (random-uuid) :max-total-per-key (+ senders 2) :max-idle-per-key (+ senders 2)})]
     (->RedisStorage {:pool pool :spec {:uri uri}} (str "{" prefix "}") pool
                     (ConcurrentLinkedQueue.) (AtomicInteger. 0) senders (AtomicInteger. 0)))))

(defn flush!
  "Deletes every key of storage. For tests and benchmarks - there's no undo."
  [^RedisStorage storage]
  (let [pattern (str (str/replace (.-prefix storage) #"[\\*?\[\]]" #(str "\\" %)) ":*")]
    (loop [cursor "0"]
      (let [[next-cursor ks] (car/wcar (.-conn-opts storage) (car/scan cursor "MATCH" pattern "COUNT" 1000))]
        (when (seq ks)
          (car/wcar (.-conn-opts storage) (apply car/del ks)))
        (when-not (= "0" next-cursor)
          (recur next-cursor))))))
