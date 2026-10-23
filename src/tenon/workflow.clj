(ns tenon.workflow
  (:require [clojure.edn :as edn]
            [clojure.walk :as walk]
            [tenon.workflow.db :as db]
            [tenon.workflow.sqlite :as sqlite]
            [tenon.workflow.timeout :as timeout])
  (:import [java.io Closeable]))

(defn register! [v raw-fn]
  (doto v (alter-meta! assoc ::raw-fn raw-fn)))

(defn raw-fn [v] (::raw-fn (meta v)))

(defn lookup
  "The var named by the namespace-qualified symbol sym, if it exists (its
   namespace must already be loaded) and was registered via register! -
   i.e. it carries raw-fn metadata. Returns nil otherwise - including when
   sym's namespace was never loaded (find-var itself throws in that case,
   which lookup must not do for an ordinary miss)."
  [sym]
  (let [v (when (find-ns (symbol (namespace sym)))
            (find-var sym))]
    (when (and v (raw-fn v))
      v)))

(defmacro ^:private emit! [engine event data]
  `((:tenon/on-event ~engine) ~event ~data))

(def ^:private local-host-name
  (delay (.getHostName (java.net.InetAddress/getLocalHost))))

(defn empty-metadata-fn [_wf-type _args] {})

(defn host-thread-metadata-fn [_wf-type _args]
  {:host @local-host-name
   :thread (.getName (Thread/currentThread))})

(defn init
  "Builds the application-state map threaded through run-invocation,
   restart-invocation, list-pending, and tenon.workflow.http's
   wrap-handler/app: {:tenon/db ... :tenon/timeout-ms ... :tenon/in-flight ... :tenon/on-event ... :tenon/metadata-fn ...}.

   db-path names a SQLite file (creating it if needed); tenon.workflow.sqlite
   opens it (sqlite/datasource) and tenon.workflow.db/init-db! - a
   Storage protocol method - ensures its schema exists, before it's
   stored under :tenon/db. Additional key/value pairs override the
   defaults - most usefully :tenon/timeout-ms (the lease of a running
   invocation, 15000 ms: how long it may stay STARTED without its process
   renewing it, before the next call of the same wf+params, a restart or a
   sweep moves it to TIMEOUT; nil never times out) and
   :tenon/on-event (a 2-arg fn (fn [event data]), never nil - does nothing by default,
   called on :started, :restarted, :done, :error, :reused, :timed-out,
   :lease-lost - a running invocation found timed out or restarted by
   someone else - and :heartbeat-error and :sweep-error) and
   :tenon/metadata-fn (a 2-arg fn (fn [wf-type args]) returning the
   metadata to persist - empty-metadata-fn by default; see also
   host-thread-metadata-fn).

   Unless :tenon/timeout-ms is nil, daemon threads renew the leases of the
   invocations running in this process and time out the expired ones of
   any process - see tenon.workflow.timeout/start! for :tenon/heartbeat-ms
   and :tenon/sweep-ms, and :tenon/lease-batch-size (500) for how many
   invocations one renewal covers.

   A caller waiting for an invocation another one is running polls it
   with exponential backoff: :tenon/backoff-base-ms (5) for the first wait,
   times :tenon/backoff-multiplier (2) for each next, up to
   :tenon/backoff-max-ms (200), each lengthened by a random amount of up
   to :tenon/backoff-jitter (0.1) of it, so callers polling together
   drift apart.

   Stop the threads with close!."
  [db-path & {:as opts}]
  (-> {:tenon/db (db/init-db! (sqlite/datasource db-path))
       :tenon/timeout-ms 15000
       :tenon/lease-batch-size 500
       :tenon/backoff-base-ms 5
       :tenon/backoff-multiplier 2
       :tenon/backoff-max-ms 200
       :tenon/backoff-jitter 0.1
       :tenon/in-flight (atom {})
       :tenon/on-event (fn [_event _data])
       :tenon/metadata-fn empty-metadata-fn}
      (merge opts)
      (timeout/start!)))

(defn close!
  "Stops the background threads of engine, as built by init, and closes its
   database."
  [engine]
  (timeout/stop! engine)
  (.close ^Closeable (:tenon/db engine)))

(def ^:dynamic *workflow-engine*
  "The current application-state map (as built by init), read by
   run-invocation/restart-invocation/list-pending when not given one
   explicitly. #workflow-tagged fns always call the implicit-engine
   arity, so binding this - typically once, via (alter-var-root!
   #'*workflow-engine* (constantly (init \"app.db\"))) at startup - is
   what makes them work at all. nil until then."
  nil)

(defn- canonical-compare [a b]
  (if (= a b) 0 (compare (pr-str a) (pr-str b))))

(defn- safe-pr-str
  "Like pr-str, but ignores any caller-bound *print-length*/*print-level*
   and canonicalizes maps and sets into a deterministic order."
  [x]
  (binding [*print-length* nil *print-level* nil]
    (pr-str (walk/postwalk
             (fn [v]
               (cond
                 (and (map? v) (not (record? v))) (into (sorted-map-by canonical-compare) v)
                 (set? v) (into (sorted-set-by canonical-compare) v)
                 :else v))
             x))))

(defn- serialize-exception [e]
  (safe-pr-str (Throwable->map e)))

(def ^:dynamic *invocation-stack*
  "Invocation ids of the #workflow invocations currently running on this
   thread, innermost (most recently started) first. Bound by
   execute-and-record! around the underlying fn's call, so a #workflow fn
   invoked from inside another #workflow fn sees its caller's invocation id
   as (first *invocation-stack*). Empty for a top-level invocation, so its
   parent-invocation-id is nil."
  '())

(defn- metadata-for
  [{metadata-fn :tenon/metadata-fn} wf-type args]
  (metadata-fn wf-type (vec args)))

(defn- execute-and-record!
  "Runs raw-fn with args as invocation-id, which the caller has already
   recorded as STARTED, and records how it ended - unless it was timed out
   or restarted meanwhile, in which case that stays its recorded outcome,
   and only this caller gets the result (or exception)."
  [{ds :tenon/db in-flight :tenon/in-flight :as engine} invocation-id wf-def-str raw-fn args]
  (let [done (promise)
        parent-id (first *invocation-stack*)
        args-vec (vec args)]
    (when in-flight (swap! in-flight assoc invocation-id done))
    (try
      (let [result (try
                     (binding [*invocation-stack* (conj *invocation-stack* invocation-id)]
                       (apply raw-fn args))
                     (catch Throwable e
                       (db/finish! ds invocation-id "ERROR" (serialize-exception e))
                       (emit! engine :error {:invocation-id invocation-id
                                             :parent-invocation-id parent-id
                                             :wf-def wf-def-str
                                             :args args-vec
                                             :error e})
                       (throw e)))]
        (db/finish! ds invocation-id "DONE" (safe-pr-str result))
        (emit! engine :done {:invocation-id invocation-id
                             :parent-invocation-id parent-id
                             :wf-def wf-def-str
                             :args args-vec
                             :result result})
        result)
      (finally
        (when in-flight
          (swap! in-flight dissoc invocation-id)
          (deliver done true))))))

(defn started? [wf] (= "STARTED" (:workflow/state wf)))
(defn done? [wf] (= "DONE" (:workflow/state wf)))
(defn error? [wf] (= "ERROR" (:workflow/state wf)))
(defn reused? [wf] (= "REUSED" (:workflow/state wf)))
(defn timeout? [wf] (= "TIMEOUT" (:workflow/state wf)))
(defn expired? [wf] (boolean (:workflow/expired wf)))
(def failed? (some-fn error? timeout?))
(def finished? (some-fn done? failed?))

(defn- backoff-waits [{:tenon/keys [backoff-base-ms backoff-multiplier backoff-max-ms backoff-jitter]}]
  (->> (iterate #(min backoff-max-ms (* backoff-multiplier %)) backoff-base-ms)
       (map #(+ % (long (rand (* backoff-jitter %)))))))

(defn- with-backoff
  ([engine f] (with-backoff engine f nil))
  ([engine f wake]
   (loop [[wait-ms & waits] (backoff-waits engine)]
     (if-some [result (f)]
       result
       (do (if-let [p (and wake (wake))]
             (deref p wait-ms nil)
             (Thread/sleep (long wait-ms)))
           (recur waits))))))

(defn- await-finished
  "Polls the workflow invocation-id belongs to until it is DONE, ERROR or
   TIMEOUT (timing it out if its lease runs out meanwhile), and returns its row.
   When the invocation is running in this JVM (:tenon/in-flight), waits on
   its completion promise instead of sleeping so same-JVM callers wake as
   soon as it finishes."
  [{ds :tenon/db in-flight :tenon/in-flight :as engine} invocation-id]
  (with-backoff engine
    (fn []
      (let [wf (db/get-workflow ds invocation-id)]
        (cond
          (finished? wf) wf
          (timeout/time-out-if-expired! engine wf) (db/get-workflow ds invocation-id))))
    #(some-> in-flight deref (get (:workflow/invocation-id (db/get-workflow ds invocation-id))))))

(defn- reuse-existing-result!
  "Awaits the invocation-id run started by someone else, logs that the
   current caller - under parent-id with metadata - reused it, then returns
   its result, or throws describing its failure."
  [{ds :tenon/db :as engine} invocation-id parent-id metadata]
  (when (some #{invocation-id} *invocation-stack*)
    (throw (ex-info "Cycle detected: workflow is already running on the current call stack"
                    {:type ::cycle-detected
                     :invocation-id invocation-id
                     :invocation-stack *invocation-stack*})))
  (let [wf (await-finished engine invocation-id)]
    (db/record-reuse! ds (:workflow/invocation-id wf) parent-id (some-> metadata safe-pr-str))
    (emit! engine :reused {:invocation-id (:workflow/invocation-id wf)
                           :parent-invocation-id parent-id
                           :wf-def (:workflow/name wf)
                           :state (:workflow/state wf)
                           :metadata metadata})
    (cond
      (done? wf) (edn/read-string (:workflow/result wf))
      (failed? wf) (let [failure (edn/read-string (:workflow/result wf))]
                     (throw (ex-info (str "Workflow previously " (if (timeout? wf) "timed out" "failed") ": "
                                          (or (:cause failure) (:message failure)))
                                     {:type ::previous-failure
                                      :invocation-id (:workflow/invocation-id wf)
                                      :state (:workflow/state wf)
                                      :failure failure}))))))

(defn run-invocation
  "Runs raw-fn with args, persisting the invocation.
   Returns raw-fn's result, or rethrows any exception."
  ([v raw-fn args]
   (run-invocation *workflow-engine* v raw-fn args))
  ([engine v raw-fn args]
   (when-not engine
     (throw (ex-info "Dynamic var *workflow-engine* is not bound."
                     {:type ::precondition-failed})))
   (let [ds (:tenon/db engine)
         timeout-ms (:tenon/timeout-ms engine)
         wf-def-str (str (symbol v))
         args-vec (vec args)
         params-str (safe-pr-str args-vec)
         parent-id (first *invocation-stack*)
         metadata (metadata-for engine (symbol v) args-vec)
         meta-str (some-> metadata safe-pr-str)]
     (if-let [existing (db/find-by-wf-def-and-params ds wf-def-str params-str)]
       (reuse-existing-result! engine (:workflow/invocation-id existing) parent-id metadata)
       (if-let [invocation-id (db/insert-workflow! ds wf-def-str params-str parent-id meta-str timeout-ms)]
         (do (emit! engine :started {:invocation-id invocation-id
                                     :parent-invocation-id parent-id
                                     :wf-def wf-def-str
                                     :args args-vec
                                     :metadata metadata})
             (execute-and-record! engine invocation-id wf-def-str raw-fn args))
         (if-let [existing (db/find-by-wf-def-and-params ds wf-def-str params-str)]
           (reuse-existing-result! engine (:workflow/invocation-id existing) parent-id metadata)
           (throw (ex-info "Lost the race to insert this workflow's row, but no winning row was found"
                            {:type ::race-condition :wf-def wf-def-str}))))))))


(defn sweep!
  ([] (sweep! *workflow-engine*))
  ([engine] (timeout/sweep! engine)))

(defn list-pending
  ([] (list-pending *workflow-engine*))
  ([engine] (->> (db/top-level-workflows (:tenon/db engine) {:state "STARTED" :top-level-only? false})
                 (remove :event/reused)
                 (vec))))

(defn get-invocation
  "The workflow invocation-id belongs to, or nil. invocation-id may also
   be a past invocation id of a since restarted workflow - the returned
   :workflow/invocation-id is its current one then. Returns a map of:

     :workflow/invocation-id        long - the current invocation id
     :workflow/name               string - the workflow fn's qualified symbol
     :workflow/params               string - EDN vector of the args
     :workflow/parent-invocation-id long - the invocation it ran nested under
                                    (as of when it first started), nil if
                                    top-level
     :workflow/state                string - \"STARTED\", \"DONE\", \"ERROR\" or
                                     \"TIMEOUT\"
     :workflow/state-changed-at     long - epoch ms of entering state
     :workflow/expires-at           long - epoch ms its STARTED lease ends, nil
                                    if not STARTED or never expiring
     :workflow/metadata             string - EDN of the :tenon/metadata-fn
                                    result when started, or nil
     :workflow/result               string - EDN of the return value (DONE) or
                                    of the failure's Throwable->map (ERROR) or of
                                     the timeout's description (TIMEOUT),
                                    nil while STARTED
     :workflow/expired              boolean - whether the STARTED lease has
                                    run out"
  ([invocation-id] (get-invocation *workflow-engine* invocation-id))
  ([engine invocation-id] (db/get-workflow (:tenon/db engine) invocation-id)))

(defn list-invocations
  "Calls of workflows matching filters, newest first - each workflow's
   first run plus every later call that reused its stored result. filters
   is a map of {:state s :wf-def w :top-level-only? t :limit n :before call-id}
   - see tenon.workflow.db/top-level-workflows. Returns a vector of maps
   with the keys of get-invocation except :workflow/expired
   (:workflow/parent-invocation-id being the caller's), plus:

     :event/call-id    long - identifies the call, the :before cursor
     :event/created-at long - epoch ms of the call
     :event/reused     boolean - whether the call reused a stored result"
  ([filters] (list-invocations *workflow-engine* filters))
  ([engine filters] (db/top-level-workflows (:tenon/db engine) filters)))

(defn list-wf-defs
  "Vector of the distinct wf_def strings (qualified fn symbols),
   alphabetical - only those of top-level workflows if top-level-only?."
  ([top-level-only?] (list-wf-defs *workflow-engine* top-level-only?))
  ([engine top-level-only?] (db/top-level-wf-defs (:tenon/db engine) top-level-only?)))

(defn full-timeline
  "Every state - past and current - of the workflow invocation-id belongs
   to and of all its sub-workflows at any depth, plus a REUSED state for
   each stored result any of them reused (not the reused workflow's own
   states - it didn't run under them), as a vector of maps in
   chronological order:

     :workflow/invocation-id         long - the invocation the state was
                                     recorded under
     :workflow/current-invocation-id long - the workflow's current invocation id
     :workflow/parent-invocation-id  long - the caller's invocation id (for
                                     REUSED, the reusing caller's), nil at the
                                     top level
     :workflow/name                string - the workflow fn's qualified symbol
     :workflow/state                 string - \"STARTED\", \"DONE\", \"ERROR\",
                                      \"TIMEOUT\" or \"REUSED\" (a caller got its stored outcome)
     :workflow/state-changed-at      long - epoch ms of entering state
     :event/data                  string - EDN of the metadata (STARTED,
                                     REUSED) or of the result (DONE, ERROR, TIMEOUT),
                                     or nil
     :workflow/depth                 long - 0 for the workflow itself, 1 for a
                                     direct sub-workflow, and so on"
  ([invocation-id] (full-timeline *workflow-engine* invocation-id))
  ([engine invocation-id] (db/full-timeline (:tenon/db engine) invocation-id)))


(defn restart-invocation
  "Restarts the workflow invocation-id belongs to (any of its current or
   past invocation ids) under a new invocation id, keeping its history.
   Requires it to be DONE, ERROR or TIMEOUT (or STARTED past its lease - see
   :tenon/timeout-ms in init), and its wf_def to currently be registered
   in this process. engine defaults to *workflow-engine*, same as
   run-invocation."
  ([invocation-id]
   (restart-invocation *workflow-engine* invocation-id))
  ([engine invocation-id]
   (let [ds (:tenon/db engine)
         wf (let [wf (or (db/get-workflow ds invocation-id)
                         (throw (ex-info "No such workflow"
                                          {:type ::precondition-failed :invocation-id invocation-id})))]
              (if (timeout/time-out-if-expired! engine wf)
                (db/get-workflow ds (:workflow/invocation-id wf))
                wf))
         not-restartable (fn [state]
                           (ex-info "Cannot restart: workflow is not DONE, ERROR or TIMEOUT"
                                    {:type ::precondition-failed
                                     :invocation-id invocation-id
                                     :state state}))]
     (when-not (finished? wf)
       (throw (not-restartable (:workflow/state wf))))
     (let [wf-def-sym (symbol (:workflow/name wf))
           v (or (lookup wf-def-sym)
                 (throw (ex-info "wf_def not registered in this process"
                                  {:type ::precondition-failed
                                   :wf-def (str wf-def-sym)})))
           impl (raw-fn v)
           args (edn/read-string (:workflow/params wf))
           metadata (metadata-for engine wf-def-sym args)
           meta-str (some-> metadata safe-pr-str)]
       ;; A concurrent restart may have restarted it since wf was read -
       ;; restarting only if it is still DONE/ERROR/TIMEOUT under the same
       ;; invocation id lets only one of them through.
       (if-let [new-invocation-id (db/restart! ds (:workflow/invocation-id wf) meta-str
                                              (:tenon/timeout-ms engine))]
         (do (emit! engine :restarted {:invocation-id new-invocation-id
                                       :previous-invocation-id (:workflow/invocation-id wf)
                                       :parent-invocation-id (:workflow/parent-invocation-id wf)
                                       :wf-def (:workflow/name wf)
                                       :args args
                                       :metadata metadata})
             (execute-and-record! engine new-invocation-id (:workflow/name wf) impl args))
         (throw (not-restartable (:workflow/state (db/get-workflow ds (:workflow/invocation-id wf))))))))))

(defn restart-invocation-recursively
  ([invocation-id]
   (restart-invocation-recursively *workflow-engine* invocation-id))
  ([{ds :tenon/db :as engine} invocation-id]
   ;; A failed (or timed out) workflow stopped at its first failing call, so at most one
   ;; callee - a reused one included - is failed, and only one path of the
   ;; hierarchy leads to it.
   (some->> (db/full-timeline ds invocation-id)
            (filter (comp #{1} :workflow/depth))
            (map :workflow/current-invocation-id)
            distinct
            (map (partial db/get-workflow ds))
            (some (fn [callee-wf]
                    (when (or (failed? callee-wf)
                              (timeout/time-out-if-expired! engine callee-wf))
                      (:workflow/invocation-id callee-wf))))
            (restart-invocation-recursively engine))
   (restart-invocation engine invocation-id)))


(defn workflow
  "Tag reader for #workflow to be placed in front of (defn) forms.
   Registers the function into the tenon engine."
  [form]
  (when-not (and (list? form) (= 'defn (first form)))
    (throw (ex-info "#workflow only supports defn forms" {:form form})))
  (let [[_ fname fn-form] (macroexpand-1 form)]
    `(do
       (declare ~fname)
       (let [raw-fn# ~fn-form]
         (def ~fname
           (fn [& args#]
             (run-invocation (var ~fname) raw-fn# args#)))
         (register! (var ~fname) raw-fn#)))))

(defmacro run-task
  "Runs expr as a workflow invocation, capturing the locals of the
   surrounding code that expr uses and passing them as the invocation's
   arguments. expr is wrapped into a fn of those locals and registered under
   a var named after the hash of expr in the current namespace, so the
   invocation is audited, deduplicated by the captured values, and
   restartable (restart-invocation) just like a #workflow defn call - as
   long as the form has been evaluated in the restarting process, and the
   captured values print and read back as EDN. Returns expr's value."
  [expr]
  (let [captured (->> (tree-seq coll? seq expr)
                      (filter symbol?)
                      (filter #(contains? &env %))
                      distinct
                      vec)
        task-name (symbol (format "task-%08x" (hash (pr-str expr))))]
    `(do
       (declare ~task-name)
       (let [raw-fn# (fn ~captured ~expr)]
         (def ~task-name
           (fn [& args#]
             (run-invocation (var ~task-name) raw-fn# args#)))
         (register! (var ~task-name) raw-fn#))
       (~task-name ~@captured))))

(defn task
  "Tag reader for #task to be placed in front of an expression. Expands
   into a run-task call, which makes the expression a workflow."
  [form]
  `(run-task ~form))
