(ns tenon.workflow
  (:require [clojure.edn :as edn]
            [clojure.walk :as walk]
            [tenon.workflow.db :as db]
            [tenon.workflow.sqlite :as sqlite]))

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

(def default-timeout-ms
  "Default :tenon/timeout-ms for init - how long after it started an
   invocation may stay STARTED. Past that, the next call of the same
   wf+params (or a restart-invocation) marks it ERROR as timed out -
   e.g. when the process running it died and it will never finish."
  30000)

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
   defaults - most usefully :tenon/timeout-ms (see default-timeout-ms;
   nil never times out) and :tenon/on-event (a 2-arg fn (fn [event data])
   called on :started, :restarted, :done, :error, :reused, and :timed-out)
   and :tenon/metadata-fn (a 2-arg fn (fn [wf-type args]) returning the
   metadata to persist - empty-metadata-fn by default; see also host-thread-metadata-fn)."
  [db-path & {:as opts}]
  (merge {:tenon/db (db/init-db! (sqlite/datasource db-path))
          :tenon/timeout-ms default-timeout-ms
          :tenon/in-flight (atom {})
          :tenon/on-event nil
          :tenon/metadata-fn empty-metadata-fn}
         opts))

(defmacro ^:private emit! [engine event data]
  `(when-let [on-event# (:tenon/on-event ~engine)]
     (on-event# ~event ~data)))

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

(defn- time-out-if-expired!
  "Marks wf ERROR as timed out if it is STARTED past its lease. Returns true
   if it did."
  [{ds :tenon/db :as engine} {:keys [invocation_id wf_def parent_invocation_id state state_changed_at expires_at expired]}]
  (when (and (= "STARTED" state) expired)
    (let [timeout-ms (- expires_at state_changed_at)]
      (when (db/time-out! ds invocation_id
                          (safe-pr-str {:message (str "Timed out: still running " timeout-ms
                                                      " ms after it started")
                                        :class (str `timed-out)
                                        :data {:type ::timed-out :timeout-ms timeout-ms}}))
        (emit! engine :timed-out {:invocation-id invocation_id
                                  :parent-invocation-id parent_invocation_id
                                  :wf-def wf_def})
        true))))

(defn- await-finished
  "Polls the workflow invocation-id belongs to until it is DONE or ERROR
   (timing it out if its lease runs out meanwhile), and returns its row.
   When the invocation is running in this JVM (:tenon/in-flight), waits on
   its completion promise instead of sleeping so same-JVM callers wake as
   soon as it finishes."
  [{ds :tenon/db in-flight :tenon/in-flight :as engine} invocation-id]
  (loop [wait-ms 5]
    (let [wf (db/get-workflow ds invocation-id)]
      (case (:state wf)
        ("DONE" "ERROR") wf
        "STARTED" (if (time-out-if-expired! engine wf)
                    (recur wait-ms)
                    (do (if-let [p (some-> in-flight deref (get (:invocation_id wf)))]
                          (deref p (long wait-ms) nil)
                          (Thread/sleep (long wait-ms)))
                        (recur (min 200 (* 2 wait-ms)))))))))

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
    (db/record-reuse! ds (:invocation_id wf) parent-id (some-> metadata safe-pr-str))
    (emit! engine :reused {:invocation-id (:invocation_id wf)
                           :parent-invocation-id parent-id
                           :wf-def (:wf_def wf)
                           :state (:state wf)
                           :metadata metadata})
    (case (:state wf)
      "DONE" (edn/read-string (:result wf))
      "ERROR" (let [failure (edn/read-string (:result wf))]
                (throw (ex-info (str "Workflow previously failed: " (or (:cause failure) (:message failure)))
                                {:type ::previous-failure
                                 :invocation-id (:invocation_id wf)
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
         timeout-ms (:tenon/timeout-ms engine default-timeout-ms)
         wf-def-str (str (symbol v))
         args-vec (vec args)
         params-str (safe-pr-str args-vec)
         parent-id (first *invocation-stack*)
         metadata (metadata-for engine (symbol v) args-vec)
         meta-str (some-> metadata safe-pr-str)]
     (if-let [existing (db/find-by-wf-def-and-params ds wf-def-str params-str)]
       (reuse-existing-result! engine (:invocation_id existing) parent-id metadata)
       (if-let [invocation-id (db/insert-workflow! ds wf-def-str params-str parent-id meta-str timeout-ms)]
         (do (emit! engine :started {:invocation-id invocation-id
                                     :parent-invocation-id parent-id
                                     :wf-def wf-def-str
                                     :args args-vec
                                     :metadata metadata})
             (execute-and-record! engine invocation-id wf-def-str raw-fn args))
         (if-let [existing (db/find-by-wf-def-and-params ds wf-def-str params-str)]
           (reuse-existing-result! engine (:invocation_id existing) parent-id metadata)
           (throw (ex-info "Lost the race to insert this workflow's row, but no winning row was found"
                            {:type ::race-condition :wf-def wf-def-str}))))))))


(defn list-pending
  ([] (list-pending *workflow-engine*))
  ([engine] (->> (db/top-level-workflows (:tenon/db engine) {:state "STARTED" :top-level-only? false})
                 (remove :reused)
                 (vec))))

(defn get-invocation
  "The workflow invocation-id belongs to, or nil. invocation-id may also
   be a past invocation id of a since restarted workflow - the returned
   :invocation_id is its current one then. Returns a map of:

     :invocation_id        long - the current invocation id
     :wf_def               string - the workflow fn's qualified symbol
     :params               string - EDN vector of the args
     :parent_invocation_id long - the invocation it ran nested under
                           (as of when it first started), nil if top-level
     :state                string - \"STARTED\", \"DONE\" or \"ERROR\"
     :state_changed_at     long - epoch ms of entering state
     :expires_at           long - epoch ms its STARTED lease ends, nil if
                           not STARTED or never expiring
     :metadata             string - EDN of the :tenon/metadata-fn result when
                           started, or nil
     :result               string - EDN of the return value (DONE) or of
                           the failure's Throwable->map (ERROR),
                           nil while STARTED
     :expired              boolean - whether the STARTED lease has run out"
  ([invocation-id] (get-invocation *workflow-engine* invocation-id))
  ([engine invocation-id] (db/get-workflow (:tenon/db engine) invocation-id)))

(defn list-invocations
  "Calls of workflows matching filters, newest first - each workflow's
   first run plus every later call that reused its stored result. filters
   is a map of {:state s :wf-def w :top-level-only? t :limit n :before call-id}
   - see tenon.workflow.db/top-level-workflows. Returns a vector of maps
   with the keys of get-invocation except :expired (:parent_invocation_id
   being the caller's), plus:

     :call_id    long - identifies the call, the :before cursor
     :created_at long - epoch ms of the call
     :reused     boolean - whether the call reused a stored result"
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

     :invocation_id         long - the invocation the state was recorded under
     :current_invocation_id long - the workflow's current invocation id
     :parent_invocation_id  long - the caller's invocation id (for REUSED,
                            the reusing caller's), nil at the top level
     :wf_def                string - the workflow fn's qualified symbol
     :state                 string - \"STARTED\", \"DONE\", \"ERROR\" or
                            \"REUSED\" (a caller got its stored outcome)
     :state_changed_at      long - epoch ms of entering state
     :data                  string - EDN of the metadata (STARTED, REUSED)
                            or of the result (DONE, ERROR), or nil
     :depth                 long - 0 for the workflow itself, 1 for a
                            direct sub-workflow, and so on"
  ([invocation-id] (full-timeline *workflow-engine* invocation-id))
  ([engine invocation-id] (db/full-timeline (:tenon/db engine) invocation-id)))


(defn restart-invocation
  "Restarts the workflow invocation-id belongs to (any of its current or
   past invocation ids) under a new invocation id, keeping its history.
   Requires it to be DONE or ERROR (or STARTED but timed out - see
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
              (if (time-out-if-expired! engine wf)
                (db/get-workflow ds (:invocation_id wf))
                wf))
         not-restartable (fn [state]
                           (ex-info "Cannot restart: workflow is not DONE or ERROR"
                                    {:type ::precondition-failed
                                     :invocation-id invocation-id
                                     :state state}))]
     (when-not (contains? #{"DONE" "ERROR"} (:state wf))
       (throw (not-restartable (:state wf))))
     (let [wf-def-sym (symbol (:wf_def wf))
           v (or (lookup wf-def-sym)
                 (throw (ex-info "wf_def not registered in this process"
                                  {:type ::precondition-failed
                                   :wf-def (str wf-def-sym)})))
           impl (raw-fn v)
           args (edn/read-string (:params wf))
           metadata (metadata-for engine wf-def-sym args)
           meta-str (some-> metadata safe-pr-str)]
       ;; A concurrent restart may have restarted it since wf was read -
       ;; restarting only if it is still DONE/ERROR under the same
       ;; invocation id lets only one of them through.
       (if-let [new-invocation-id (db/restart! ds (:invocation_id wf) meta-str
                                              (:tenon/timeout-ms engine default-timeout-ms))]
         (do (emit! engine :restarted {:invocation-id new-invocation-id
                                       :previous-invocation-id (:invocation_id wf)
                                       :parent-invocation-id (:parent_invocation_id wf)
                                       :wf-def (:wf_def wf)
                                       :args args
                                       :metadata metadata})
             (execute-and-record! engine new-invocation-id (:wf_def wf) impl args))
         (throw (not-restartable (:state (db/get-workflow ds (:invocation_id wf))))))))))

(defn restart-invocation-recursively
  ([invocation-id]
   (restart-invocation-recursively *workflow-engine* invocation-id))
  ([{ds :tenon/db :as engine} invocation-id]
   ;; A failed workflow stopped at its first failing call, so at most one
   ;; callee - a reused one included - is failed, and only one path of the
   ;; hierarchy leads to it.
   (some->> (db/full-timeline ds invocation-id)
            (filter (comp #{1} :depth))
            (map :current_invocation_id)
            distinct
            (map (partial db/get-workflow ds))
            (some (fn [callee-wf]
                    (when (or (= "ERROR" (:state callee-wf))
                              (time-out-if-expired! engine callee-wf))
                      (:invocation_id callee-wf))))
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
