(ns tenon.workflow
  (:require [clojure.edn :as edn]
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

(defn init
  "Builds the application-state map threaded through run-invocation,
   restart-invocation, list-pending, and tenon.workflow.http's
   wrap-handler/app: {:tenon/db ... :tenon/timeout-ms ...}.

   db-path names a SQLite file (creating it if needed); tenon.workflow.sqlite
   opens it (sqlite/datasource) and tenon.workflow.db/init-db! - a
   Storage protocol method - ensures its schema exists, before it's
   stored under :tenon/db. Additional key/value pairs override the
   defaults - most usefully :tenon/timeout-ms (see default-timeout-ms;
   nil never times out)."
  [db-path & {:as opts}]
  (merge {:tenon/db (db/init-db! (sqlite/datasource db-path))
          :tenon/timeout-ms default-timeout-ms}
         opts))

(def ^:dynamic *workflow-engine*
  "The current application-state map (as built by init), read by
   run-invocation/restart-invocation/list-pending when not given one
   explicitly. #workflow-tagged fns always call the implicit-engine
   arity, so binding this - typically once, via (alter-var-root!
   #'*workflow-engine* (constantly (init \"app.db\"))) at startup - is
   what makes them work at all. nil until then."
  nil)

(defn- safe-pr-str
  "Like pr-str, but ignores any caller-bound *print-length*/*print-level*
   so serialized values are always complete and readable back via edn/read-string,
   regardless of dynamic bindings in effect at the call site (e.g. from a REPL)."
  [x]
  (binding [*print-length* nil *print-level* nil]
    (pr-str x)))

(defn- serialize-exception [e]
  (safe-pr-str {:message (ex-message e) :class (.getName (class e)) :data (ex-data e)}))

(def ^:dynamic *invocation-stack*
  "Invocation ids of the #workflow invocations currently running on this
   thread, innermost (most recently started) first. Bound by
   execute-and-record! around the underlying fn's call, so a #workflow fn
   invoked from inside another #workflow fn sees its caller's invocation id
   as (first *invocation-stack*). Empty for a top-level invocation, so its
   parent-invocation-id is nil."
  '())

(def ^:dynamic *workflow-meta*
  "Arbitrary caller-supplied metadata (e.g. an actor id, span id, or trace
   id) to attach to the next #workflow invocation. run-invocation reads
   this once, at the moment it starts a new invocation (not on a dedup
   hit), serializes it, and stores it in that invocation's workflow row.
   restart-invocation stores it too when bound, otherwise the restarted
   invocation keeps the metadata of the previous one. Callers bind it
   around a call:

     (binding [engine/*workflow-meta* {:actor-id 42}]
       (charge-card customer-id amount))

   nil (the default) stores nothing."
  nil)

(defn- execute-and-record!
  "Runs raw-fn with args as invocation-id, which the caller has already
   recorded as STARTED, and records how it ended - unless it was timed out
   or restarted meanwhile, in which case that stays its recorded outcome,
   and only this caller gets the result (or exception)."
  [ds invocation-id raw-fn args]
  (let [result (try
                 (binding [*invocation-stack* (conj *invocation-stack* invocation-id)]
                   (apply raw-fn args))
                 (catch Throwable e
                   (db/finish! ds invocation-id "ERROR" (serialize-exception e))
                   (throw e)))]
    (db/finish! ds invocation-id "DONE" (safe-pr-str result))
    result))

(defn- time-out-if-expired!
  "Marks wf ERROR as timed out if it is STARTED past its lease. Returns true
   if it did."
  [ds {:keys [invocation_id state state_changed_at expires_at expired]}]
  (when (and (= "STARTED" state) expired)
    (let [timeout-ms (- expires_at state_changed_at)]
      (db/time-out! ds invocation_id
                    (safe-pr-str {:message (str "Timed out: still running " timeout-ms
                                                " ms after it started")
                                  :class (str `timed-out)
                                  :data {:type ::timed-out :timeout-ms timeout-ms}})))))

(defn- await-existing-result [ds invocation-id]
  (loop [wait-ms 5]
    (let [wf (db/get-workflow ds invocation-id)]
      (case (:state wf)
        "DONE" (edn/read-string (:result wf))
        "ERROR" (let [failure (edn/read-string (:result wf))]
                  (throw (ex-info (str "Workflow previously failed: " (:message failure))
                                   {:type ::previous-failure
                                    :invocation-id (:invocation_id wf)
                                    :failure failure})))
        "STARTED" (if (time-out-if-expired! ds wf)
                    (recur wait-ms)
                    (do (Thread/sleep (long wait-ms))
                        (recur (min 200 (* 2 wait-ms)))))))))

(defn run-invocation
  "Runs raw-fn with args, persisting the invocation.
   Returns raw-fn's result, or rethrows any exception."
  ([v raw-fn args]
   (run-invocation *workflow-engine* v raw-fn args))
  ([engine v raw-fn args]
   (let [ds (:tenon/db engine)
         timeout-ms (:tenon/timeout-ms engine default-timeout-ms)
         wf-def-str (str (symbol v))
         params-str (safe-pr-str (vec args))]
     (if-let [existing (db/find-by-wf-def-and-params ds wf-def-str params-str)]
       (await-existing-result ds (:invocation_id existing))
       (let [parent-id (first *invocation-stack*)
             meta-str (when (some? *workflow-meta*) (safe-pr-str *workflow-meta*))]
         (if-let [invocation-id (db/insert-workflow! ds wf-def-str params-str parent-id meta-str timeout-ms)]
           (execute-and-record! ds invocation-id raw-fn args)
           (if-let [existing (db/find-by-wf-def-and-params ds wf-def-str params-str)]
             (await-existing-result ds (:invocation_id existing))
             (throw (ex-info "Lost the race to insert this workflow's row, but no winning row was found"
                              {:type ::race-condition :wf-def wf-def-str})))))))))


(defn list-pending
  ([] (list-pending *workflow-engine*))
  ([engine] (db/top-level-workflows (:tenon/db engine) {:state "STARTED" :top-level-only? false})))


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
              (if (time-out-if-expired! ds wf)
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
           meta-str (if (some? *workflow-meta*) (safe-pr-str *workflow-meta*) (:metadata wf))]
       ;; A concurrent restart may have restarted it since wf was read -
       ;; restarting only if it is still DONE/ERROR under the same
       ;; invocation id lets only one of them through.
       (if-let [new-invocation-id (db/restart! ds (:invocation_id wf) meta-str
                                              (:tenon/timeout-ms engine default-timeout-ms))]
         (execute-and-record! ds new-invocation-id impl args)
         (throw (not-restartable (:state (db/get-workflow ds (:invocation_id wf))))))))))


(defn workflow
  "Tag reader for #workflow to be placed in front of (defn) forms.
   Registers the function into the tenon engine."
  [form]
  (when-not (and (list? form) (= 'defn (first form)))
    (throw (ex-info "#workflow only supports defn forms" {:form form})))
  (let [[_ fname & fdecl] form
        fdecl (if (string? (first fdecl)) (next fdecl) fdecl)
        fdecl (if (map? (first fdecl)) (next fdecl) fdecl)]
    `(do
       (declare ~fname)
       (let [raw-fn# (fn ~@fdecl)]
         (defn ~fname [& args#]
           (run-invocation (var ~fname) raw-fn# args#))
         (register! (var ~fname) raw-fn#)))))
