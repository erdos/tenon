(ns tenon.workflow.timeout
  "Telling an invocation that is running from one that never will finish.

   A STARTED invocation holds a lease (:tenon/timeout-ms): it must be
   renewed before it runs out, or the invocation's process is taken to have
   died. Every process renews the leases of the invocations it runs - the
   heartbeat - and times out the leases that ran out, whoever's they were -
   the sweep, or any call that meets one (time-out-if-expired!). A timed out
   invocation is TIMEOUT: nothing restarts it, a person does.

   Depends on the storage only - tenon.workflow, which this serves, requires
   it."
  (:require [tenon.workflow.db :as db])
  (:import [java.util.concurrent RejectedExecutionException ScheduledExecutorService
            ScheduledThreadPoolExecutor ThreadFactory TimeUnit]))

(defn- emit! [engine event data]
  ((:tenon/on-event engine) event data))

(defn- pr-str-whole
  "pr-str, ignoring any caller-bound *print-length*/*print-level*."
  [x]
  (binding [*print-length* nil *print-level* nil]
    (pr-str x)))

(defn time-out-if-expired!
  "Moves wf - a row of the storage - to TIMEOUT if it is STARTED past its
   lease, emitting :timed-out. Returns true if it did, false or nil if it
   did not, e.g. because it was not expired or somebody else timed it out
   first."
  [{ds :tenon/db :as engine} {wf-name :workflow/name :workflow/keys [invocation-id parent-invocation-id state state-changed-at expires-at expired]}]
  (when (and (= "STARTED" state) expired)
    (let [timeout-ms (- expires-at state-changed-at)]
      (when (db/time-out! ds invocation-id
                          (pr-str-whole {:message (str "Timed out: still running " timeout-ms
                                                       " ms after it started")
                                         :class (str `timed-out)
                                         :data {:type ::timed-out :timeout-ms timeout-ms}}))
        (emit! engine :timed-out {:invocation-id invocation-id
                                  :parent-invocation-id parent-invocation-id
                                  :wf-def wf-name})
        true))))

(defn sweep!
  "Times out every STARTED invocation of any nesting whose lease has run out
   - e.g. because its process died - and returns their invocation ids,
   oldest first. Unlike a call of the same wf+params or a restart, which
   only time out the one invocation they meet, it needs nobody to touch
   them. It never restarts anything: they are TIMEOUT now, for a person to
   restart-invocation. Safe to run from any number of processes at once -
   timing out is a compare-and-set, so each invocation is timed out, and
   its :timed-out event emitted, by exactly one of them."
  [engine]
  (->> (db/list-expired (:tenon/db engine))
       (filter #(time-out-if-expired! engine %))
       (mapv :workflow/invocation-id)))

(defn- heartbeat!
  "One heartbeat of engine: renews the lease of every invocation running in
   this process (:tenon/in-flight), :tenon/lease-batch-size of them at a time, and
   emits :lease-lost for those found timed out or restarted by someone
   else. lost is the set of the invocation ids found so by the previous
   heartbeat, so each is reported once. Returns the set of those found so
   now - which only ever holds the ones still running."
  [{ds :tenon/db in-flight :tenon/in-flight timeout-ms :tenon/timeout-ms
    lease-batch-size :tenon/lease-batch-size :as engine} lost]
  (let [lost-ids (transduce (comp (partition-all lease-batch-size)
                                  (mapcat #(remove (db/extend-lease! ds % timeout-ms) %)))
                            conj #{} (keys @in-flight))]
    (run! #(emit! engine :lease-lost {:invocation-id %}) (remove lost lost-ids))
    lost-ids))

(defn- start-background!
  "Starts the daemon threads of engine and returns their executor: the
   heartbeat every heartbeat-ms (unless nil) and the sweep every sweep-ms
   (unless nil), the latter first right away - to time out what a previous
   run of this process left behind."
  ^ScheduledExecutorService [engine heartbeat-ms sweep-ms]
  (let [executor (doto (ScheduledThreadPoolExecutor.
                        2 (reify ThreadFactory
                            (newThread [_ r] (doto (Thread. ^Runnable r "tenon-background") (.setDaemon true)))))
                   ;; On shutdown, drop the runs still waiting for their time,
                   ;; but let the one already running finish its write.
                   (.setExecuteExistingDelayedTasksAfterShutdownPolicy false)
                   (.setContinueExistingPeriodicTasksAfterShutdownPolicy false))
        ;; Runs (step state) after delay-ms, then again after period-ms with the
        ;; state it returned (or the same, if it threw).
        schedule! (fn schedule! [error-event delay-ms period-ms step state]
                    (.schedule executor
                               ^Runnable (fn []
                                           (let [state (try (step state)
                                                            (catch Throwable e
                                                              (try (emit! engine error-event {:error e})
                                                                   (catch Throwable _))
                                                              state))]
                                             (try (schedule! error-event period-ms period-ms step state)
                                                  (catch RejectedExecutionException _))))
                               (long delay-ms) TimeUnit/MILLISECONDS))]
    (when heartbeat-ms
      (schedule! :heartbeat-error heartbeat-ms heartbeat-ms #(heartbeat! engine %) #{}))
    (when sweep-ms
      (schedule! :sweep-error 0 sweep-ms (fn [_] (sweep! engine) nil) nil))
    executor))

(defn start!
  "Starts the daemon threads that keep the leases of engine - the map built
   by tenon.workflow/init - honest, and returns it with the
   :tenon/heartbeat-ms and :tenon/sweep-ms they run at, and the
   :tenon/background executor to stop! them with:

   - Every :tenon/heartbeat-ms (by default a third of :tenon/timeout-ms;
     nil: no heartbeat, and none when :tenon/timeout-ms is nil) the
     heartbeat renews the lease of the invocations running in this
     process, so a long workflow is not timed out as long as its process
     lives.
   - Right away and then every :tenon/sweep-ms (by default :tenon/timeout-ms;
     nil: no sweep) the sweep times out the invocations whose lease ran
     out, whichever process ran them - see sweep!. It does not restart
     them."
  [{:tenon/keys [timeout-ms in-flight] :as engine}]
  (let [default-ms (fn [k default]
                     (if (contains? engine k) (get engine k) default))
        heartbeat-ms (default-ms :tenon/heartbeat-ms (some-> timeout-ms (quot 3) (max 1)))
        sweep-ms (default-ms :tenon/sweep-ms timeout-ms)
        engine (assoc engine :tenon/heartbeat-ms heartbeat-ms :tenon/sweep-ms sweep-ms)
        heartbeat-ms (when (and timeout-ms in-flight) heartbeat-ms)]
    (cond-> engine
      (or heartbeat-ms sweep-ms)
      (assoc :tenon/background (start-background! engine heartbeat-ms sweep-ms)))))

(def ^:private stop-wait-ms
  "How long stop! waits for a heartbeat or sweep already running - a single
   write, bounded by SQLite's 5 s busy timeout - before interrupting it."
  10000)

(defn stop!
  "Stops the threads start! started for engine: no new heartbeat or sweep
   runs, and the one running, if any, may finish (for up to 10 s, then it
   is interrupted). Idempotent."
  [engine]
  (when-let [^ScheduledExecutorService executor (:tenon/background engine)]
    (.shutdown executor)
    (when-not (.awaitTermination executor stop-wait-ms TimeUnit/MILLISECONDS)
      (.shutdownNow executor)))
  nil)
