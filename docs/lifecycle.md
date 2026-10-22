# Engine lifecycle

`tenon.workflow/init` opens the database and starts the background threads that renew and sweep leases. It returns an engine that is a map and also `java.io.Closeable`. Close it once you are done:

```clojure
(with-open [engine (tenon.workflow/init "app.db")]
  (tenon.workflow/run-invocation engine #'my-wf my-wf-impl [42]))

; or by hand
(tenon.workflow/close! engine)
```

`close!` is idempotent and shuts down in this order:

1. New work is rejected: `run-invocation` and `restart-invocation` throw `:tenon.workflow/closed`. Workflows nested inside an invocation that is already running can still be called, so running work can finish.
2. It waits up to `:tenon/close-timeout-ms` (default `0`, so no wait) for the invocations running in this process to finish. Leases keep being renewed meanwhile.
3. The heartbeat and sweep threads stop.
4. The database is closed.

An invocation still running after that can't record its outcome, and its caller gets an exception. It stays `STARTED` until its lease runs out, then a sweep in any process moves it to `TIMEOUT`, the same as if the process had died. Pass e.g. `:tenon/close-timeout-ms 30000` to `init` to give running work a chance to finish on shutdown.

A copy made with `assoc` shares the database and threads of the original, so closing either one closes both. Only the code that called `init` should close the engine. Closing does not reset `*workflow-engine*`.

## With lifecycle libraries

The core has no dependency on any lifecycle library. Wiring the engine into one takes a few lines.

[Integrant](https://github.com/weavejester/integrant):

```clojure
(defmethod ig/init-key :tenon/engine [_ {:keys [db-path] :as opts}]
  (tenon.workflow/init db-path (dissoc opts :db-path)))

(defmethod ig/halt-key! :tenon/engine [_ engine]
  (tenon.workflow/close! engine))
```

[Component](https://github.com/stuartsierra/component):

```clojure
(defrecord WorkflowEngine [db-path opts engine]
  component/Lifecycle
  (start [this] (if engine this (assoc this :engine (tenon.workflow/init db-path opts))))
  (stop [this] (some-> engine tenon.workflow/close!) (assoc this :engine nil)))
```

[mount](https://github.com/tolitius/mount):

```clojure
(defstate engine
  :start (tenon.workflow/init "app.db")
  :stop (tenon.workflow/close! engine))
```

Since `#workflow` functions use `tenon.workflow/*workflow-engine*`, also set it once the engine starts, e.g. `(alter-var-root #'tenon.workflow/*workflow-engine* (constantly engine))`.
