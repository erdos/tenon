(ns tenon.workflow-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.edn :as edn]
            [next.jdbc :as jdbc]
            [tenon.workflow :as engine]
            [tenon.workflow.db :as db]
            [tenon.workflow.test-util :as test-util :refer [temp-db-fixture]]))

(use-fixtures :each temp-db-fixture)

(def ^:private add-fn (fn [a b] (+ a b)))
(def ^:private boom-fn (fn [msg] (throw (ex-info msg {}))))
(def ^:private finished-fn (fn [] :ok))
(def ^:private flaky-fn (fn [n] (if (< n 1) (throw (ex-info "fail" {})) :recovered)))
(def ^:private long-args-fn (fn [xs] xs))
(def ^:private done-record-fails-fn (fn [] :ok))
(def ^:private nested-inner-fn (fn [n] (* n 2)))
(def ^:private nested-outer-fn (fn [n] (+ 1 (engine/run-invocation #'nested-inner-fn nested-inner-fn [n]))))
(def ^:private echo-fn (fn [x] x))
(def ^:private unregistered-fn (fn [x] x))
(def ^:private transactional-fn (fn [a b] (+ a b)))
(def ^:private dedup-fn (fn [x] (* x 10)))
(def ^:private dedup-error-fn (fn [msg] (throw (ex-info msg {}))))
(def ^:private dedup-started-fn (fn [n] n))
(def ^:private meta-fn (fn [x] x))
(def ^:private no-meta-fn (fn [x] x))

(deftest run-invocation-success-test
  (let [result (engine/run-invocation #'add-fn add-fn [2 3])
        wf (first (test-util/find-by-wf-def (test-util/ds) "tenon.workflow-test/add-fn"))
        events (test-util/history (test-util/ds) (:invocation_id wf))]
    (is (= 5 result))
    (is (= [2 3] (edn/read-string (:params wf))))
    (is (nil? (:parent_invocation_id wf)) "a top-level invocation has no parent")
    (is (= ["STARTED" "DONE"] (mapv :state events)))
    (is (= 5 (edn/read-string (:data (last events)))))))

(deftest run-invocation-stores-bound-workflow-meta-test
  (binding [engine/*workflow-meta* {:actor-id 42 :trace-id "abc"}]
    (engine/run-invocation #'meta-fn meta-fn [1]))
  (let [wf (first (test-util/find-by-wf-def (test-util/ds) "tenon.workflow-test/meta-fn"))]
    (is (= {:actor-id 42 :trace-id "abc"} (edn/read-string (:metadata wf))))))

(deftest run-invocation-without-bound-workflow-meta-stores-nil-test
  (engine/run-invocation #'no-meta-fn no-meta-fn [1])
  (let [wf (first (test-util/find-by-wf-def (test-util/ds) "tenon.workflow-test/no-meta-fn"))]
    (is (nil? (:metadata wf)))))

(deftest nested-invocation-records-parent-workflow-id-test
  ;; A #workflow fn invoked from inside another running #workflow fn must
  ;; record the outer invocation's id as its parent_invocation_id, via
  ;; *invocation-stack*, without either caller passing it explicitly.
  (let [result (engine/run-invocation #'nested-outer-fn nested-outer-fn [5])
        outer-wf (first (test-util/find-by-wf-def (test-util/ds) "tenon.workflow-test/nested-outer-fn"))
        inner-wf (first (test-util/find-by-wf-def (test-util/ds) "tenon.workflow-test/nested-inner-fn"))]
    (is (= 11 result))
    (is (nil? (:parent_invocation_id outer-wf)))
    (is (= (:invocation_id outer-wf) (:parent_invocation_id inner-wf)))
    (is (nil? (seq engine/*invocation-stack*))
        "the stack unwinds back to empty once both invocations return")))

(deftest run-invocation-error-test
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"boom"
        (engine/run-invocation #'boom-fn boom-fn ["boom"])))
  (let [wf (first (test-util/find-by-wf-def (test-util/ds) "tenon.workflow-test/boom-fn"))
        events (test-util/history (test-util/ds) (:invocation_id wf))]
    (is (= ["STARTED" "ERROR"] (mapv :state events)))
    (is (= "boom" (:message (edn/read-string (:data (last events))))))))

(deftest list-pending-test
  (let [id (test-util/insert! "test.ns/crashed" :params (pr-str [1]))]
    (is (some #(= id (:invocation_id %)) (engine/list-pending)))
    (is (= (pr-str [1]) (:params (first (filter #(= id (:invocation_id %)) (engine/list-pending)))))
        "pending rows carry their params")
    (let [child-id (test-util/insert! "test.ns/crashed-child" :params (pr-str [2]) :parent id)]
      (is (some #(= child-id (:invocation_id %)) (engine/list-pending)) "nested workflows are listed too"))
    (engine/run-invocation #'finished-fn finished-fn [])
    (is (not (some #(= "tenon.workflow-test/finished-fn" (:wf_def %)) (engine/list-pending))))))

(deftest restart-invocation-test
  (engine/register! #'flaky-fn flaky-fn)
  (is (thrown? clojure.lang.ExceptionInfo
        (engine/run-invocation #'flaky-fn flaky-fn [0])))
  (let [id (:invocation_id (first (test-util/find-by-wf-def (test-util/ds) "tenon.workflow-test/flaky-fn")))]
    (is (= "ERROR" (:state (db/get-workflow (test-util/ds) id))))
    (engine/register! #'flaky-fn (fn [_n] :recovered))
    (let [result (engine/restart-invocation id)]
      (is (= :recovered result))
      (is (= ["STARTED" "ERROR" "STARTED" "DONE"] (mapv :state (test-util/history (test-util/ds) id)))))))

(deftest run-invocation-preserves-full-args-and-result-despite-print-length-test
  ;; A caller-bound *print-length*/*print-level* (common in REPLs/editors) must
  ;; not truncate what gets stored: truncated EDN ("...") can't be read back,
  ;; so a later restart would replay different arguments than were really used.
  (let [long-arg (vec (range 500))
        result (binding [*print-length* 3 *print-level* 2]
                 (engine/run-invocation #'long-args-fn long-args-fn [long-arg]))]
    (is (= long-arg result)))
  (let [wf (first (test-util/find-by-wf-def (test-util/ds) "tenon.workflow-test/long-args-fn"))
        events (test-util/history (test-util/ds) (:invocation_id wf))]
    (is (= [(vec (range 500))] (edn/read-string (:params wf))))
    (is (= (vec (range 500)) (edn/read-string (:data (last events)))))))

(deftest done-recording-failure-not-misclassified-as-error-test
  ;; If the underlying fn succeeds (its side effect already happened) but
  ;; recording the DONE event fails, that must not be reported as the
  ;; function's own ERROR - doing so would make a caller think the side
  ;; effect never happened, and restarting would repeat it.
  (let [calls (atom [])
        side-effect-ran (atom false)
        f (fn [] (reset! side-effect-ran true) :ok)]
    (with-redefs [db/finish! (let [real-finish! db/finish!]
                               (fn [ds invocation-id state result]
                                 (swap! calls conj state)
                                 (if (= state "DONE")
                                   (throw (ex-info "db down while recording DONE" {}))
                                   (real-finish! ds invocation-id state result))))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"db down while recording DONE"
            (engine/run-invocation #'done-record-fails-fn f []))))
    (is (true? @side-effect-ran) "the underlying fn ran and succeeded")
    (is (= ["DONE"] @calls)
        "only DONE was attempted - the DONE-recording failure was not misclassified as an ERROR of the function")))

(deftest run-invocation-persists-despite-enclosing-transaction-rollback-test
  ;; Application code may wrap a #workflow invocation in its own SQLite
  ;; transaction (e.g. as part of a larger unit of work) and later roll
  ;; that transaction back because something else in it failed. tenon's
  ;; audit rows must not be swept into that rollback: db.clj writes each
  ;; event via its own connection (auto-commit per statement, not the
  ;; caller's transaction-bound connection), so they're durable the
  ;; instant they're written, regardless of what the surrounding
  ;; transaction later does.
  (let [outer-ds (jdbc/get-datasource (str "jdbc:sqlite:" test-util/*db-path*))]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"caller failure after the workflow call"
          (jdbc/with-transaction [_tx outer-ds]
            (engine/run-invocation #'transactional-fn transactional-fn [10 20])
            (throw (ex-info "caller failure after the workflow call" {})))))
    (let [wf (first (test-util/find-by-wf-def (test-util/ds) "tenon.workflow-test/transactional-fn"))
          events (test-util/history (test-util/ds) (:invocation_id wf))]
      (is (some? wf)
          "the workflow row survived the enclosing transaction's rollback")
      (is (= ["STARTED" "DONE"] (mapv :state events))
          "both events were committed, not undone by the caller's rollback")
      (is (= 30 (edn/read-string (:data (last events))))))))

(deftest run-invocation-dedups-completed-invocation-test
  ;; Same wf-def + same args as an already-DONE invocation must not
  ;; re-execute - it should replay the stored result instead.
  (let [result1 (engine/run-invocation #'dedup-fn dedup-fn [7])
        calls (atom 0)
        result2 (engine/run-invocation #'dedup-fn (fn [x] (swap! calls inc) (* x 10)) [7])]
    (is (= 70 result1 result2))
    (is (zero? @calls) "must not have re-executed - the DONE row was replayed")
    (is (= 1 (count (test-util/find-by-wf-def (test-util/ds) "tenon.workflow-test/dedup-fn")))
        "only one workflow row exists for this wf-def+args pair")))

(deftest run-invocation-dedups-failed-invocation-test
  ;; Same wf-def + same args as an already-ERROR invocation must not
  ;; re-execute - it should rethrow describing the earlier failure.
  (is (thrown? clojure.lang.ExceptionInfo
        (engine/run-invocation #'dedup-error-fn dedup-error-fn ["boom"])))
  (let [calls (atom 0)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"previously failed"
          (engine/run-invocation #'dedup-error-fn (fn [msg] (swap! calls inc) msg) ["boom"])))
    (is (zero? @calls) "must not have re-executed - the ERROR row was replayed as a failure")))

(deftest run-invocation-dedups-started-invocation-by-polling-test
  ;; A concurrent call already at STARTED must be waited out (polled) rather
  ;; than re-executed; once it reaches DONE, the waiter returns that result.
  (let [wf-def "tenon.workflow-test/dedup-started-fn"
        id (test-util/insert! wf-def :params (pr-str [42]))]
    (future
      (Thread/sleep 50)
      (db/finish! (test-util/ds) id "DONE" (pr-str :polled-result)))
    (let [calls (atom 0)
          result (engine/run-invocation #'dedup-started-fn
                                         (fn [n] (swap! calls inc) n)
                                         [42])]
      (is (= :polled-result result))
      (is (zero? @calls) "must not have re-executed - it polled the STARTED row instead"))))

(deftest run-invocation-race-loser-awaits-winner-test
  ;; Simulates two threads racing to start the same (wf-def, args) call,
  ;; where the dedup pre-check (find-by-wf-def-and-params) sees nothing
  ;; for both, then a "winner" wins the actual insert-workflow! race. The
  ;; "loser" here is the real call: its own insert-workflow! is stubbed to
  ;; first let the winner's row (already DONE) into the table, then attempt
  ;; its own insert - which must hit ON CONFLICT DO NOTHING (0 rows, nil
  ;; return) and fall back to awaiting (and returning) the winner's result
  ;; instead of trying to execute raw-fn itself.
  (let [real-insert-workflow! db/insert-workflow!
        calls (atom 0)]
    (with-redefs [db/insert-workflow!
                  (fn [ds wf-def params-str parent-id meta-str timeout-ms]
                    (db/finish! ds (real-insert-workflow! ds wf-def params-str parent-id meta-str timeout-ms)
                                "DONE" (pr-str :winner-result))
                    (real-insert-workflow! ds wf-def params-str parent-id meta-str timeout-ms))]
      (let [result (engine/run-invocation #'dedup-fn
                                           (fn [x] (swap! calls inc) (* x 10))
                                           [999])]
        (is (= :winner-result result))
        (is (zero? @calls) "the loser must not have run raw-fn itself")))))

(deftest restart-preconditions-test
  (is (thrown? clojure.lang.ExceptionInfo (engine/restart-invocation 123456)))
  (let [id (test-util/insert! "test.ns/pending-only")]
    (is (thrown? clojure.lang.ExceptionInfo (engine/restart-invocation id))))
  (let [id (test-util/insert! "test.ns/never-registered")]
    (db/finish! (test-util/ds) id "DONE" (pr-str nil))
    (is (thrown? clojure.lang.ExceptionInfo (engine/restart-invocation id)))))

(deftest register-and-lookup-test
  (engine/register! #'echo-fn echo-fn)
  (let [v (engine/lookup 'tenon.workflow-test/echo-fn)]
    (is (= #'echo-fn v))
    (is (= echo-fn (engine/raw-fn v)))))

(deftest lookup-unknown-returns-nil-test
  (is (nil? (engine/lookup 'tenon.workflow-test/does-not-exist))))

(deftest lookup-existing-but-unregistered-var-returns-nil-test
  ;; The var is real (defined, loaded) but register! was never called on
  ;; it, so it carries no raw-fn metadata - lookup must not treat "the
  ;; var exists" as "it was registered".
  (is (nil? (engine/lookup 'tenon.workflow-test/unregistered-fn))))

(deftest lookup-in-unloaded-namespace-returns-nil-test
  ;; find-var throws IllegalArgumentException for a namespace that was
  ;; never loaded (distinct from an existing namespace missing the var) -
  ;; lookup must swallow that and return nil like any other miss, since
  ;; this is exactly what happens for a stale/bogus wf_def string.
  (is (nil? (engine/lookup 'this.namespace.was.never/loaded))))

(deftest run-invocation-times-out-expired-started-invocation-test
  ;; A STARTED row older than :tenon/timeout-ms (e.g. its process died)
  ;; must not make later calls with the same args wait - it gets marked
  ;; ERROR as timed out, without re-executing the function.
  (let [id (test-util/insert! "tenon.workflow-test/dedup-started-fn" :params (pr-str [5]))
        calls (atom 0)]
    (test-util/expire! (test-util/ds) id)
    (let [e (is (thrown? clojure.lang.ExceptionInfo
                  (engine/run-invocation #'dedup-started-fn (fn [n] (swap! calls inc) n) [5])))]
      (is (= ::engine/previous-failure (:type (ex-data e))))
      (is (= ::engine/timed-out (-> e ex-data :failure :data :type))))
    (is (zero? @calls))
    (is (= ["STARTED" "ERROR"] (mapv :state (test-util/history (test-util/ds) id))))))

(deftest run-invocation-timeout-counts-from-start-time-test
  ;; The deadline is the invocation's start time + :tenon/timeout-ms, not
  ;; the moment a waiter begins waiting: a waiter arriving late must give
  ;; up sooner than timeout-ms.
  (test-util/insert! "tenon.workflow-test/dedup-started-fn" :params (pr-str [7]) :timeout-ms 2000)
  (Thread/sleep 1500)
  (let [t0 (System/nanoTime)]
    (is (thrown? clojure.lang.ExceptionInfo
          (engine/run-invocation #'dedup-started-fn dedup-started-fn [7])))
    (is (< (/ (- (System/nanoTime) t0) 1e6) 1800)
        "gave up well before a full timeout-ms after it began waiting")))

(deftest restart-invocation-restarts-timed-out-invocation-test
  (engine/register! #'echo-fn echo-fn)
  (let [id (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [2]) :timeout-ms 60000)]
    (test-util/expire! (test-util/ds) id)
    (is (= 2 (engine/restart-invocation id)))
    (is (= ["STARTED" "ERROR" "STARTED" "DONE"] (mapv :state (test-util/history (test-util/ds) id))))))

(deftest run-invocation-without-timeout-waits-without-limit-test
  ;; nil :tenon/timeout-ms opts out - even a long-STARTED invocation is
  ;; waited out until it finishes.
  (let [id (test-util/insert! "tenon.workflow-test/dedup-started-fn" :params (pr-str [6]) :timeout-ms nil)]
    (jdbc/execute! (test-util/ds) ["UPDATE workflow SET state_changed_at = 0 WHERE invocation_id = ?" id])
    (future
      (Thread/sleep 300)
      (db/finish! (test-util/ds) id "DONE" (pr-str :late-result)))
    (is (= :late-result (engine/run-invocation #'dedup-started-fn dedup-started-fn [6])))))

(deftest concurrent-restart-runs-function-once-test
  ;; Two restarts that both passed the DONE/ERROR precondition check before
  ;; either recorded STARTED: only one may run the function. Simulated
  ;; deterministically by making the second restart's check see the stale
  ;; latest event from before the first restart started.
  (let [calls (atom 0)
        running (promise)
        release (promise)
        blocking-fn (fn [x] (swap! calls inc) (deliver running true) @release x)]
    (engine/register! #'echo-fn blocking-fn)
    (let [id (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [4]))]
      (db/finish! (test-util/ds) id "DONE" (pr-str 4))
      (let [stale (db/get-workflow (test-util/ds) id)
            first-restart (future (engine/restart-invocation id))]
        @running
        (let [e (with-redefs [db/get-workflow (fn [_ _] stale)]
                  (is (thrown? clojure.lang.ExceptionInfo (engine/restart-invocation id))))]
          (is (= ::engine/precondition-failed (:type (ex-data e)))))
        (deliver release true)
        (is (= 4 @first-restart))
        (is (= 1 @calls))
        (is (= ["STARTED" "DONE" "STARTED" "DONE"] (mapv :state (test-util/history (test-util/ds) id))))))))

(deftest late-finish-after-timeout-keeps-timeout-test
  ;; An invocation timed out while still running: when it finally finishes,
  ;; its DONE must not overwrite the recorded timeout - but its own caller
  ;; still gets the result.
  (let [running (promise)
        release (promise)
        f (fn [x] (deliver running true) @release x)
        caller (future (engine/run-invocation #'echo-fn f [8]))]
    @running
    (let [id (:invocation_id (first (test-util/find-by-wf-def (test-util/ds) "tenon.workflow-test/echo-fn")))]
      (test-util/expire! (test-util/ds) id)
      (is (thrown? clojure.lang.ExceptionInfo (engine/run-invocation #'echo-fn f [8]))
          "a waiter times it out")
      (deliver release true)
      (is (= 8 @caller))
      (is (= ["STARTED" "ERROR"] (mapv :state (test-util/history (test-util/ds) id)))))))
