(ns tenon.workflow-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.edn :as edn]
            [matcher-combinators.test :refer [match? thrown-match?]]
            [matcher-combinators.matchers :as m]
            [next.jdbc :as jdbc]
            [tenon.workflow :as engine]
            [tenon.workflow.db :as db]
            [tenon.workflow.timeout :as timeout]
            [tenon.workflow.fixtures :as fixtures]
            [tenon.workflow.test-util :as test-util :refer [temp-db-fixture]]))

(def ^:private ^:dynamic *emitted-events* nil)

(use-fixtures :each
  temp-db-fixture
  (fn [f]
    (let [events (atom [])]
      (binding [*emitted-events* events
                engine/*workflow-engine* (assoc engine/*workflow-engine*
                                                :tenon/on-event #(swap! events conj [%1 %2])
                                                ;; most tests expect no metadata
                                                :tenon/metadata-fn (constantly nil))]
        (f)))))

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
        wf (first (test-util/find-by-wf-def "tenon.workflow-test/add-fn"))]
    (is (= 5 result))
    (is (match? {:workflow/params "[2 3]" :workflow/parent-invocation-id nil?} wf))
    (is (match? [{:workflow/state "STARTED"} {:workflow/state "DONE" :event/data "5"}]
                (test-util/timeline (:workflow/invocation-id wf))))
    (is (match? [[:started {:invocation-id (:workflow/invocation-id wf)
                            :parent-invocation-id nil
                            :wf-def "tenon.workflow-test/add-fn"
                            :args [2 3]
                            :metadata nil}]
                 [:done {:invocation-id (:workflow/invocation-id wf)
                         :parent-invocation-id nil
                         :wf-def "tenon.workflow-test/add-fn"
                         :args [2 3]
                         :result 5}]]
                @*emitted-events*))))

(deftest run-invocation-stores-metadata-from-metadata-fn-test
  (binding [engine/*workflow-engine*
            (assoc engine/*workflow-engine*
                   :tenon/metadata-fn (constantly {:actor-id 42 :trace-id "abc"}))]
    (engine/run-invocation #'meta-fn meta-fn [1]))
  (let [wf (first (test-util/find-by-wf-def "tenon.workflow-test/meta-fn"))]
    (is (= {:actor-id 42 :trace-id "abc"} (edn/read-string (:workflow/metadata wf))))
    (is (match? [[:started {:metadata {:actor-id 42 :trace-id "abc"}}]
                 [:done {}]]
                @*emitted-events*))))

(deftest metadata-fn-returning-nil-stores-nil-test
  (binding [engine/*workflow-engine* (assoc engine/*workflow-engine* :tenon/metadata-fn (constantly nil))]
    (engine/run-invocation #'no-meta-fn no-meta-fn [1]))
  (is (nil? (:workflow/metadata (first (test-util/find-by-wf-def "tenon.workflow-test/no-meta-fn"))))))

(deftest metadata-fn-receives-workflow-type-and-args-test
  (let [calls (atom [])]
    (binding [engine/*workflow-engine*
              (assoc engine/*workflow-engine*
                     :tenon/metadata-fn (fn [wf-type args] (swap! calls conj [wf-type args]) nil))]
      (engine/run-invocation #'add-fn add-fn [2 3]))
    (is (= [['tenon.workflow-test/add-fn [2 3]]] @calls))))

(deftest empty-metadata-fn-test
  (is (= {} (engine/empty-metadata-fn 'some.ns/wf [1 2]))))

(deftest host-thread-metadata-fn-test
  (let [meta (engine/host-thread-metadata-fn 'some.ns/wf [1 2])]
    (is (= (.getName (Thread/currentThread)) (:thread meta)))
    (is (string? (:host meta)))
    (is (= meta (engine/host-thread-metadata-fn 'other.ns/wf [])) "independent of workflow and args")))

(deftest init-defaults-to-empty-metadata-fn-test
  (let [path (str (java.io.File/createTempFile "tenon-meta" ".db"))
        eng (engine/init path)]
    (try
      (is (identical? engine/empty-metadata-fn (:tenon/metadata-fn eng)))
      (binding [engine/*workflow-engine* eng]
        (engine/run-invocation eng #'meta-fn meta-fn [7])
        (is (= "{}" (:workflow/metadata (first (test-util/find-by-wf-def "tenon.workflow-test/meta-fn"))))))
      (finally
        (.close ^java.io.Closeable (:tenon/db eng))
        (doseq [suffix ["" "-wal" "-shm"]]
          (.delete (java.io.File. (str path suffix))))))))

(deftest host-thread-metadata-fn-stores-calling-thread-test
  (let [eng (assoc engine/*workflow-engine* :tenon/metadata-fn engine/host-thread-metadata-fn)]
    (.join (doto (Thread. #(engine/run-invocation eng #'meta-fn meta-fn [7]) "meta-test-thread") .start))
    (let [wf (first (test-util/find-by-wf-def "tenon.workflow-test/meta-fn"))]
      (is (= "meta-test-thread" (:thread (edn/read-string (:workflow/metadata wf))))))))

(deftest nested-invocation-records-parent-workflow-id-test
  ;; A #workflow fn invoked from inside another running #workflow fn must
  ;; record the outer invocation's id as its parent_invocation_id, via
  ;; *invocation-stack*, without either caller passing it explicitly.
  (let [result (engine/run-invocation #'nested-outer-fn nested-outer-fn [5])
        outer-wf (first (test-util/find-by-wf-def "tenon.workflow-test/nested-outer-fn"))
        inner-wf (first (test-util/find-by-wf-def "tenon.workflow-test/nested-inner-fn"))]
    (is (= 11 result))
    (is (match? {:workflow/parent-invocation-id nil?} outer-wf))
    (is (match? {:workflow/parent-invocation-id (:workflow/invocation-id outer-wf)} inner-wf))
    (is (match? [[:started {:parent-invocation-id nil}]
                 [:started {:parent-invocation-id (:workflow/invocation-id outer-wf)}]
                 [:done {:parent-invocation-id (:workflow/invocation-id outer-wf)}]
                 [:done {:parent-invocation-id nil}]]
                @*emitted-events*))
    (is (nil? (seq engine/*invocation-stack*))
        "the stack unwinds back to empty once both invocations return")))

(deftest run-invocation-error-test
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"boom"
        (engine/run-invocation #'boom-fn boom-fn ["boom"])))
  (let [wf (first (test-util/find-by-wf-def "tenon.workflow-test/boom-fn"))
        events (test-util/timeline (:workflow/invocation-id wf))]
    (is (match? [{:workflow/state "STARTED"} {:workflow/state "ERROR"}] events))
    (is (match? {:cause "boom" :via vector? :trace vector?}
                (edn/read-string (:event/data (last events)))))
    (is (match? [[:started {:wf-def "tenon.workflow-test/boom-fn"}]
                 [:error {:wf-def "tenon.workflow-test/boom-fn"
                          :error #(instance? clojure.lang.ExceptionInfo %)}]]
                @*emitted-events*))))

(deftest list-pending-test
  (let [id (test-util/insert! "test.ns/crashed" :params (pr-str [1]))]
    (is (match? [{:workflow/invocation-id id :workflow/params (pr-str [1])}]
                (engine/list-pending))
        "pending rows carry their params")
    (let [child-id (test-util/insert! "test.ns/crashed-child" :params (pr-str [2]) :parent id)]
      (is (match? (m/in-any-order [{:workflow/invocation-id id} {:workflow/invocation-id child-id}])
                  (engine/list-pending))
          "nested workflows are listed too"))
    (engine/run-invocation #'finished-fn finished-fn [])
    (is (not-any? #(= "tenon.workflow-test/finished-fn" (:workflow/name %)) (engine/list-pending)))))

(deftest restart-invocation-test
  (engine/register! #'flaky-fn flaky-fn)
  (is (thrown? clojure.lang.ExceptionInfo
        (engine/run-invocation #'flaky-fn flaky-fn [0])))
  (let [id (:workflow/invocation-id (first (test-util/find-by-wf-def "tenon.workflow-test/flaky-fn")))]
    (is (= "ERROR" (:workflow/state (engine/get-invocation id))))
    (engine/register! #'flaky-fn (fn [_n] :recovered))
    (let [result (engine/restart-invocation id)]
      (is (= :recovered result))
      (is (match? [{:workflow/state "STARTED"} {:workflow/state "ERROR"} {:workflow/state "STARTED"} {:workflow/state "DONE"}]
                  (test-util/timeline id)))
      (is (match? [[:started {}]
                   [:error {}]
                   [:restarted {:previous-invocation-id id}]
                   [:done {:result :recovered}]]
                  @*emitted-events*)))))

(deftest restart-invocation-replaces-metadata-test
  (let [id (binding [engine/*workflow-engine* (assoc engine/*workflow-engine*
                                                     :tenon/metadata-fn (constantly {:run 1}))]
             (is (thrown? clojure.lang.ExceptionInfo
                   (engine/run-invocation #'boom-fn boom-fn ["x"])))
             (:workflow/invocation-id (first (test-util/find-by-wf-def "tenon.workflow-test/boom-fn"))))
        calls (atom [])]
    (engine/register! #'boom-fn (fn [_] :ok))
    (binding [engine/*workflow-engine* (assoc engine/*workflow-engine*
                                              :tenon/metadata-fn (fn [wf-type args]
                                                                   (swap! calls conj [wf-type args])
                                                                   {:run 2}))]
      (engine/restart-invocation id))
    (is (= [['tenon.workflow-test/boom-fn ["x"]]] @calls))
    (is (= {:run 2} (edn/read-string (:workflow/metadata (engine/get-invocation id)))))
    (is (match? [{:workflow/state "STARTED" :event/data (pr-str {:run 1})} {:workflow/state "ERROR"}
                 {:workflow/state "STARTED" :event/data (pr-str {:run 2})} {:workflow/state "DONE"}]
                (test-util/timeline id)))
    (is (match? [:restarted {:metadata {:run 2}}] (nth @*emitted-events* 2)))))

(deftest run-invocation-preserves-full-args-and-result-despite-print-length-test
  ;; A caller-bound *print-length*/*print-level* (common in REPLs/editors) must
  ;; not truncate what gets stored: truncated EDN ("...") can't be read back,
  ;; so a later restart would replay different arguments than were really used.
  (let [long-arg (vec (range 500))
        result (binding [*print-length* 3 *print-level* 2]
                 (engine/run-invocation #'long-args-fn long-args-fn [long-arg]))]
    (is (= long-arg result)))
  (let [wf (first (test-util/find-by-wf-def "tenon.workflow-test/long-args-fn"))
        events (test-util/timeline (:workflow/invocation-id wf))]
    (is (= [(vec (range 500))] (edn/read-string (:workflow/params wf))))
    (is (= (vec (range 500)) (edn/read-string (:event/data (last events)))))))

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
  ;; Application code may wrap a #workflow invocation in its own database
  ;; transaction (e.g. as part of a larger unit of work) and later roll
  ;; that transaction back because something else in it failed. tenon's
  ;; audit rows must not be swept into that rollback: db.clj writes each
  ;; event via its own connection (auto-commit per statement, not the
  ;; caller's transaction-bound connection), so they're durable the
  ;; instant they're written, regardless of what the surrounding
  ;; transaction later does.
  (let [outer-ds (jdbc/get-datasource test-util/*jdbc-url*)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"caller failure after the workflow call"
          (jdbc/with-transaction [_tx outer-ds]
            (engine/run-invocation #'transactional-fn transactional-fn [10 20])
            (throw (ex-info "caller failure after the workflow call" {})))))
    (let [wf (first (test-util/find-by-wf-def "tenon.workflow-test/transactional-fn"))
          events (test-util/timeline (:workflow/invocation-id wf))]
      (is (some? wf)
          "the workflow row survived the enclosing transaction's rollback")
      (is (match? [{:workflow/state "STARTED"} {:workflow/state "DONE" :event/data (pr-str 30)}] events)
          "both events were committed, not undone by the caller's rollback"))))

(deftest run-invocation-dedups-completed-invocation-test
  ;; Same wf-def + same args as an already-DONE invocation must not
  ;; re-execute - it should replay the stored result instead.
  (let [result1 (engine/run-invocation #'dedup-fn dedup-fn [7])
        calls (atom 0)
        result2 (engine/run-invocation #'dedup-fn (fn [x] (swap! calls inc) (* x 10)) [7])]
    (is (= 70 result1 result2))
    (is (zero? @calls) "must not have re-executed - the DONE row was replayed")
    (is (= 1 (count (test-util/find-by-wf-def "tenon.workflow-test/dedup-fn")))
        "only one workflow row exists for this wf-def+args pair")
    (is (match? [[:started {}] [:done {:result 70}] [:reused {:state "DONE"}]]
                @*emitted-events*))))

(deftest run-invocation-dedups-failed-invocation-test
  ;; Same wf-def + same args as an already-ERROR invocation must not
  ;; re-execute - it should rethrow describing the earlier failure.
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"boom"
        (engine/run-invocation #'dedup-error-fn dedup-error-fn ["boom"])))
  (let [calls (atom 0)]
    (is (thrown-match? clojure.lang.ExceptionInfo
                       {:type ::engine/previous-failure}
                       (engine/run-invocation #'dedup-error-fn (fn [msg] (swap! calls inc) msg) ["boom"])))
    (is (zero? @calls) "must not have re-executed - the ERROR row was replayed as a failure")))

(deftest run-invocation-dedups-started-invocation-by-polling-test
  ;; A concurrent call already at STARTED must be waited out (polled) rather
  ;; than re-executed; once it reaches DONE, the waiter returns that result.
  (let [wf-def "tenon.workflow-test/dedup-started-fn"
        id (test-util/insert! wf-def :params (pr-str [42]))]
    (future
      (Thread/sleep 50)
      (test-util/finish! id "DONE" (pr-str :polled-result)))
    (let [calls (atom 0)
          result (engine/run-invocation #'dedup-started-fn
                                         (fn [n] (swap! calls inc) n)
                                         [42])]
      (is (= :polled-result result))
      (is (zero? @calls) "must not have re-executed - it polled the STARTED row instead"))))

(defn- reuse-rows [wf-def]
  (let [id (:workflow/invocation-id (first (test-util/find-by-wf-def wf-def)))]
    (filterv #(= "REUSED" (:workflow/state %)) (test-util/timeline id))))

(deftest run-invocation-logs-reused-result-test
  (let [id (test-util/insert! "tenon.workflow-test/dedup-fn" :params (pr-str [8]))
        parent (test-util/insert! "test.ns/reusing-parent")]
    (test-util/finish! id "DONE" (pr-str 80))
    (is (= 80 (binding [engine/*invocation-stack* (list parent)
                        engine/*workflow-engine* (assoc engine/*workflow-engine*
                                                        :tenon/metadata-fn (constantly {:actor-id 42}))]
                (engine/run-invocation #'dedup-fn dedup-fn [8]))))
    (is (match? [{:workflow/invocation-id id :event/data (pr-str {:actor-id 42})}]
                (reuse-rows "tenon.workflow-test/dedup-fn")))
    (is (match? [{:workflow/depth 1 :workflow/invocation-id id :workflow/state "REUSED"}]
                (filter #(= "REUSED" (:workflow/state %)) (engine/full-timeline parent)))
        "the reuse shows up in the reusing parent's timeline as its sub-workflow")))

(deftest run-invocation-logs-reused-failure-test
  (is (thrown? clojure.lang.ExceptionInfo
        (engine/run-invocation #'dedup-error-fn dedup-error-fn ["reused boom"])))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"previously failed"
        (engine/run-invocation #'dedup-error-fn dedup-error-fn ["reused boom"])))
  (is (match? [{:event/data nil}] (reuse-rows "tenon.workflow-test/dedup-error-fn"))
      "a top-level reuse of a failure is logged too"))

(deftest run-invocation-logs-reuse-after-polling-test
  (let [id (test-util/insert! "tenon.workflow-test/dedup-started-fn" :params (pr-str [43]))]
    (future
      (Thread/sleep 50)
      (test-util/finish! id "DONE" (pr-str :polled)))
    (is (= :polled (engine/run-invocation #'dedup-started-fn dedup-started-fn [43])))
    (is (match? [{:workflow/state "STARTED"} {:workflow/state "DONE"} {:workflow/state "REUSED"}]
                (test-util/timeline id))
        "logged once, after the awaited result arrived")))

(deftest run-invocation-storage-calls-test
  ;; Each storage call is a round trip to a database server: a call that
  ;; runs is one to start and one to finish, a call reusing a finished
  ;; result one in all.
  (let [calls (atom [])
        caller (Thread/currentThread)
        counted (fn [k f] (fn [& args]
                            (when (identical? caller (Thread/currentThread)) (swap! calls conj k))
                            (apply f args)))]
    (with-redefs [db/start-workflow! (counted :start db/start-workflow!)
                  db/finish! (counted :finish db/finish!)
                  db/record-reuse! (counted :record-reuse db/record-reuse!)
                  db/get-workflow (counted :get-workflow db/get-workflow)
                  db/find-by-wf-def-and-params (counted :find db/find-by-wf-def-and-params)]
      (is (= 11 (engine/run-invocation #'nested-outer-fn nested-outer-fn [5])))
      (is (= [:start :start :finish :finish] @calls) "a nested call")
      (reset! calls [])
      (is (= 11 (engine/run-invocation #'nested-outer-fn nested-outer-fn [5])))
      (is (= [:start] @calls) "a reuse"))))

(deftest restart-preconditions-test
  (is (thrown-match? clojure.lang.ExceptionInfo
                     {:type ::engine/precondition-failed}
                     (engine/restart-invocation 123456)))
  (let [id (test-util/insert! "test.ns/pending-only")]
    (is (thrown-match? clojure.lang.ExceptionInfo
                       {:type ::engine/precondition-failed}
                       (engine/restart-invocation id))))
  (let [id (test-util/insert! "test.ns/never-registered")]
    (test-util/finish! id "DONE" (pr-str nil))
    (is (thrown-match? clojure.lang.ExceptionInfo
                       {:type ::engine/precondition-failed}
                       (engine/restart-invocation id)))))

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
    (is (thrown-match? clojure.lang.ExceptionInfo
                       {:type ::engine/previous-failure
                        :failure {:data {:type ::timeout/timed-out}}}
                       (engine/run-invocation #'dedup-started-fn (fn [n] (swap! calls inc) n) [5])))
    (is (zero? @calls))
    (is (match? [{:workflow/state "STARTED"} {:workflow/state "TIMEOUT"} {:workflow/state "REUSED"}]
                (test-util/timeline id))
        "the caller that timed it out reused the timeout failure")
    (is (match? [[:timed-out {:invocation-id id}]
                 [:reused {:invocation-id id :state "TIMEOUT"}]]
                @*emitted-events*))))

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
    (is (match? [{:workflow/state "STARTED"} {:workflow/state "TIMEOUT"} {:workflow/state "STARTED"} {:workflow/state "DONE"}]
                (test-util/timeline id)))))

(deftest run-invocation-without-timeout-waits-without-limit-test
  ;; nil :tenon/timeout-ms opts out - even a long-STARTED invocation is
  ;; waited out until it finishes.
  (let [id (test-util/insert! "tenon.workflow-test/dedup-started-fn" :params (pr-str [6]) :timeout-ms nil)]
    (jdbc/execute! (test-util/ds) ["UPDATE event SET occurred_at = 0 WHERE id = ?" id])
    (future
      (Thread/sleep 300)
      (test-util/finish! id "DONE" (pr-str :late-result)))
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
      (test-util/finish! id "DONE" (pr-str 4))
      (let [stale (engine/get-invocation id)
            first-restart (future (engine/restart-invocation id))]
        @running
        (with-redefs [db/get-workflow (fn [_ _] stale)]
          (is (thrown-match? clojure.lang.ExceptionInfo
                             {:type ::engine/precondition-failed}
                             (engine/restart-invocation id))))
        (deliver release true)
        (is (= 4 @first-restart))
        (is (= 1 @calls))
        (is (match? [{:workflow/state "STARTED"} {:workflow/state "DONE"} {:workflow/state "STARTED"} {:workflow/state "DONE"}]
                    (test-util/timeline id)))))))

(deftest late-finish-after-timeout-keeps-timeout-test
  ;; An invocation timed out while still running: when it finally finishes,
  ;; its DONE must not overwrite the recorded timeout - but its own caller
  ;; still gets the result.
  (let [running (promise)
        release (promise)
        f (fn [x] (deliver running true) @release x)
        caller (future (engine/run-invocation #'echo-fn f [8]))]
    @running
    (let [id (:workflow/invocation-id (first (test-util/find-by-wf-def "tenon.workflow-test/echo-fn")))]
      (test-util/expire! (test-util/ds) id)
      (is (thrown-match? clojure.lang.ExceptionInfo
                         {:type ::engine/previous-failure
                          :failure {:data {:type ::timeout/timed-out}}}
                         (engine/run-invocation #'echo-fn f [8]))
          "a waiter times it out")
      (deliver release true)
      (is (= 8 @caller))
      (is (match? [{:workflow/state "STARTED"} {:workflow/state "TIMEOUT"} {:workflow/state "REUSED"}]
                  (test-util/timeline id))
          "only the waiter's reuse of the timeout is logged - no late DONE"))))

(deftest run-invocation-canonicalizes-map-and-set-args-for-dedup-test
  ;; Maps and sets that are = but have different iteration order (e.g.
  ;; array-map vs hash-map, or different insertion histories) must
  ;; serialize to the same canonical EDN string and hit the dedup cache.
  (let [calls (atom 0)
        f (fn [m s] (swap! calls inc) [(:a m) (count s)])
        m1 (array-map :b 2 :a 1 "z" #{:y :x 1} :nested (array-map :d 4 :c 3))
        m2 (array-map :nested (hash-map :c 3 :d 4) "z" (conj #{1} :x :y) :a 1 :b 2)
        s1 (conj #{} {:b 2 :a 1} :z "a" 10)
        s2 (conj #{} 10 "a" :z (array-map :a 1 :b 2))]
    (is (not= (pr-str [m1 s1]) (pr-str [m2 s2]))
        "raw pr-str differs due to map/set ordering")
    (is (= [1 4] (engine/run-invocation #'dedup-fn f [m1 s1])))
    (is (= [1 4] (engine/run-invocation #'dedup-fn f [m2 s2])))
    (is (= 1 @calls) "second call with equal map/set args must reuse stored result")
    (is (= 1 (count (test-util/find-by-wf-def "tenon.workflow-test/dedup-fn"))))))

(deftest same-jvm-concurrent-dedup-wakes-without-sleep-polling-test
  ;; A concurrent caller in the same JVM waits on the in-flight promise
  ;; rather than sleeping through the 200ms poll backoff: once the leader
  ;; reaches the 200ms backoff window and then finishes, the waiter wakes
  ;; immediately and the in-flight entry is cleaned up.
  (let [started (promise)
        release (promise)
        f (fn [x] (deliver started true) @release (* x 10))
        leader (future (engine/run-invocation #'dedup-started-fn f [77]))]
    @started
    (let [waiter (future (engine/run-invocation #'dedup-started-fn f [77]))]
      ;; Let the waiter reach the 200ms backoff cap, then release the leader
      ;; and verify the waiter wakes right away instead of sleeping ~200ms.
      (Thread/sleep 400)
      (let [t0 (System/nanoTime)]
        (deliver release true)
        (is (= 770 @leader))
        (is (= 770 @waiter))
        (is (< (/ (- (System/nanoTime) t0) 1e6) 50)
            "waiter woke via in-process promise without waiting out 200ms sleep")))
    (is (empty? @(:tenon/in-flight engine/*workflow-engine*))
        "in-flight promise map is cleaned up after completion")))

(deftest cross-engine-concurrent-dedup-polling-test
  ;; Two separate engine instances sharing the same SQLite file do not share
  ;; :tenon/in-flight, so the second engine falls back to DB polling until
  ;; the first engine's run finishes.
  (let [engine1 engine/*workflow-engine*
        engine2 (engine/init test-util/*db*)]
    (try
      (let [calls (atom 0)
            started (promise)
            release (promise)
            f (fn [x] (swap! calls inc) (deliver started true) @release (* x 10))
            task1 (future (engine/run-invocation engine1 #'dedup-started-fn f [88]))
            _ @started
            task2 (future (engine/run-invocation engine2 #'dedup-started-fn f [88]))]
        (Thread/sleep 50)
        (deliver release true)
        (is (= 880 @task1))
        (is (= 880 @task2))
        (is (= 1 @calls) "raw-fn ran once; second engine polled DB and reused result"))
      (finally
        (engine/close! engine2)))))

(defn- wait-for
  "Polls pred until it is truthy, for at most timeout-ms. Returns its value."
  [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (or (pred)
          (when (< (System/currentTimeMillis) deadline)
            (Thread/sleep 10)
            (recur))))))

(defn- run-slow-workflow-while-polled
  "Runs echo-fn on engine1, taking run-ms, while a second engine - sharing
   the database, so it can only poll it - calls the same workflow
   meanwhile. Returns [result-of-leader outcome-of-waiter], the latter being
   its result or exception."
  [engine1 run-ms]
  (let [engine2 (engine/init test-util/*db*)
        started (promise)
        f (fn [x] (deliver started true) (Thread/sleep ^long run-ms) x)]
    (try
      (let [leader (future (engine/run-invocation engine1 #'echo-fn f [21]))
            _ @started
            waiter (try (engine/run-invocation engine2 #'echo-fn f [21])
                        (catch clojure.lang.ExceptionInfo e e))]
        [@leader waiter])
      (finally
        (engine/close! engine2)))))

(deftest heartbeat-keeps-long-running-invocation-alive-test
  ;; The lease is far shorter than the run, but the heartbeat renews it, so
  ;; another process polling the database never times the run out.
  (let [engine1 (engine/init test-util/*db* :tenon/timeout-ms 300)]
    (try
      (let [[leader waiter] (run-slow-workflow-while-polled engine1 1200)
            wf (first (test-util/find-by-wf-def "tenon.workflow-test/echo-fn"))]
        (is (= 21 leader))
        (is (= 21 waiter) "the waiter got the real result, not a timeout")
        (is (match? [{:workflow/state "STARTED"} {:workflow/state "DONE"} {:workflow/state "REUSED"}]
                    (test-util/timeline (:workflow/invocation-id wf)))))
      (finally
        (engine/close! engine1)))))

(deftest no-heartbeat-lets-lease-run-out-test
  ;; The same run without the heartbeat is timed out by the waiter - what
  ;; the heartbeat prevents.
  (let [engine1 (engine/init test-util/*db* :tenon/timeout-ms 300 :tenon/heartbeat-ms nil)]
    (try
      (let [[leader waiter] (run-slow-workflow-while-polled engine1 1200)]
        (is (= 21 leader) "its own caller still gets the result")
        (is (thrown-match? clojure.lang.ExceptionInfo
                           {:type ::engine/previous-failure
                            :failure {:data {:type ::timeout/timed-out}}}
                           (throw waiter))))
      (finally
        (engine/close! engine1)))))

(deftest engine-config-defaults-test
  (let [e engine/*workflow-engine*]
    (is (match? {:tenon/timeout-ms 15000 :tenon/lease-batch-size 500
                 :tenon/backoff-base-ms 5 :tenon/backoff-multiplier 2
                 :tenon/backoff-max-ms 200 :tenon/backoff-jitter 0.1}
                e))))

(deftest backoff-waits-grow-exponentially-up-to-the-maximum-test
  (is (= [10 30 90 100 100 100]
         (take 6 (@#'engine/backoff-waits {:tenon/backoff-base-ms 10 :tenon/backoff-multiplier 3
                                           :tenon/backoff-max-ms 100 :tenon/backoff-jitter 0})))))

(deftest backoff-waits-are-lengthened-by-a-random-amount-within-the-jitter-test
  (let [waits (take 200 (@#'engine/backoff-waits {:tenon/backoff-base-ms 100 :tenon/backoff-multiplier 1
                                                  :tenon/backoff-max-ms 100 :tenon/backoff-jitter 0.25}))]
    (is (every? #(<= 100 % 124) waits) "never shorter, and by less than a quarter longer")
    (is (> (count (distinct waits)) 10) "and not all the same")))

(deftest with-backoff-calls-until-something-but-nil-test
  (let [calls (atom 0)
        engine {:tenon/backoff-base-ms 1 :tenon/backoff-multiplier 2 :tenon/backoff-max-ms 4 :tenon/backoff-jitter 0}]
    (is (= :found (@#'engine/with-backoff engine #(when (= 3 (swap! calls inc)) :found))))
    (is (= 3 @calls))
    (is (false? (@#'engine/with-backoff engine (constantly false))) "only nil means try again")))

(deftest with-backoff-waits-in-between-test
  (let [engine {:tenon/backoff-base-ms 50 :tenon/backoff-multiplier 1 :tenon/backoff-max-ms 50 :tenon/backoff-jitter 0}
        calls (atom 0)
        t0 (System/nanoTime)]
    (@#'engine/with-backoff engine #(when (= 3 (swap! calls inc)) :done))
    (is (<= 100 (/ (- (System/nanoTime) t0) 1e6)) "two waits of 50 ms")))

(deftest with-backoff-waits-on-the-promise-it-is-given-instead-of-sleeping-test
  (let [engine {:tenon/backoff-base-ms 10000 :tenon/backoff-multiplier 2 :tenon/backoff-max-ms 20000 :tenon/backoff-jitter 0}
        calls (atom 0)
        woken (doto (promise) (deliver true))
        t0 (System/nanoTime)]
    (is (= :done (@#'engine/with-backoff engine #(when (= 2 (swap! calls inc)) :done) (constantly woken))))
    (is (< (/ (- (System/nanoTime) t0) 1e6) 1000) "it did not wait out the 10 s")
    (reset! calls 0)
    (is (= :done (@#'engine/with-backoff (assoc engine :tenon/backoff-base-ms 1)
                                         #(when (= 2 (swap! calls inc)) :done)
                                         (constantly nil)))
        "with nothing to wait on it sleeps")))

(deftest engine-on-event-is-never-nil-test
  (let [engine1 (engine/init test-util/*db* :tenon/sweep-ms nil)]
    (try
      (is (fn? (:tenon/on-event engine1)))
      (is (nil? ((:tenon/on-event engine1) :anything {})) "does nothing by default")
      (finally
        (engine/close! engine1)))))

(deftest heartbeat-renews-in-batches-and-reports-lost-leases-test
  ;; More invocations than one storage call renews: all of them are, and
  ;; the one that was timed out meanwhile is reported - once.
  (let [events (atom [])
        engine1 (engine/init test-util/*db* :tenon/timeout-ms 60000 :tenon/heartbeat-ms nil :tenon/sweep-ms nil
                             :tenon/lease-batch-size 7
                             :tenon/on-event #(swap! events conj [%1 %2]))
        ds (:tenon/db engine1)
        heartbeat! @#'timeout/heartbeat!]
    (try
      (let [running (mapv #(test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [%]) :timeout-ms 60000)
                          (range 20))
            timed-out (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [:t]) :timeout-ms 0)]
        (db/time-out! ds timed-out (pr-str {:message "timed out"}))
        (reset! (:tenon/in-flight engine1) (zipmap (conj running timed-out) (repeat nil)))
        (jdbc/execute! ds ["UPDATE workflow SET expires_at = 1 WHERE state = 0"])
        (is (= 20 (count (db/list-expired ds))))
        (let [lost (heartbeat! engine1 #{})]
          (is (empty? (db/list-expired ds)) "all of them were renewed")
          (is (= #{timed-out} lost))
          (is (= [[:lease-lost {:invocation-id timed-out}]] @events))
          (let [lost (heartbeat! engine1 lost)]
            (is (= #{timed-out} lost))
            (is (= 1 (count @events)) "reported once")
            (swap! (:tenon/in-flight engine1) dissoc timed-out)
            (is (= #{} (heartbeat! engine1 lost)) "forgotten once it no longer runs"))))
      (finally
        (engine/close! engine1)))))

(deftest heartbeat-reports-lost-lease-once-test
  ;; An invocation timed out while its process still runs it learns of it
  ;; from the heartbeat - once, not on every beat.
  (let [events (atom [])
        engine1 (engine/init test-util/*db* :tenon/timeout-ms 300 :tenon/sweep-ms nil
                             :tenon/on-event #(swap! events conj [%1 %2]))
        started (promise)
        release (promise)
        f (fn [x] (deliver started true) @release x)]
    (try
      (let [caller (future (engine/run-invocation engine1 #'echo-fn f [22]))
            _ @started
            id (:workflow/invocation-id (first (test-util/find-by-wf-def "tenon.workflow-test/echo-fn")))]
        ;; a beat may renew the lease between expiring it and timing it out
        (is (wait-for (fn [] (test-util/expire! (test-util/ds) id)
                           (db/time-out! (test-util/ds) id (pr-str {:message "timed out"})))
                      2000))
        (is (wait-for #(some (comp #{:lease-lost} first) @events) 2000))
        (Thread/sleep 400)
        (is (= [[:lease-lost {:invocation-id id}]]
               (filterv (comp #{:lease-lost} first) @events))
            "reported once")
        (deliver release true)
        (is (= 22 @caller)))
      (finally
        (engine/close! engine1)))))

(deftest cycle-detection-test
  ;; Direct or mutual recursion with the exact same (wf_def, params)
  ;; already on *invocation-stack* must fail immediately with
  ;; ::engine/cycle-detected instead of deadlocking until timeout.
  (let [direct (fn direct [x] (engine/run-invocation #'echo-fn direct [x]))]
    (is (thrown-match? clojure.lang.ExceptionInfo
                       {:type ::engine/cycle-detected}
                       (engine/run-invocation #'echo-fn direct [1])))
    (is (match? [{:workflow/state "ERROR"}] (test-util/find-by-wf-def "tenon.workflow-test/echo-fn"))))
  (let [step-b (fn [step-a x] (engine/run-invocation #'nested-outer-fn (fn [y] (step-a y)) [x]))
        step-a (fn step-a [x] (engine/run-invocation #'nested-inner-fn (fn [y] (step-b step-a y)) [x]))]
    (is (thrown-match? clojure.lang.ExceptionInfo
                       {:type ::engine/cycle-detected}
                       (step-a 2)))))

(deftest sweep-times-out-expired-invocations-test
  ;; Without anybody calling the dead workflows again, and at any nesting.
  (let [dead (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [31]) :timeout-ms 60000)
        dead-child (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [32])
                                      :parent dead :timeout-ms 60000)
        alive (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [33]) :timeout-ms 60000)
        never (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [34]) :timeout-ms nil)
        done (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [35]) :timeout-ms 60000)]
    (test-util/finish! done "DONE" "35")
    (doseq [id [dead dead-child]]
      (test-util/expire! (test-util/ds) id))
    (is (= [dead dead-child] (engine/sweep!)))
    (is (match? [{:workflow/state "STARTED"} {:workflow/state "TIMEOUT" :event/data (m/via edn/read-string {:data {:type ::timeout/timed-out}})}]
                (test-util/timeline dead)))
    (is (= ["TIMEOUT" "TIMEOUT" "STARTED" "STARTED" "DONE"]
           (mapv (comp :workflow/state engine/get-invocation) [dead dead-child alive never done])))
    (is (match? [[:timed-out {:invocation-id dead :parent-invocation-id nil}]
                 [:timed-out {:invocation-id dead-child :parent-invocation-id dead}]]
                @*emitted-events*))
    (is (= [] (engine/sweep!)) "nothing left to time out")
    (is (= #{dead dead-child} (set (map :workflow/invocation-id (filter #(= "TIMEOUT" (:workflow/state %))
                                                               (engine/list-invocations {:top-level-only? false})))))
        "the sweep only timed out the expired ones")))

(deftest sweep-leaves-timed-out-invocation-restartable-test
  (engine/register! #'echo-fn echo-fn)
  (let [id (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [36]) :timeout-ms 60000)]
    (test-util/expire! (test-util/ds) id)
    (is (= [id] (engine/sweep!)))
    (is (= 36 (engine/restart-invocation id)) "restarting stays a person's call")))

(deftest concurrent-sweeps-time-out-each-invocation-once-test
  (let [id (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [37]) :timeout-ms 60000)
        engine2 (engine/init test-util/*db* :tenon/sweep-ms nil
                             :tenon/on-event #(swap! *emitted-events* conj [%1 %2]))]
    (try
      (test-util/expire! (test-util/ds) id)
      (let [sweeps (mapv #(future (engine/sweep! %)) [engine/*workflow-engine* engine2])]
        (is (= [[id] []] (sort-by count > (mapv deref sweeps))) "one of them timed it out")
        (is (= 1 (count (filter (comp #{:timed-out} first) @*emitted-events*)))))
      (finally
        (engine/close! engine2)))))

(deftest init-sweeps-expired-invocations-left-by-a-previous-run-test
  (let [id (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [38]) :timeout-ms 60000)]
    (test-util/expire! (test-util/ds) id)
    (let [engine2 (engine/init test-util/*db*)]
      (try
        (is (wait-for #(= "TIMEOUT" (:workflow/state (engine/get-invocation engine2 id))) 2000))
        (finally
          (engine/close! engine2))))))

(deftest sweeps-periodically-test
  (let [engine2 (engine/init test-util/*db* :tenon/sweep-ms 20)
        id (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [39]) :timeout-ms 60000)]
    (try
      (is (= "STARTED" (:workflow/state (engine/get-invocation engine2 id))) "a live lease is left alone")
      (test-util/expire! (test-util/ds) id)
      (is (wait-for #(= "TIMEOUT" (:workflow/state (engine/get-invocation engine2 id))) 2000))
      (finally
        (engine/close! engine2)))))

(deftest timeout-is-a-state-of-its-own-test
  ;; A timeout ends in TIMEOUT, a throw in ERROR - told apart by state, at
  ;; any nesting.
  (let [dead (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [41]) :timeout-ms 60000)
        threw (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [42]))
        running (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [43]))
        nested (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [44]) :parent running
                                  :timeout-ms 60000)]
    (test-util/expire! (test-util/ds) dead)
    (test-util/expire! (test-util/ds) nested)
    (engine/sweep!)
    (test-util/finish! threw "ERROR" (pr-str {:message "boom"}))
    (is (= {dead "TIMEOUT" threw "ERROR" running "STARTED" nested "TIMEOUT"}
           (into {} (map (fn [id] [id (:workflow/state (engine/get-invocation id))])) [dead threw running nested])))
    (is (= #{dead nested}
           (set (map :workflow/invocation-id (engine/list-invocations {:state "TIMEOUT" :top-level-only? false})))))
    (is (= #{threw}
           (set (map :workflow/invocation-id (engine/list-invocations {:state "ERROR" :top-level-only? false}))))
        "the ERROR filter has the failure only")))

(deftest timeout-is-reused-as-a-failure-but-says-so-test
  ;; A caller meeting a timed out run gets it as a ::previous-failure, like
  ;; an ERROR, naming the timeout.
  (let [id (test-util/insert! "tenon.workflow-test/echo-fn" :params (pr-str [45]) :timeout-ms 60000)]
    (test-util/expire! (test-util/ds) id)
    (is (thrown-match? clojure.lang.ExceptionInfo
                       {:type ::engine/previous-failure :state "TIMEOUT"
                        :failure {:data {:type ::timeout/timed-out}}}
                       (engine/run-invocation #'echo-fn echo-fn [45])))
    (try (engine/run-invocation #'echo-fn echo-fn [45])
         (catch clojure.lang.ExceptionInfo e
           (is (re-find #"^Workflow previously timed out: " (ex-message e)))))))

#workflow
(defn recursive-inner [n]
  (throw (ex-info "inner down" {:n n})))

#workflow
(defn recursive-middle [n]
  (inc (recursive-inner n)))

#workflow
(defn recursive-outer [n]
  (inc (recursive-middle n)))

(deftest restart-invocation-recursively-restarts-failed-nested-workflows-test
  (is (thrown? Exception (recursive-outer 5)))
  (let [outer (first (test-util/find-by-wf-def "tenon.workflow-test/recursive-outer"))]
    (is (thrown-match? clojure.lang.ExceptionInfo {:type ::engine/previous-failure}
                       (engine/restart-invocation (:workflow/invocation-id outer)))
        "a plain restart trips over the cached failure")
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"inner down"
                          (engine/restart-invocation-recursively (:workflow/invocation-id outer)))
        "a nested workflow failing again aborts the restart with its own failure")
    (engine/register! #'recursive-inner (fn [n] (* n 2)))
    (is (= 12 (engine/restart-invocation-recursively (:workflow/invocation-id outer))))
    (is (every? #(= "DONE" (:workflow/state (first (test-util/find-by-wf-def %))))
                ["tenon.workflow-test/recursive-outer"
                 "tenon.workflow-test/recursive-middle"
                 "tenon.workflow-test/recursive-inner"]))))

(deftest restart-invocation-recursively-leaves-done-nested-workflows-test
  (fixtures/nested-parent 2)
  (let [parent (first (test-util/find-by-wf-def "tenon.workflow.fixtures/nested-parent"))
        child-id (:workflow/invocation-id (first (test-util/find-by-wf-def "tenon.workflow.fixtures/nested-child")))]
    (is (= 5 (engine/restart-invocation-recursively (:workflow/invocation-id parent))))
    (is (= child-id (:workflow/invocation-id (first (test-util/find-by-wf-def "tenon.workflow.fixtures/nested-child"))))
        "the done child keeps its invocation")))

#workflow
(defn fan-out-first []
  (throw (ex-info "first down" {})))

#workflow
(defn fan-out-second []
  (throw (ex-info "second down" {})))

#workflow
(defn fan-out-parent []
  [(fan-out-first) (fan-out-second)])

(defn- started-count [wf-def]
  (->> (test-util/find-by-wf-def wf-def)
       first
       :workflow/invocation-id
       test-util/timeline
       (filter (comp #{"STARTED"} :workflow/state))
       count))

(deftest restart-invocation-recursively-restarts-one-failed-callee-at-a-time-test
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"first down" (fan-out-parent)))
  (let [parent-id (:workflow/invocation-id (first (test-util/find-by-wf-def "tenon.workflow-test/fan-out-parent")))]
    (engine/register! #'fan-out-first (fn [] :first-ok))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"second down"
                          (engine/restart-invocation-recursively parent-id))
        "the first callee recovers, the second one - not called before - fails")
    (engine/register! #'fan-out-second (fn [] :second-ok))
    (is (= [:first-ok :second-ok] (engine/restart-invocation-recursively parent-id)))
    (is (= ["DONE" "DONE" "DONE"]
           (map #(:workflow/state (first (test-util/find-by-wf-def %)))
                ["tenon.workflow-test/fan-out-parent"
                 "tenon.workflow-test/fan-out-first"
                 "tenon.workflow-test/fan-out-second"])))
    (is (= 2 (started-count "tenon.workflow-test/fan-out-first")) "one failed, one succeeded run")
    (is (= 2 (started-count "tenon.workflow-test/fan-out-second")) "one failed, one succeeded run")
    (is (= 3 (started-count "tenon.workflow-test/fan-out-parent")))))

#workflow
(defn timeout-inner [n]
  (throw (ex-info "inner down" {:n n})))

#workflow
(defn timeout-outer [n]
  (inc (timeout-inner n)))

(deftest restart-invocation-recursively-restarts-timed-out-nested-workflows-test
  ;; A callee that timed out (its process died, and a sweep found out) is
  ;; restarted first, like a failed one - else the parent would trip over
  ;; its cached timeout.
  (is (thrown? Exception (timeout-outer 7)))
  (let [outer (first (test-util/find-by-wf-def "tenon.workflow-test/timeout-outer"))
        inner (first (test-util/find-by-wf-def "tenon.workflow-test/timeout-inner"))
        running (test-util/restart! (:workflow/invocation-id inner))]
    (test-util/expire! (test-util/ds) running)
    (engine/sweep!)
    (is (= "TIMEOUT" (:workflow/state (engine/get-invocation (:workflow/invocation-id inner)))))
    (engine/register! #'timeout-inner (fn [n] (* n 2)))
    (is (= 15 (engine/restart-invocation-recursively (:workflow/invocation-id outer))))
    (is (= "DONE" (:workflow/state (engine/get-invocation (:workflow/invocation-id inner)))))))
