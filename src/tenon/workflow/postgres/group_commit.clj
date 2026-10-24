(ns tenon.workflow.postgres.group-commit
  "Group commit: concurrent writes share a transaction - and so the flush
   of the WAL their commit waits for.

   Writes queue up in lanes. In each, one writer at a time - the leader -
   takes every write queued so far and runs them on one connection in one
   transaction, then hands each its result once they are committed. The
   others wait meanwhile: those whose write it ran return with their
   result, and one of the rest leads the next batch - every write that
   arrived during this one. A lane runs its statements one after the
   other, so there are several, each with its own connection: a write
   goes to a lane with no batch running if there is one, so it starts at
   once, and is queued in one otherwise.

   A write is a fn of a java.sql.Connection running one statement, which
   sees the effects of the writes before it in its batch. Should one of
   them fail, the batch is rolled back, and its writes are run again one
   by one, each in a transaction of its own, so that each fails or
   succeeds on its own - as it would have alone. Should the commit fail,
   they all fail: they may or may not have been committed.

   A batch takes the locks of its writes in their order - not, as each
   write does on its own, in the order of the rows' ids - so it could
   deadlock with another batch, or a write running alone: so a batch only
   waits for a lock for lock-timeout-ms, then it is rolled back and run
   one by one. Every lock cycle has a batch in it, and so is broken at
   once (rather than after the server's deadlock_timeout, a second by
   default); a write running alone is run again should it be chosen to
   break one all the same.

   With no lanes, each write runs alone, in a transaction of its own."
  (:import [java.sql Connection SQLException]
           [java.util.concurrent CompletableFuture ConcurrentLinkedQueue ExecutionException]
           [java.util.concurrent.atomic AtomicBoolean AtomicLong]
           [java.util.concurrent.locks Condition ReentrantLock]
           [javax.sql DataSource]))

(deftype Lane [^ReentrantLock lock ^Condition batch-done ^AtomicBoolean leading ^ConcurrentLinkedQueue writes])

(deftype Writer [^DataSource ds lanes ^AtomicLong turn])

(defn writer
  "A writer running writes on connections of ds, in lanes lanes (0: each
   write alone)."
  ^Writer [ds lanes]
  (Writer. ds
           (vec (repeatedly lanes #(let [lock (ReentrantLock.)]
                                     (Lane. lock (.newCondition lock) (AtomicBoolean. false)
                                            (ConcurrentLinkedQueue.)))))
           (AtomicLong.)))

(defn- quietly [f]
  (try (f) (catch Throwable _)))

(def ^:private lock-timeout-ms
  "How long a batch waits for a lock."
  50)

(defn- lock-conflict?
  "Whether e is a write losing a deadlock, or a lock timeout: nothing it
   did is left, and it may be run again."
  [e]
  (and (instance? SQLException e) (#{"40P01" "55P03"} (.getSQLState ^SQLException e))))

(defn- run-alone
  "Runs (f c) in a transaction of its own, again should it lose a lock
   conflict."
  [^Connection c f]
  (loop [attempt 1]
    (let [[ok v] (try [true (f c)]
                      (catch Throwable e
                        (if (and (< attempt 5) (lock-conflict? e))
                          [false]
                          (throw e))))]
      (if ok v (recur (inc attempt))))))

(defn- run-each
  "Runs each write of batch on c alone."
  [^Connection c batch]
  (doseq [[f ^CompletableFuture result] batch]
    (try (.complete result (run-alone c f))
         (catch Throwable e (.completeExceptionally result e)))))

(defn- run-together
  "Runs the writes of batch on c in one transaction, or - should one of
   them fail - each alone."
  [^Connection c batch]
  (.setAutoCommit c false)
  (let [results (try (with-open [st (.createStatement c)]
                       (.execute st (str "SET LOCAL lock_timeout = " lock-timeout-ms)))
                     (mapv (fn [[f]] (f c)) batch)
                     (catch Throwable _
                       (quietly #(.rollback c))
                       nil))]
    (if results
      (let [failure (try (.commit c) nil
                         (catch Throwable e
                           (quietly #(.rollback c))
                           e))]
        (.setAutoCommit c true)
        (doseq [[[_ ^CompletableFuture result] v] (map vector batch results)]
          (if failure
            (.completeExceptionally result failure)
            (.complete result v))))
      (do (.setAutoCommit c true)
          (run-each c batch)))))

(defn- run-batch!
  "Runs batch - on a connection of ds - as one transaction if it has
   several writes, as is otherwise."
  [^DataSource ds batch]
  (try
    (with-open [c (.getConnection ds)]
      (if (next batch)
        (run-together c batch)
        (run-each c batch)))
    (catch Throwable e
      ;; e.g. no connection: fail the batch - its writes are no longer
      ;; queued, so nobody else would run them
      (doseq [[_ ^CompletableFuture result] batch]
        (when-not (.isDone result) (.completeExceptionally result e))))))

(defmacro ^:private locking-with [lock & body]
  `(let [^ReentrantLock lock# ~lock]
     (.lock lock#)
     (try ~@body (finally (.unlock lock#)))))

(defn- pick-lane
  "A lane with no batch running if there is one, else the next one in
   turn."
  ^Lane [^Writer w]
  (let [lanes (.-lanes w)]
    (or (some #(when-not (.get ^AtomicBoolean (.-leading ^Lane %)) %) lanes)
        (nth lanes (mod (.getAndIncrement ^AtomicLong (.-turn w)) (count lanes))))))

(defn write!
  "Runs (f connection) as part of a group commit (see the ns doc), and
   returns its result once committed - or throws what it threw."
  [^Writer w f]
  (if (empty? (.-lanes w))
    (with-open [c (.getConnection ^DataSource (.-ds w))]
      (run-alone c f))
    (let [^Lane lane (pick-lane w)
          result (CompletableFuture.)
          ^ReentrantLock lock (.-lock lane)
          ^Condition batch-done (.-batch-done lane)
          ^AtomicBoolean leading (.-leading lane)
          ^ConcurrentLinkedQueue writes (.-writes lane)]
      (.add writes [f result])
      (when (locking-with lock
              (while (and (.get leading) (not (.isDone result)))
                (.await batch-done))
              (when-not (.isDone result)
                (.set leading true)
                true))
        (try
          (run-batch! (.-ds w) (loop [batch []]
                                 (if-let [x (.poll writes)] (recur (conj batch x)) batch)))
          (finally
            (locking-with lock
              (.set leading false)
              (.signalAll batch-done)))))
      (try (.get result)
           (catch ExecutionException e (throw (.getCause e)))))))
