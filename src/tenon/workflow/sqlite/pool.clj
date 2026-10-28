(ns tenon.workflow.sqlite.pool
  "A small SQLite connection pool whose connections keep their prepared statements."
  (:import [java.io Closeable]
           [java.sql Connection DriverManager PreparedStatement]
           [java.util HashMap Map]
           [java.util.concurrent CompletableFuture ConcurrentLinkedQueue ExecutionException
            LinkedBlockingDeque Semaphore]
           [java.util.concurrent.locks Condition ReentrantLock]
           [java.util.concurrent.atomic AtomicBoolean]))

(defn- close-quietly [^java.lang.AutoCloseable c]
  (try (.close c) (catch Throwable _)))

(deftype Conn [^Connection conn ^Map stmts]) ; a connection and its prepared statements, by SQL

(defn- set-params ^PreparedStatement [^PreparedStatement ps params]
  (loop [i 1 params (seq params)]
    (when params
      (.setObject ps i (first params))
      (recur (inc i) (next params))))
  ps)

(defn- with-statement [^Conn c ^String sql params f]
  (let [^Map stmts (.-stmts c)
        ^PreparedStatement ps (or (.get stmts sql)
                                  (let [ps (.prepareStatement ^Connection (.-conn c) sql)] (.put stmts sql ps) ps))]
    (try
      (f (set-params ps params))
      (catch Throwable e
        (.remove stmts sql)
        (close-quietly ps)
        (throw e)))))

(defn- discard [^Conn c]
  (close-quietly (.-conn c)))

(declare borrow give-back)

(defmacro ^:private with-borrowed [[sym pool] & body]
  `(let [pool# ~pool
         ~sym (borrow pool#)]
     (try ~@body (finally (give-back pool# ~sym)))))

(deftype Pool [^String url ^LinkedBlockingDeque idle ^Semaphore permits ^AtomicBoolean closed
               ^ReentrantLock write-lock ^Condition batch-done ^AtomicBoolean leading
               ^ConcurrentLinkedQueue writes]
  Closeable
  (close [_this]
    (.set closed true)
    (loop []
      (when-let [c (.poll idle)]
        (discard c)
        (recur)))))

(defn open ^Pool [url size]
  (let [lock (ReentrantLock.)]
    (Pool. url (LinkedBlockingDeque.) (Semaphore. size) (AtomicBoolean. false) lock (.newCondition lock)
           (AtomicBoolean. false) (ConcurrentLinkedQueue.))))

(defn with-connection
  "Calls (f connection) with the java.sql.Connection of a connection
   borrowed from pool, giving it back after."
  [pool f]
  (with-borrowed [^Conn c pool] (f (.-conn c))))

(defn execute!
  "Runs sql - a statement without a result - with params on c, a
   connection of the pool as write! passes to its fn, through its cached
   prepared statement. Returns the update count."
  [c sql & params]
  (with-statement c sql params (fn [^PreparedStatement ps] (.executeUpdate ps))))

(defn- run-each
  "Runs each write of batch on c in a transaction of its own."
  [c batch]
  (doseq [[f ^CompletableFuture result] batch]
    (try (.complete result (f c))
         (catch Throwable e (.completeExceptionally result e)))))

(defn- run-together
  "Runs the writes of batch on c in one transaction - one commit, so one
   fsync, for all of them. A write that throws rolls them all back, and
   they are retried one by one so each fails or succeeds on its own."
  [c batch]
  (let [results (try
                  (execute! c "BEGIN IMMEDIATE")
                  (mapv (fn [[f]] (f c)) batch)
                  (catch Throwable _
                    (try (execute! c "ROLLBACK") (catch Throwable _))
                    nil))]
    (if results
      (let [committed (try (execute! c "COMMIT") nil
                           (catch Throwable e
                             (try (execute! c "ROLLBACK") (catch Throwable _))
                             e))]
        (doseq [[[_ ^CompletableFuture result] v] (map vector batch results)]
          (if committed
            (.completeExceptionally result committed)
            (.complete result v))))
      (run-each c batch))))

(defmacro ^:private locking-with [lock & body]
  `(let [^ReentrantLock lock# ~lock]
     (.lock lock#)
     (try ~@body (finally (.unlock lock#)))))

(defn write!
  "Calls (f conn) with a connection of the pool, as part of a group commit:
   SQLite runs one writer at a time, and each commit waits for an fsync. So
   writes queue up here, and one writer at a time - the leader - runs
   every write queued so far in a single transaction, and hands each its
   result once they are durable. The others wait meanwhile: those whose
   write it ran return with their result, and one of the rest leads the
   next batch - every write that arrived during this one's commit. A write
   must be a single statement (atomic on its own), and sees the effects of
   the writes before it in its batch. Writers of other processes still meet
   at SQLite's own lock."
  [^Pool pool f]
  (let [result (CompletableFuture.)
        ^ReentrantLock lock (.-write-lock pool)
        ^Condition batch-done (.-batch-done pool)
        ^AtomicBoolean leading (.-leading pool)
        ^ConcurrentLinkedQueue writes (.-writes pool)]
    (.add writes [f result])
    (when (locking-with lock
            (while (and (.get leading) (not (.isDone result)))
              (.await batch-done))
            (when-not (.isDone result)
              (.set leading true)
              true))
      (try
        (let [batch (loop [batch []]
                      (if-let [w (.poll writes)] (recur (conj batch w)) batch))]
          (try
            (with-borrowed [c pool]
              (if (next batch)
                (run-together c batch)
                (run-each c batch)))
            (catch Throwable e
              ;; e.g. no connection: fail the batch - its writes are no
              ;; longer queued, so nobody else would run them
              (doseq [[_ ^CompletableFuture r] batch]
                (.completeExceptionally r e)))))
        (finally
          (locking-with lock
            (.set leading false)
            (.signalAll batch-done)))))
    (try (.get result)
         (catch ExecutionException e (throw (.getCause e))))))

(defn query
  "Runs sql - a statement with a result, e.g. a SELECT or a write with
   RETURNING - with params on c - a connection of the pool, as write!
   passes to its fn, or the pool itself - through its cached prepared
   statement, and returns the vector of (row-fn result-set) for every row
   of the result. Reads the result to its end: SQLite only auto-checkpoints
   the WAL when a statement steps to its end, so a RETURNING write closed
   after its first row would let the WAL grow forever. Uses executeQuery:
   the driver's execute steps a statement more than once."
  [c ^String sql params row-fn]
  (if (instance? Pool c)
    (with-borrowed [c c] (query c sql params row-fn))
    (let [^Conn c c]
      (with-statement c sql params
        (fn [^PreparedStatement ps]
          (with-open [r (.executeQuery ps)]
            (loop [rows (transient [])]
              (if (.next r)
                (recur (conj! rows (row-fn r)))
                (persistent! rows)))))))))

(defn- check-open [^Pool pool]
  (when (.get ^AtomicBoolean (.-closed pool))
    (throw (IllegalStateException. "pool is closed"))))

(defn- idle-conn ^Conn [^Pool pool]
  (when-let [^Conn c (.pollFirst ^LinkedBlockingDeque (.-idle pool))]
    (if (.isClosed ^Connection (.-conn c))
      (recur pool)
      c)))

(defn- borrow ^Conn [^Pool pool]
  (check-open pool)
  (.acquire ^Semaphore (.-permits pool))
  (try
    (check-open pool)
    (or (idle-conn pool)
        (->Conn (DriverManager/getConnection (.-url pool)) (HashMap.)))
    (catch Throwable e
      (.release ^Semaphore (.-permits pool))
      (throw e))))

(defn- give-back [^Pool pool ^Conn c]
  (if (or (.isClosed ^Connection (.-conn c)) (.get ^AtomicBoolean (.-closed pool)))
    (discard c)
    (do (.offerFirst ^LinkedBlockingDeque (.-idle pool) c)
        ;; closed meanwhile: do not leave it behind
        (when (.get ^AtomicBoolean (.-closed pool))
          (some-> (.pollFirst ^LinkedBlockingDeque (.-idle pool)) discard))))
  (.release ^Semaphore (.-permits pool)))
