(ns tenon.workflow.sqlite.pool
  "A small SQLite connection pool whose connections keep their prepared statements."
  (:require [next.jdbc.prepare :as prepare]
            [next.jdbc.protocols :as p]
            [next.jdbc.result-set :as rs])
  (:import [java.io Closeable]
           [java.sql Connection DriverManager PreparedStatement ResultSet]
           [java.util HashMap Map]
           [java.util.concurrent CompletableFuture ConcurrentLinkedQueue ExecutionException
            LinkedBlockingDeque Semaphore]
           [java.util.concurrent.locks ReentrantLock]
           [java.util.concurrent.atomic AtomicBoolean]))

(defn- close-quietly [^java.lang.AutoCloseable c]
  (try (.close c) (catch Throwable _)))

(defn- with-statement [^Connection conn ^Map stmts turso? ^String sql params f]
  (if turso?
    ;; Turso's driver (0.8.3) never resets a statement: run again, it
    ;; returns no rows and an update does nothing. So no caching there.
    (with-open [ps (.prepareStatement conn sql)]
      (f (prepare/set-parameters ps params)))
    (let [^PreparedStatement ps (or (.get stmts sql)
                                    (let [ps (.prepareStatement conn sql)] (.put stmts sql ps) ps))]
      (try
        (f (prepare/set-parameters ps params))
        (catch Throwable e
          (.remove stmts sql)
          (close-quietly ps)
          (throw e))))))

(defn turso-url? [^String url]
  (.startsWith url "jdbc:turso:"))

(defn- returns-rows?
  "Whether sql yields rows. Turso's driver (0.8.3) needs to be told: its
   PreparedStatement.execute() steps the first row itself and getResultSet()
   then hands back the advanced cursor, losing that row - executeQuery()
   does not."
  [^String sql]
  (boolean (re-find #"(?is)^\s*(select|with|pragma|values)\b|\breturning\b" sql)))

(defn- result-set ^ResultSet [turso? sql ^PreparedStatement ps opts]
  (if turso?
    (when (returns-rows? sql) (.executeQuery ps))
    (if (.execute ps)
      (.getResultSet ps)
      (when (:return-keys opts)
        (try (.getGeneratedKeys ps) (catch Exception _))))))

(defn- first-row
  "The first row of ps's result. Reads the result to its end: SQLite only
   auto-checkpoints the WAL when a statement steps to its end, so a
   RETURNING write closed after its first row would let the WAL grow forever."
  [turso? sql ^Connection conn ^PreparedStatement ps opts]
  (if-let [^ResultSet r (result-set turso? sql ps opts)]
    (try
      (reduce (fn [first-row row] (or first-row (rs/datafiable-row row conn opts)))
              nil
              (rs/reducible-result-set r opts))
      (finally (close-quietly r)))
    {:next.jdbc/update-count (if turso? (.executeUpdate ps) (.getUpdateCount ps))}))

(deftype Conn [^Connection conn ^Map stmts turso?]
  p/Executable
  (-execute [_ sql-params opts]
    ;; a reducible outlives this call: no caching
    (p/-execute conn sql-params opts))
  (-execute-one [_ [sql & params] opts]
    (with-statement conn stmts turso? sql params
      #(first-row turso? sql conn % (merge {:return-keys false} opts))))
  (-execute-all [_ [sql & params] opts]
    (with-statement conn stmts turso? sql params
      (fn [^PreparedStatement ps]
        (if turso?
          (if (returns-rows? sql)
            (rs/datafiable-result-set (.executeQuery ps) conn opts)
            [{:next.jdbc/update-count (.executeUpdate ps)}])
          (p/-execute-all ps nil (merge {:return-keys false} opts))))))
  p/Transactable
  (-transact [this body-fn opts]
    (p/-transact conn (fn [_] (body-fn this)) opts)))

(defn- discard [^Conn c]
  (close-quietly (.-conn c)))

(declare borrow give-back)

(defmacro ^:private with-borrowed [[sym pool] & body]
  `(let [pool# ~pool
         ~sym (borrow pool#)]
     (try ~@body (finally (give-back pool# ~sym)))))

(deftype Pool [^String url ^LinkedBlockingDeque idle ^Semaphore permits ^AtomicBoolean closed
               ^ReentrantLock write-lock ^ConcurrentLinkedQueue writes]
  p/Executable
  (-execute [_ _ _]
    (throw (UnsupportedOperationException. "plan needs a connection: borrow one with with-connection")))
  (-execute-one [this sql-params opts]
    (with-borrowed [c this] (p/-execute-one c sql-params opts)))
  (-execute-all [this sql-params opts]
    (with-borrowed [c this] (p/-execute-all c sql-params opts)))
  p/Transactable
  (-transact [this body-fn opts]
    (with-borrowed [c this] (p/-transact c body-fn opts)))
  Closeable
  (close [_this]
    (.set closed true)
    (loop []
      (when-let [c (.poll idle)]
        (discard c)
        (recur)))))

(defn open ^Pool [url size]
  (Pool. url (LinkedBlockingDeque.) (Semaphore. size) (AtomicBoolean. false) (ReentrantLock.)
         (ConcurrentLinkedQueue.)))

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
                  (p/-execute-one c ["BEGIN IMMEDIATE"] {})
                  (mapv (fn [[f]] (f c)) batch)
                  (catch Throwable _
                    (try (p/-execute-one c ["ROLLBACK"] {}) (catch Throwable _))
                    nil))]
    (if results
      (let [committed (try (p/-execute-one c ["COMMIT"] {}) nil
                           (catch Throwable e
                             (try (p/-execute-one c ["ROLLBACK"] {}) (catch Throwable _))
                             e))]
        (doseq [[[_ ^CompletableFuture result] v] (map vector batch results)]
          (if committed
            (.completeExceptionally result committed)
            (.complete result v))))
      (run-each c batch))))

(defn write!
  "Calls (f conn) with a connection of the pool, as part of a group commit:
   SQLite runs one writer at a time, and each commit waits for an fsync. So
   writes queue up here, and whoever holds the write lock runs every write
   queued so far in a single transaction - the ones that arrived while the
   previous batch was committing - and hands each its result once they are
   durable. A write must be a single statement (atomic on its own), and
   sees the effects of the writes before it in its batch. Writers of other
   processes still meet at SQLite's own lock."
  [^Pool pool f]
  (let [result (CompletableFuture.)
        ^ReentrantLock lock (.-write-lock pool)
        ^ConcurrentLinkedQueue writes (.-writes pool)]
    (.add writes [f result])
    (.lock lock)
    (try
      (when-not (.isDone result)
        (let [batch (loop [batch []]
                      (if-let [w (.poll writes)] (recur (conj batch w)) batch))]
          (with-borrowed [c pool]
            (if (next batch)
              (run-together c batch)
              (run-each c batch)))))
      (finally (.unlock lock)))
    (try (.get result)
         (catch ExecutionException e (throw (.getCause e))))))

(defn- check-open [^Pool pool]
  (when (.get ^AtomicBoolean (.-closed pool))
    (throw (IllegalStateException. "pool is closed"))))

(defn- idle-conn ^Conn [^Pool pool]
  (when-let [^Conn c (.pollFirst ^LinkedBlockingDeque (.-idle pool))]
    (if (.isClosed ^Connection (.-conn c))
      (recur pool)
      c)))

(defn- new-conn ^Conn [^String url]
  (let [conn (DriverManager/getConnection url)
        turso? (turso-url? url)]
    (when turso?
      ;; no URL parameters in Turso's driver: the path is everything after the prefix
      (with-open [st (.createStatement conn)]
        (.execute st "PRAGMA busy_timeout = 5000")
        (.execute st "PRAGMA cache_size = -16384")))
    (->Conn conn (HashMap.) turso?)))

(defn- borrow ^Conn [^Pool pool]
  (check-open pool)
  (.acquire ^Semaphore (.-permits pool))
  (try
    (check-open pool)
    (or (idle-conn pool)
        (new-conn (.-url pool)))
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
