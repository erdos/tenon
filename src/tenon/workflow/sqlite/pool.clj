(ns tenon.workflow.sqlite.pool
  "A small SQLite connection pool whose connections keep their prepared statements."
  (:require [next.jdbc.prepare :as prepare]
            [next.jdbc.protocols :as p]
            [next.jdbc.result-set :as rs])
  (:import [java.io Closeable]
           [java.sql Connection DriverManager PreparedStatement ResultSet]
           [java.util HashMap Map]
           [java.util.concurrent CompletableFuture ConcurrentLinkedQueue ExecutionException
            LinkedBlockingQueue Semaphore]
           [java.util.concurrent.locks ReentrantLock]
           [java.util.concurrent.atomic AtomicBoolean]))

(defn- close-quietly [^java.lang.AutoCloseable c]
  (try (.close c) (catch Throwable _)))

(defn- with-statement [^Connection conn ^Map stmts ^String sql params f]
  (let [^PreparedStatement ps (or (.get stmts sql)
                                  (let [ps (.prepareStatement conn sql)] (.put stmts sql ps) ps))]
    (try
      (f (prepare/set-parameters ps params))
      (catch Throwable e
        (.remove stmts sql)
        (close-quietly ps)
        (throw e)))))

(defn- result-set ^ResultSet [^PreparedStatement ps opts]
  (if (.execute ps)
    (.getResultSet ps)
    (when (:return-keys opts)
      (try (.getGeneratedKeys ps) (catch Exception _)))))

(defn- first-row [^Connection conn ^PreparedStatement ps opts]
  (if-let [^ResultSet r (result-set ps opts)]
    (try
      (reduce (fn [_ row] (reduced (rs/datafiable-row row conn opts)))
              nil
              (rs/reducible-result-set r opts))
      (finally (close-quietly r)))
    {:next.jdbc/update-count (.getUpdateCount ps)}))

(deftype Conn [^Connection conn ^Map stmts]
  p/Executable
  (-execute [_ sql-params opts]
    ;; a reducible outlives this call: no caching
    (p/-execute conn sql-params opts))
  (-execute-one [_ [sql & params] opts]
    (with-statement conn stmts sql params
      #(first-row conn % (merge {:return-keys false} opts))))
  (-execute-all [_ [sql & params] opts]
    (with-statement conn stmts sql params
      #(p/-execute-all % nil (merge {:return-keys false} opts))))
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

(deftype Pool [^String url ^LinkedBlockingQueue idle ^Semaphore permits ^AtomicBoolean closed
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
  (Pool. url (LinkedBlockingQueue.) (Semaphore. size) (AtomicBoolean. false) (ReentrantLock.)
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
  (when-let [^Conn c (.poll ^LinkedBlockingQueue (.-idle pool))]
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
    (do (.offer ^LinkedBlockingQueue (.-idle pool) c)
        ;; closed meanwhile: do not leave it behind
        (when (.get ^AtomicBoolean (.-closed pool))
          (some-> (.poll ^LinkedBlockingQueue (.-idle pool)) discard))))
  (.release ^Semaphore (.-permits pool)))
