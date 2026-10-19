(ns tenon.workflow.sqlite.pool
  "A small SQLite connection pool whose connections keep their prepared statements."
  (:require [next.jdbc.prepare :as prepare]
            [next.jdbc.protocols :as p]
            [next.jdbc.result-set :as rs])
  (:import [java.io Closeable]
           [java.sql Connection DriverManager PreparedStatement ResultSet]
           [java.util HashMap Map]
           [java.util.concurrent LinkedBlockingQueue Semaphore]
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
               ^ReentrantLock write-lock]
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
  (Pool. url (LinkedBlockingQueue.) (Semaphore. size) (AtomicBoolean. false) (ReentrantLock. true)))

(defn write!
  "Calls (f) holding the pool's write lock. SQLite runs one writer at a
   time anyway, and a writer finding the database locked backs off by
   sleeping (busy_timeout) - so writers of this process queue up here
   instead, in arrival order, and each starts the moment the last one is
   done. Writers of other processes still meet at SQLite's lock."
  [^Pool pool f]
  (let [^ReentrantLock lock (.-write-lock pool)]
    (.lock lock)
    (try (f) (finally (.unlock lock)))))

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
