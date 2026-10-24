(ns tenon.workflow.versioning-test
  "Workflow versions (:version metadata, part of a workflow's identity) and
   the system metadata recorded with each invocation (code hash, arglists)."
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [matcher-combinators.test :refer [match? thrown-match?]]
            [tenon.workflow :as engine]
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
                                                :tenon/metadata-fn (constantly nil))]
        (f)))))

(defn ^:private versioned-fn [x] x)
(defn ^:private arity-fn [x] x)

(defn- set-version! [v version]
  (alter-meta! v #(if (some? version) (assoc % :version version) (dissoc % :version))))

(defn- rows [wf-def]
  (vec (test-util/find-by-wf-def wf-def)))

(defn- system-metadata [invocation-id]
  (some-> (engine/get-invocation invocation-id) :workflow/system-metadata edn/read-string))

(defn- events-of [event]
  (filterv #(= event (first %)) @*emitted-events*))

(deftest code-hash-test
  (testing "is the same each time the code is read, though auto-generated symbols differ"
    (let [src "(fn [x] (map #(inc %) `(a# ~x)))"]
      (is (not= (read-string src) (read-string src)) "the reads differ")
      (is (= (engine/code-hash (read-string src)) (engine/code-hash (read-string src))))))
  (testing "ignores metadata, e.g. the position in the file"
    (is (= (engine/code-hash '(fn [x] (inc x)))
           (engine/code-hash (with-meta '(fn [x] (inc x)) {:line 42 :column 1})))))
  (testing "differs for different code"
    (is (not= (engine/code-hash '(fn [x] (inc x))) (engine/code-hash '(fn [x] (dec x)))))))

(deftest reader-records-system-metadata-test
  (is (= 3 (fixtures/add-numbers 1 2)))
  (let [id (:workflow/invocation-id (first (rows "tenon.workflow.fixtures/add-numbers")))
        sm (system-metadata id)]
    (is (match? {:code-hash #"^[0-9a-f]{64}$" :arglists '([a b])} sm))
    (is (= (::engine/code-hash (meta #'fixtures/add-numbers)) (:code-hash sm)))
    (is (nil? (:workflow/version (engine/get-invocation id))) "unversioned")))

(deftest docstring-and-attr-map-are-not-code-test
  ;; with-docstring's docstring and attr-map aren't part of its fn form.
  (let [hash-of #(-> (engine/workflow (read-string %)) last last)]
    (is (= (hash-of "(defn f \"Doc one.\" {:a 1} [x] (inc x))")
           (hash-of "(defn f \"Doc two.\" {:a 2 :version 9} [x] (inc x))")))
    (is (not= (hash-of "(defn f [x] (inc x))") (hash-of "(defn f [x] (dec x))")))))

(deftest version-is-part-of-identity-test
  (set-version! #'versioned-fn 1)
  (let [calls (atom 0)
        impl (fn [x] (swap! calls inc) [:v @calls x])]
    (is (= [:v 1 :a] (engine/run-invocation #'versioned-fn impl [:a])))
    (is (= [:v 1 :a] (engine/run-invocation #'versioned-fn impl [:a])) "same version: reused")
    (set-version! #'versioned-fn 2)
    (is (= [:v 2 :a] (engine/run-invocation #'versioned-fn impl [:a])) "bumped: run anew")
    (is (= [:v 2 :a] (engine/run-invocation #'versioned-fn impl [:a])))
    (set-version! #'versioned-fn 1)
    (is (= [:v 1 :a] (engine/run-invocation #'versioned-fn impl [:a])) "the old version's result is still there")
    (set-version! #'versioned-fn nil)
    (is (= [:v 3 :a] (engine/run-invocation #'versioned-fn impl [:a])) "unversioned is a version of its own")
    (is (= 3 @calls))
    (is (= #{"1" "2" nil} (set (map :workflow/version (rows "tenon.workflow.versioning-test/versioned-fn")))))
    (is (match? [[:started {:version 1}] [:started {:version 2}] [:started {:version nil}]]
                (events-of :started)))))

(deftest unversioned-rows-predating-versions-are-reused-test
  ;; Rows from before versions existed are unversioned - and so are the
  ;; fns that never got a :version.
  (set-version! #'versioned-fn nil)
  (let [id (test-util/insert! "tenon.workflow.versioning-test/versioned-fn" :params "[:a]")]
    (test-util/finish! id "DONE" ":stored")
    (is (= :stored (engine/run-invocation #'versioned-fn (fn [_] :fresh) [:a])))))

(deftest code-change-is-warned-about-but-reused-test
  (set-version! #'versioned-fn nil)
  (engine/register! #'versioned-fn (fn [x] [:old x]) "hash-1")
  (is (= [:old 1] (engine/run-invocation #'versioned-fn (engine/raw-fn #'versioned-fn) [1])))
  (let [id (:workflow/invocation-id (first (rows "tenon.workflow.versioning-test/versioned-fn")))]
    (is (= "hash-1" (:code-hash (system-metadata id))))
    (testing "the same code: no warning"
      (is (= [:old 1] (engine/run-invocation #'versioned-fn (engine/raw-fn #'versioned-fn) [1])))
      (is (empty? (events-of :code-changed))))
    (testing "changed code: the stored result is reused, with a warning"
      (engine/register! #'versioned-fn (fn [x] [:new x]) "hash-2")
      (is (= [:old 1] (engine/run-invocation #'versioned-fn (engine/raw-fn #'versioned-fn) [1])))
      (is (match? [[:code-changed {:invocation-id id
                                   :wf-def "tenon.workflow.versioning-test/versioned-fn"
                                   :version nil
                                   :recorded-code-hash "hash-1"
                                   :code-hash "hash-2"}]]
                  (events-of :code-changed))))
    (testing "unknown code (registered without a hash): nothing to compare"
      (engine/register! #'versioned-fn (fn [x] [:other x]))
      (is (nil? (::engine/code-hash (meta #'versioned-fn))) "the stale hash is dropped")
      (is (= [:old 1] (engine/run-invocation #'versioned-fn (engine/raw-fn #'versioned-fn) [1])))
      (is (= 1 (count (events-of :code-changed)))))))

(deftest restart-records-the-new-code-test
  (set-version! #'versioned-fn nil)
  (engine/register! #'versioned-fn (fn [_] (throw (ex-info "boom" {}))) "hash-1")
  (is (thrown? clojure.lang.ExceptionInfo (engine/run-invocation #'versioned-fn (engine/raw-fn #'versioned-fn) [1])))
  (let [id (:workflow/invocation-id (first (rows "tenon.workflow.versioning-test/versioned-fn")))]
    (engine/register! #'versioned-fn (fn [x] [:fixed x]) "hash-2")
    (is (= [:fixed 1] (engine/restart-invocation id)))
    (is (= "hash-2" (:code-hash (system-metadata id))))
    (is (= [:fixed 1] (engine/run-invocation #'versioned-fn (engine/raw-fn #'versioned-fn) [1])))
    (is (empty? (events-of :code-changed)) "the result was recorded by the code at hand")))

(deftest restart-refuses-another-version-test
  (set-version! #'versioned-fn 1)
  (engine/register! #'versioned-fn (fn [_] (throw (ex-info "boom" {}))))
  (is (thrown? clojure.lang.ExceptionInfo (engine/run-invocation #'versioned-fn (engine/raw-fn #'versioned-fn) [1])))
  (let [id (:workflow/invocation-id (first (rows "tenon.workflow.versioning-test/versioned-fn")))]
    (engine/register! #'versioned-fn (fn [x] [:fixed x]))
    (set-version! #'versioned-fn 2)
    (is (thrown-match? clojure.lang.ExceptionInfo
                       {:type ::engine/version-mismatch :recorded-version 1 :version 2}
                       (engine/restart-invocation id)))
    (is (= "ERROR" (:workflow/state (engine/get-invocation id))) "left as it was")
    (testing "the new version runs when called"
      (is (= [:fixed 1] (engine/run-invocation #'versioned-fn (engine/raw-fn #'versioned-fn) [1]))))
    (testing "back at the recorded version, it restarts"
      (set-version! #'versioned-fn 1)
      (is (= [:fixed 1] (engine/restart-invocation id)))
      (is (match? [:restarted {:version 1}] (first (events-of :restarted)))))))

(deftest restart-refuses-a-signature-the-args-dont-fit-test
  (alter-meta! #'arity-fn assoc :arglists '([x]))
  (engine/register! #'arity-fn (fn [_] (throw (ex-info "boom" {}))))
  (is (thrown? clojure.lang.ExceptionInfo (engine/run-invocation #'arity-fn (engine/raw-fn #'arity-fn) [1])))
  (let [id (:workflow/invocation-id (first (rows "tenon.workflow.versioning-test/arity-fn")))]
    (is (= {:arglists '([x])} (system-metadata id)))
    (alter-meta! #'arity-fn assoc :arglists '([x y]))
    (engine/register! #'arity-fn (fn [x y] [x y]))
    (is (thrown-match? clojure.lang.ExceptionInfo
                       {:type ::engine/signature-mismatch :args [1] :arglists '([x y])}
                       (engine/restart-invocation id)))
    (is (= "ERROR" (:workflow/state (engine/get-invocation id))))
    (testing "renamed parameters, or a variadic list the args fit, restart"
      (alter-meta! #'arity-fn assoc :arglists '([a b] [y & more]))
      (engine/register! #'arity-fn (fn [& xs] (vec xs)))
      (is (= [1] (engine/restart-invocation id)))
      (is (= {:arglists '([a b] [y & more])} (system-metadata id))))))
