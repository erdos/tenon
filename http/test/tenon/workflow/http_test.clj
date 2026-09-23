(ns tenon.workflow.http-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.string :as str]
            [ring.mock.request :as mock]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.json :refer [wrap-json-response]]
            [cheshire.core :as json]
            [tenon.workflow.http :as http]
            [tenon.workflow :as engine]
            [tenon.workflow.db :as db]
            [tenon.workflow.test-util :as test-util :refer [temp-db-fixture]]))

(use-fixtures :each temp-db-fixture)

(def ^:private http-flaky-fn (fn [] (throw (ex-info "fail" {}))))

(deftest pending-endpoint-test
  (let [id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) id "test.ns/crashed" (pr-str [1 2]) nil nil)
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/workflows/pending"))
          body (json/parse-string (:body response) true)]
      (is (= 200 (:status response)))
      (is (some #(= id (:id %)) body)))))

(deftest pending-endpoint-handles-invalid-arguments-edn-test
  ;; A single row with unreadable :arguments must not fail the whole
  ;; response - it should fall back to :arguments_raw while every other
  ;; (valid) row still comes through normally.
  (let [good-id (str (java.util.UUID/randomUUID))
        bad-id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) good-id "test.ns/good" (pr-str [1 2]) nil nil)
    (db/insert-event! (test-util/ds) good-id "STARTED" nil)
    (db/insert-workflow! (test-util/ds) bad-id "test.ns/bad" "(1 2" nil nil)
    (db/insert-event! (test-util/ds) bad-id "STARTED" nil)
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/workflows/pending"))
          body (json/parse-string (:body response) true)]
      (is (= 200 (:status response)))
      (is (some #(= good-id (:id %)) body))
      (let [bad-row (first (filter #(= bad-id (:id %)) body))]
        (is (some? bad-row))
        (is (contains? bad-row :arguments_raw))))))

(deftest pending-endpoint-returns-500-on-unexpected-error-test
  (with-redefs [engine/list-pending (fn [] (throw (ex-info "kaboom" {})))]
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/workflows/pending"))]
      (is (= 500 (:status response))))))

(deftest restart-endpoint-test
  (engine/register! #'http-flaky-fn http-flaky-fn)
  (is (thrown? clojure.lang.ExceptionInfo
        (engine/run-invocation #'http-flaky-fn http-flaky-fn [])))
  (let [id (:id (first (test-util/find-by-wf-def (test-util/ds) "tenon.workflow.http-test/http-flaky-fn")))]
    (engine/register! #'http-flaky-fn (fn [] :ok))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :post (str "/workflows/" id "/restart")))
          body (json/parse-string (:body response) true)]
      (is (= 200 (:status response)))
      (is (= "DONE" (:state body))))))

(deftest restart-endpoint-precondition-failure-test
  (let [response ((http/app engine/*workflow-engine*) (mock/request :post "/workflows/unknown-id/restart"))]
    (is (= 409 (:status response)))))

(deftest dashboard-lists-top-level-workflows-test
  (let [id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) id "test.ns/top" (pr-str [1]) nil nil)
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (db/insert-event! (test-util/ds) id "DONE" (pr-str :ok))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/"))]
      (is (= 200 (:status response)))
      (is (re-find #"text/html" (get-in response [:headers "Content-Type"])))
      (is (re-find #"test\.ns/top" (:body response)))
      (is (re-find (re-pattern (str "/workflows/" id)) (:body response))))))

(deftest dashboard-excludes-nested-workflows-test
  (let [parent-id (str (java.util.UUID/randomUUID))
        child-id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) parent-id "test.ns/parent" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) parent-id "STARTED" nil)
    (db/insert-workflow! (test-util/ds) child-id "test.ns/child" (pr-str []) parent-id nil)
    (db/insert-event! (test-util/ds) child-id "STARTED" nil)
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/"))]
      (is (re-find #"test\.ns/parent" (:body response)))
      (is (not (re-find #"test\.ns/child" (:body response)))))))

(deftest dashboard-shows-nested-workflows-with-all-checkbox-test
  (let [parent-id (str (java.util.UUID/randomUUID))
        child-id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) parent-id "test.ns/parent4" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) parent-id "STARTED" nil)
    (db/insert-workflow! (test-util/ds) child-id "test.ns/child4" (pr-str []) parent-id nil)
    (db/insert-event! (test-util/ds) child-id "STARTED" nil)
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/" {:all "1"}))
          body (:body response)]
      (is (re-find #"test\.ns/parent4" body))
      (is (re-find #"test\.ns/child4" body)
          "checked all=1 must include sub-workflows too")
      (is (re-find #"<input checked name=\"all\"" body)
          "the checkbox itself must reflect the checked state"))))

(deftest dashboard-defaults-to-all-states-test
  (let [done-id (str (java.util.UUID/randomUUID))
        pending-id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) done-id "test.ns/done-wf" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) done-id "STARTED" nil)
    (db/insert-event! (test-util/ds) done-id "DONE" (pr-str :ok))
    (db/insert-workflow! (test-util/ds) pending-id "test.ns/pending-wf" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) pending-id "STARTED" nil)
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/"))]
      (is (re-find #"test\.ns/done-wf" (:body response)))
      (is (re-find #"test\.ns/pending-wf" (:body response)))
      (is (re-find #"<option selected value=\"\">All</option>" (:body response))))))

(deftest dashboard-filters-by-state-query-param-test
  (let [done-id (str (java.util.UUID/randomUUID))
        pending-id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) done-id "test.ns/done-wf2" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) done-id "STARTED" nil)
    (db/insert-event! (test-util/ds) done-id "DONE" (pr-str :ok))
    (db/insert-workflow! (test-util/ds) pending-id "test.ns/pending-wf2" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) pending-id "STARTED" nil)
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/" {:state "DONE"}))]
      (is (re-find (re-pattern (str "/workflows/" done-id)) (:body response)))
      (is (not (re-find (re-pattern (str "/workflows/" pending-id)) (:body response)))
          "the STARTED row must be filtered out of the table - it may still appear as a dropdown option")
      (is (re-find #"<option selected value=\"DONE\">DONE</option>" (:body response))))))

(deftest dashboard-filters-by-wf-def-query-param-test
  (let [a-id (str (java.util.UUID/randomUUID))
        b-id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) a-id "test.ns/name-a" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) a-id "STARTED" nil)
    (db/insert-workflow! (test-util/ds) b-id "test.ns/name-b" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) b-id "STARTED" nil)
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/" {:wf_def "test.ns/name-a"}))]
      (is (re-find (re-pattern (str "/workflows/" a-id)) (:body response)))
      (is (not (re-find (re-pattern (str "/workflows/" b-id)) (:body response))))
      (is (re-find #"<option selected value=\"test\.ns/name-a\">test\.ns/name-a</option>" (:body response)))
      (is (re-find #"<option value=\"test\.ns/name-b\">test\.ns/name-b</option>" (:body response))
          "the other name still appears as an unselected dropdown option"))))

(defn- insert-n-workflows! [n prefix]
  (dotimes [i n]
    (let [id (str prefix "-" i)]
      (db/insert-workflow! (test-util/ds) id (str "test.ns/" prefix i) (pr-str [i]) nil nil)
      (db/insert-event! (test-util/ds) id "STARTED" nil))))

(deftest dashboard-defaults-to-page-size-100-with-no-next-page-link-for-small-result-sets-test
  (insert-n-workflows! 5 "small")
  (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/"))]
    (is (re-find #"<option selected value=\"100\">100</option>" (:body response)))
    (is (not (re-find #"before_ts" (:body response)))
        "fewer rows than the page size - no next page")))

(deftest dashboard-paginates-with-seek-based-next-page-link-test
  (insert-n-workflows! 105 "page")
  (let [page1 ((http/app engine/*workflow-engine*) (mock/request :get "/" {:page_size "100"}))
        body1 (:body page1)
        row-count (fn [body] (count (re-seq #"class=\"state-STARTED\"" body)))]
    (is (= 100 (row-count body1)) "capped at the requested page size")
    (is (re-find #"<option selected value=\"100\">100</option>" body1))
    (let [[_ href] (re-find #"<a href=\"([^\"]*before_ts[^\"]*)\"" body1)]
      (is (some? href) "a next-page link with a before_ts/before_id cursor is present")
      (let [href (str/replace href "&amp;" "&")
            [path qs] (str/split href #"\?" 2)
            page2 ((http/app engine/*workflow-engine*) (assoc (mock/request :get path) :query-string qs))
            body2 (:body page2)]
        (is (= 5 (row-count body2)) "the remaining rows, none repeated or skipped")
        (is (not (re-find #"before_ts" body2))
            "no further next-page link once the last page is reached")))))

(deftest workflow-detail-page-shows-arguments-and-result-test
  (let [id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) id "test.ns/detail" (pr-str [1 2]) nil nil)
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (db/insert-event! (test-util/ds) id "DONE" (pr-str 42))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id)))]
      (is (= 200 (:status response)))
      (is (re-find #"\[1 2\]" (:body response)))
      (is (re-find #"42" (:body response))))))

(deftest workflow-detail-page-id-line-is-a-real-link-test
  (let [id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) id "test.ns/id-link" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
      (is (re-find (re-pattern (str "<b>Id: </b><a href=\"/workflows/" id "\""))
                   body))
      (is (re-find #"text-decoration:underline" body)))))

(deftest workflow-detail-page-links-to-parent-workflow-test
  (let [parent-id (str (java.util.UUID/randomUUID))
        child-id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) parent-id "test.ns/parent-link" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) parent-id "STARTED" nil)
    (db/insert-workflow! (test-util/ds) child-id "test.ns/child-link" (pr-str []) parent-id nil)
    (db/insert-event! (test-util/ds) child-id "STARTED" nil)
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" child-id))))]
      (is (re-find #"<b>Parent: </b>" body))
      (is (re-find (re-pattern (str "href=\"/workflows/" parent-id "\"")) body)))))

(deftest workflow-detail-page-hides-parent-link-for-top-level-workflow-test
  (let [id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) id "test.ns/top-level-no-parent" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
      (is (not (re-find #"<b>Parent: </b>" body))))))

(deftest workflow-detail-page-shows-metadata-test
  (let [id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) id "test.ns/with-meta" (pr-str [1 2]) nil (pr-str {:actor-id 42}))
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id)))]
      (is (re-find #"<h2>Metadata</h2>" (:body response)))
      (is (re-find #"actor-id 42" (:body response))))))

(deftest workflow-detail-page-hides-metadata-section-when-absent-test
  (let [id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) id "test.ns/no-meta" (pr-str [1 2]) nil nil)
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id)))]
      (is (not (re-find #"<h2>Metadata</h2>" (:body response)))))))

(deftest workflow-detail-page-shows-error-test
  (let [id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) id "test.ns/failed" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (db/insert-event! (test-util/ds) id "ERROR" (pr-str {:message "boom" :class "clojure.lang.ExceptionInfo" :data nil}))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id)))]
      (is (= 200 (:status response)))
      (is (re-find #"boom" (:body response))))))

(deftest workflow-detail-page-shows-full-timeline-test
  ;; A restarted invocation appends more events to the same row - the
  ;; timeline must show every one of them, in order, not just the latest.
  (let [id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) id "test.ns/timeline" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (db/insert-event! (test-util/ds) id "ERROR" (pr-str {:message "first failure" :class "clojure.lang.ExceptionInfo" :data nil}))
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (db/insert-event! (test-util/ds) id "DONE" (pr-str :recovered))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id)))
          events (test-util/get-events (test-util/ds) id)]
      (is (= 4 (count events)))
      (is (re-find #"first failure" (:body response)))
      (is (re-find #"recovered" (:body response)))
      (is (= 4 (count (re-seq #"class=\"state-(?:STARTED|DONE|ERROR)\"" (:body response))))
          "one timeline row per event"))))

(deftest workflow-detail-page-interleaves-sub-workflow-timeline-test
  ;; Sub-workflow events (recursively, at any depth) must appear as rows in
  ;; the same single timeline table as the workflow's own events - not in
  ;; a separate table - interleaved in chronological order.
  (let [parent-id (str (java.util.UUID/randomUUID))
        child-id (str (java.util.UUID/randomUUID))
        grandchild-id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) parent-id "test.ns/parent2" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) parent-id "STARTED" nil)
    (db/insert-workflow! (test-util/ds) child-id "test.ns/child2" (pr-str []) parent-id nil)
    (db/insert-event! (test-util/ds) child-id "STARTED" nil)
    (db/insert-workflow! (test-util/ds) grandchild-id "test.ns/grandchild2" (pr-str []) child-id nil)
    (db/insert-event! (test-util/ds) grandchild-id "STARTED" nil)
    (db/insert-event! (test-util/ds) grandchild-id "DONE" (pr-str :ok))
    (db/insert-event! (test-util/ds) child-id "DONE" (pr-str :ok))
    (db/insert-event! (test-util/ds) parent-id "DONE" (pr-str :ok))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" parent-id)))
          body (:body response)
          timeline (db/full-timeline (test-util/ds) parent-id)]
      (is (= 6 (count timeline))
          "parent's own 2 events + child's 2 + grandchild's 2")
      (is (re-find (re-pattern (str "/workflows/" child-id)) body))
      (is (re-find (re-pattern (str "/workflows/" grandchild-id)) body)
          "the grandchild's events are pulled in too, not just direct children")
      (is (re-find #"test\.ns/child2" body))
      (is (re-find #"test\.ns/grandchild2" body))
      (is (= 6 (count (re-seq #"class=\"state-(?:STARTED|DONE|ERROR)\"" body)))
          "a single merged table, not a separate sub-workflows table"))))

(deftest workflow-detail-page-timeline-rows-carry-wf-id-attribute-test
  ;; Each timeline row needs data-tenon-wf-id so a hover script can find and
  ;; highlight every other row belonging to the same workflow.
  (let [parent-id (str (java.util.UUID/randomUUID))
        child-id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) parent-id "test.ns/hoverable" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) parent-id "STARTED" nil)
    (db/insert-event! (test-util/ds) parent-id "DONE" (pr-str :ok))
    (db/insert-workflow! (test-util/ds) child-id "test.ns/hoverable-child" (pr-str []) parent-id nil)
    (db/insert-event! (test-util/ds) child-id "STARTED" nil)
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" parent-id))))]
      (is (= 2 (count (re-seq (re-pattern (str "data-tenon-wf-id=\"" parent-id "\"")) body)))
          "both of the parent's own events carry its id")
      (is (re-find (re-pattern (str "data-tenon-wf-id=\"" child-id "\"")) body))
      (is (re-find #"addEventListener\('mouseover'" body))
      (is (re-find #"addEventListener\('mouseout'" body)))))

(deftest workflow-detail-page-indents-name-by-depth-test
  (let [parent-id (str (java.util.UUID/randomUUID))
        child-id (str (java.util.UUID/randomUUID))
        grandchild-id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) parent-id "test.ns/parent3" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) parent-id "STARTED" nil)
    (db/insert-workflow! (test-util/ds) child-id "test.ns/child3" (pr-str []) parent-id nil)
    (db/insert-event! (test-util/ds) child-id "STARTED" nil)
    (db/insert-workflow! (test-util/ds) grandchild-id "test.ns/grandchild3" (pr-str []) child-id nil)
    (db/insert-event! (test-util/ds) grandchild-id "STARTED" nil)
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" parent-id))))]
      (is (re-find (re-pattern (str ">" "<code>test\\.ns/parent3</code>")) body)
          "depth 0 - no leading padding")
      (is (re-find (re-pattern (str ">  " "<code>test\\.ns/child3</code>")) body)
          "depth 1 - two nbsp of left padding")
      (is (re-find (re-pattern (str ">    " "<code>test\\.ns/grandchild3</code>")) body)
          "depth 2 - four nbsp of left padding"))))

(deftest workflow-detail-page-404-for-unknown-id-test
  (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/workflows/does-not-exist"))]
    (is (= 404 (:status response)))))

(deftest workflow-detail-page-shows-restart-button-when-restartable-test
  (engine/register! #'http-flaky-fn http-flaky-fn)
  (is (thrown? clojure.lang.ExceptionInfo
        (engine/run-invocation #'http-flaky-fn http-flaky-fn [])))
  (let [id (:id (first (test-util/find-by-wf-def (test-util/ds) "tenon.workflow.http-test/http-flaky-fn")))
        body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
    (is (re-find (re-pattern (str "tenonRestart\\(&apos;/workflows/" id "/restart&apos;\\)")) body))))

(deftest workflow-detail-page-hides-restart-button-when-pending-test
  ;; tenonRestart's definition is in the page's shared script regardless -
  ;; check for the absence of the <button> itself, not that substring.
  (let [id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) id "test.ns/still-running" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
      (is (not (re-find #"<button" body))))))

(deftest workflow-detail-page-hides-restart-button-when-wf-def-unregistered-test
  (let [id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) id "test.ns/never-registered-anywhere" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (db/insert-event! (test-util/ds) id "DONE" (pr-str :ok))
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
      (is (not (re-find #"<button" body))))))

(defn- wrapped-app [prefix original-handler]
  (-> (http/wrap-handler engine/*workflow-engine* prefix original-handler) wrap-params wrap-json-response))

(deftest wrap-handler-delegates-non-prefixed-uris-test
  (let [original (constantly {:status 200 :body "from the original app"})
        app (wrapped-app "/tenon" original)]
    (is (= "from the original app" (:body (app (mock/request :get "/other")))))
    (is (= "from the original app" (:body (app (mock/request :get "/")))))))

(deftest wrap-handler-does-not-match-a-uri-that-merely-starts-with-the-prefix-test
  ;; "/tenonx" must not be treated as under prefix "/tenon" - only an exact
  ;; match or a "/tenon/..." path should route to tenon.
  (let [original (constantly {:status 200 :body "from the original app"})
        app (wrapped-app "/tenon" original)]
    (is (= "from the original app" (:body (app (mock/request :get "/tenonx")))))))

(deftest wrap-handler-routes-prefixed-uris-to-tenon-test
  (let [original (constantly {:status 200 :body "from the original app"})
        app (wrapped-app "/tenon" original)]
    (is (re-find #"<h1>Workflows</h1>" (:body (app (mock/request :get "/tenon")))))
    (let [id (str (java.util.UUID/randomUUID))]
      (db/insert-workflow! (test-util/ds) id "test.ns/prefixed" (pr-str []) nil nil)
      (db/insert-event! (test-util/ds) id "STARTED" nil)
      (let [body (:body (app (mock/request :get "/tenon/workflows/pending")))]
        (is (some #(= id (:id %)) (json/parse-string body true)))))))

(deftest wrap-handler-generates-prefixed-links-test
  (let [original (constantly {:status 200 :body "unreachable"})
        app (wrapped-app "/tenon" original)
        id (str (java.util.UUID/randomUUID))]
    (db/insert-workflow! (test-util/ds) id "test.ns/prefixed-link" (pr-str []) nil nil)
    (db/insert-event! (test-util/ds) id "STARTED" nil)
    (let [dashboard-body (:body (app (mock/request :get "/tenon")))
          detail-body (:body (app (mock/request :get (str "/tenon/workflows/" id))))]
      (is (re-find (re-pattern (str "href=\"/tenon/workflows/" id "\"")) dashboard-body))
      (is (re-find #"action=\"/tenon/\"" dashboard-body))
      (is (re-find (re-pattern (str "href=\"/tenon/workflows/" id "\"")) detail-body)
          "the workflow's own timeline row also links back through the prefix"))))

(deftest wrap-handler-with-empty-prefix-behaves-like-root-mount-test
  (let [original (constantly {:status 200 :body "unreachable"})
        app (wrapped-app "" original)]
    (is (re-find #"<h1>Workflows</h1>" (:body (app (mock/request :get "/")))))))
