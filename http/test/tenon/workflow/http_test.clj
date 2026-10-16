(ns tenon.workflow.http-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.string :as str]
            [ring.mock.request :as mock]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.json :refer [wrap-json-response]]
            [cheshire.core :as json]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [tenon.workflow.http :as http]
            [tenon.workflow :as engine]
            [tenon.workflow.test-util :as test-util :refer [temp-db-fixture]]))

(use-fixtures :each temp-db-fixture)

(def ^:private http-flaky-fn (fn [] (throw (ex-info "fail" {}))))

(deftest pending-endpoint-test
  (let [id (test-util/insert! "test.ns/crashed" :params (pr-str [1 2]))
        response ((http/app engine/*workflow-engine*) (mock/request :get "/workflows/pending"))
        body (json/parse-string (:body response) true)]
    (is (= 200 (:status response)))
    (is (some #(= id (:invocation_id %)) body))))

(deftest pending-endpoint-handles-invalid-params-edn-test
  ;; A single row with unreadable :params must not fail the whole
  ;; response - it should fall back to :params_raw while every other
  ;; (valid) row still comes through normally.
  (let [good-id (test-util/insert! "test.ns/good" :params (pr-str [1 2]))
        bad-id (test-util/insert! "test.ns/bad" :params "(1 2")
        response ((http/app engine/*workflow-engine*) (mock/request :get "/workflows/pending"))
        body (json/parse-string (:body response) true)]
    (is (= 200 (:status response)))
    (is (some #(= good-id (:invocation_id %)) body))
    (let [bad-row (first (filter #(= bad-id (:invocation_id %)) body))]
      (is (some? bad-row))
      (is (contains? bad-row :params_raw)))))

(deftest pending-endpoint-returns-500-on-unexpected-error-test
  (with-redefs [engine/list-pending (fn [] (throw (ex-info "kaboom" {})))]
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/workflows/pending"))]
      (is (= 500 (:status response))))))

(deftest restart-endpoint-test
  (engine/register! #'http-flaky-fn http-flaky-fn)
  (is (thrown? clojure.lang.ExceptionInfo
        (engine/run-invocation #'http-flaky-fn http-flaky-fn [])))
  (let [id (:invocation_id (first (test-util/find-by-wf-def "tenon.workflow.http-test/http-flaky-fn")))]
    (engine/register! #'http-flaky-fn (fn [] :ok))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :post (str "/workflows/" id "/restart")))
          body (json/parse-string (:body response) true)]
      (is (= 200 (:status response)))
      (is (= "DONE" (:state body))))))

(deftest restart-endpoint-precondition-failure-test
  (let [response ((http/app engine/*workflow-engine*) (mock/request :post "/workflows/unknown-id/restart"))]
    (is (= 409 (:status response)))))

(deftest dashboard-lists-top-level-workflows-test
  (let [id (test-util/insert! "test.ns/top" :params (pr-str [1]))]
    (test-util/finish! id "DONE" (pr-str :ok))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/"))]
      (is (= 200 (:status response)))
      (is (re-find #"text/html" (get-in response [:headers "Content-Type"])))
      (is (re-find #"test\.ns/top" (:body response)))
      (is (re-find (re-pattern (str "/workflows/" id)) (:body response))))))

(deftest dashboard-excludes-nested-workflows-test
  (let [parent-id (test-util/insert! "test.ns/parent")
        _ (test-util/insert! "test.ns/child" :parent parent-id)
        response ((http/app engine/*workflow-engine*) (mock/request :get "/"))]
    (is (re-find #"test\.ns/parent" (:body response)))
    (is (not (re-find #"test\.ns/child" (:body response))))))

(deftest dashboard-shows-nested-workflows-with-all-checkbox-test
  (let [parent-id (test-util/insert! "test.ns/parent4")
        _ (test-util/insert! "test.ns/child4" :parent parent-id)
        response ((http/app engine/*workflow-engine*) (mock/request :get "/" {:all "1"}))
        body (:body response)]
    (is (re-find #"test\.ns/parent4" body))
    (is (re-find #"test\.ns/child4" body)
        "checked all=1 must include sub-workflows too")
    (is (re-find #"<input checked name=\"all\"" body)
        "the checkbox itself must reflect the checked state")))

(deftest dashboard-defaults-to-all-states-test
  (let [done-id (test-util/insert! "test.ns/done-wf")
        _ (test-util/insert! "test.ns/pending-wf")]
    (test-util/finish! done-id "DONE" (pr-str :ok))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/"))]
      (is (re-find #"test\.ns/done-wf" (:body response)))
      (is (re-find #"test\.ns/pending-wf" (:body response)))
      (is (re-find #"<option selected value=\"\">All</option>" (:body response))))))

(deftest dashboard-filters-by-state-query-param-test
  (let [done-id (test-util/insert! "test.ns/done-wf2")
        pending-id (test-util/insert! "test.ns/pending-wf2")]
    (test-util/finish! done-id "DONE" (pr-str :ok))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/" {:state "DONE"}))]
      (is (re-find (re-pattern (str "/workflows/" done-id)) (:body response)))
      (is (not (re-find (re-pattern (str "/workflows/" pending-id)) (:body response)))
          "the STARTED row must be filtered out of the table - it may still appear as a dropdown option")
      (is (re-find #"<option selected value=\"DONE\">DONE</option>" (:body response))))))

(deftest dashboard-filters-by-wf-def-query-param-test
  (let [a-id (test-util/insert! "test.ns/name-a")
        b-id (test-util/insert! "test.ns/name-b")
        response ((http/app engine/*workflow-engine*) (mock/request :get "/" {:wf_def "test.ns/name-a"}))]
    (is (re-find (re-pattern (str "/workflows/" a-id)) (:body response)))
    (is (not (re-find (re-pattern (str "/workflows/" b-id)) (:body response))))
    (is (re-find #"<option selected value=\"test\.ns/name-a\">test\.ns/name-a</option>" (:body response)))
    (is (re-find #"<option value=\"test\.ns/name-b\">test\.ns/name-b</option>" (:body response))
        "the other name still appears as an unselected dropdown option")))

(defn- insert-n-workflows! [n prefix]
  (dotimes [i n]
    (test-util/insert! (str "test.ns/" prefix i) :params (pr-str [i]))))

(deftest dashboard-defaults-to-page-size-100-with-no-next-page-link-for-small-result-sets-test
  (insert-n-workflows! 5 "small")
  (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/"))]
    (is (re-find #"<option selected value=\"100\">100</option>" (:body response)))
    (is (not (re-find #"before_id" (:body response)))
        "fewer rows than the page size - no next page")))

(deftest dashboard-paginates-with-seek-based-next-page-link-test
  (insert-n-workflows! 105 "page")
  (let [page1 ((http/app engine/*workflow-engine*) (mock/request :get "/" {:page_size "100"}))
        body1 (:body page1)
        row-count (fn [body] (count (re-seq #"class=\"state-STARTED\"" body)))]
    (is (= 100 (row-count body1)) "capped at the requested page size")
    (is (re-find #"<option selected value=\"100\">100</option>" body1))
    (let [[_ href] (re-find #"<a href=\"([^\"]*before_id[^\"]*)\"" body1)]
      (is (some? href) "a next-page link with a before_id cursor is present")
      (let [href (str/replace href "&amp;" "&")
            [path qs] (str/split href #"\?" 2)
            page2 ((http/app engine/*workflow-engine*) (assoc (mock/request :get path) :query-string qs))
            body2 (:body page2)]
        (is (= 5 (row-count body2)) "the remaining rows, none repeated or skipped")
        (is (not (re-find #"before_id" body2))
            "no further next-page link once the last page is reached")))))

(deftest workflow-detail-page-shows-params-and-result-test
  (let [id (test-util/insert! "test.ns/detail" :params (pr-str [1 2]))]
    (test-util/finish! id "DONE" (pr-str 42))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id)))]
      (is (= 200 (:status response)))
      (is (str/includes? (:body response) "2 arguments"))
      (is (str/includes? (:body response) "<dt><code>%1</code></dt><dd><pre>1</pre></dd>"))
      (is (str/includes? (:body response) "<dt><code>%2</code></dt><dd><pre>2</pre></dd>"))
      (is (re-find #"(?s)<section class=\"panel\">.*<h2>Result</h2><span class=\"panel-muted\">done</span>.*<pre>42</pre>" (:body response))))))

(deftest workflow-detail-page-shows-no-arguments-and-pending-result-test
  (let [id (test-util/insert! "test.ns/no-args" :params (pr-str []))
        body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
    (is (str/includes? body "no arguments"))
    (is (str/includes? body "Pending \u2014 no result yet."))))

(deftest workflow-detail-page-shows-unreadable-params-raw-test
  (let [id (test-util/insert! "test.ns/unreadable" :params "[#object[Foo 0x1 \"x\"]]")
        body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
    (is (str/includes? body "unreadable EDN, shown raw:"))
    (is (str/includes? body "#object[Foo"))))

(deftest workflow-detail-page-shows-error-panel-test
  (let [id (test-util/insert! "test.ns/error-panel")]
    (test-util/finish! id "ERROR" (pr-str {:message "boom" :class "clojure.lang.ExceptionInfo" :data {:k 1}}))
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
      (is (str/includes? body "<section class=\"panel\"><div class=\"panel-head\"><h2>Error</h2>"))
      (is (str/includes? body "<div class=\"error-class\"><code>clojure.lang.ExceptionInfo</code></div>"))
      (is (str/includes? body "<p class=\"error-message\">boom</p>"))
      (is (str/includes? body "<pre>{:k 1}</pre>")))))

(deftest workflow-detail-page-id-line-is-a-real-link-test
  (let [id (test-util/insert! "test.ns/id-link")
        body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
    (is (re-find (re-pattern (str "<dt>Id</dt><dd><a href=\"/workflows/" id "\""))
                 body))
    (is (re-find #"text-decoration:underline" body))))

(deftest workflow-detail-page-links-to-parent-workflow-test
  (let [parent-id (test-util/insert! "test.ns/parent-link")
        child-id (test-util/insert! "test.ns/child-link" :parent parent-id)
        body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" child-id))))]
    (is (re-find (re-pattern (str "<dt>Parent</dt><dd><a href=\"/workflows/" parent-id "\"")) body))))

(deftest workflow-detail-page-summary-card-test
  ;; Started -> Error, restarted -> Done, with a sub-workflow, reused once:
  ;; the card counts the attempts and reuses, and opening a
  ;; past invocation's id says it shows the current one.
  (let [id (test-util/insert! "test.ns/summary")]
    (test-util/finish! id "ERROR" (pr-str {:message "first" :class "clojure.lang.ExceptionInfo" :data nil}))
    (let [current (test-util/restart! id)
          child (test-util/insert! "test.ns/summary-child" :parent current)]
      (test-util/finish! child "DONE" (pr-str :ok))
      (test-util/finish! current "DONE" (pr-str :ok))
      (test-util/record-reuse! current nil)
      (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
        (is (str/includes? body "<section class=\"wf-summary state-DONE\">"))
        (is (str/includes? body "<dt>Attempts</dt><dd>2</dd>"))
        (is (not (str/includes? body "Sub-workflows")))
        (is (str/includes? body "<dt>Reused</dt><dd>1 time</dd>"))
        (is (str/includes? body "<dt>Finished</dt>"))
        (is (str/includes? body "\u2014 (top level)"))
        (is (re-find (re-pattern (str "Invocation <code>" id "</code> was restarted")) body))))))

(deftest workflow-detail-page-hides-parent-link-for-top-level-workflow-test
  (let [id (test-util/insert! "test.ns/top-level-no-parent")
        body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
    (is (str/includes? body "<dt>Parent</dt><dd>\u2014 (top level)</dd>"))))

(deftest workflow-detail-page-shows-metadata-test
  (let [id (test-util/insert! "test.ns/with-meta" :params (pr-str [1 2]) :metadata (pr-str {:actor-id 42}))
        response ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id)))]
    (is (re-find #"<h2>Metadata</h2>" (:body response)))
    (is (str/includes? (:body response) "<dt><code>:actor-id</code></dt><dd><pre>42</pre></dd>"))))

(deftest workflow-detail-page-hides-metadata-section-when-absent-test
  (let [id (test-util/insert! "test.ns/no-meta" :params (pr-str [1 2]))
        response ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id)))]
    (is (not (re-find #"<h2>Metadata</h2>" (:body response))))))

(deftest workflow-detail-page-shows-error-test
  (let [id (test-util/insert! "test.ns/failed")]
    (test-util/finish! id "ERROR" (pr-str {:message "boom" :class "clojure.lang.ExceptionInfo" :data nil}))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id)))]
      (is (= 200 (:status response)))
      (is (re-find #"boom" (:body response))))))

(deftest workflow-detail-page-shows-full-timeline-test
  ;; A restarted workflow keeps the states of its earlier invocations -
  ;; the timeline must show every one of them, in order, not just the latest.
  (let [id (test-util/insert! "test.ns/timeline")]
    (test-util/finish! id "ERROR" (pr-str {:message "first failure" :class "clojure.lang.ExceptionInfo" :data nil}))
    (test-util/finish! (test-util/restart! id) "DONE" (pr-str :recovered))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id)))
          events (test-util/timeline id)]
      (is (= 4 (count events)))
      (is (re-find #"first failure" (:body response)))
      (is (re-find #"recovered" (:body response)))
      (is (= 4 (count (re-seq #"class=\"tree-event state-(?:STARTED|DONE|ERROR)\"" (:body response))))
          "one timeline entry per event"))))

(deftest workflow-detail-page-interleaves-sub-workflow-timeline-test
  ;; Sub-workflow events (recursively, at any depth) must appear in the
  ;; same single timeline as the workflow's own events - not in a separate
  ;; section.
  (let [parent-id (test-util/insert! "test.ns/parent2")
        child-id (test-util/insert! "test.ns/child2" :parent parent-id)
        grandchild-id (test-util/insert! "test.ns/grandchild2" :parent child-id)]
    (test-util/finish! grandchild-id "DONE" (pr-str :ok))
    (test-util/finish! child-id "DONE" (pr-str :ok))
    (test-util/finish! parent-id "DONE" (pr-str :ok))
    (let [response ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" parent-id)))
          body (:body response)
          timeline (engine/full-timeline parent-id)]
      (is (= 6 (count timeline))
          "parent's own 2 events + child's 2 + grandchild's 2")
      (is (re-find (re-pattern (str "/workflows/" child-id)) body))
      (is (re-find (re-pattern (str "/workflows/" grandchild-id)) body)
          "the grandchild's events are pulled in too, not just direct children")
      (is (re-find #"test\.ns/child2" body))
      (is (re-find #"test\.ns/grandchild2" body))
      (is (= 6 (count (re-seq #"class=\"tree-event state-(?:STARTED|DONE|ERROR)\"" body)))
          "a single merged timeline, not a separate sub-workflows section"))))

(deftest workflow-detail-page-shows-timeline-tree-test
  ;; Below the table, the same events are shown as a foldable tree - one
  ;; <details> per workflow, its sub-workflows nested inside it - with no
  ;; script needed to fold it.
  (let [parent-id (test-util/insert! "test.ns/tree-parent")
        child-id (test-util/insert! "test.ns/tree-child" :parent parent-id)
        grandchild-id (test-util/insert! "test.ns/tree-grandchild" :parent child-id)]
    (test-util/finish! grandchild-id "DONE" (pr-str :ok))
    (test-util/finish! child-id "DONE" (pr-str :ok))
    (test-util/finish! parent-id "DONE" (pr-str :ok))
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" parent-id))))
          tree (subs body (str/index-of body "<ul class=\"tree\">"))]
      (is (= 3 (count (re-seq #"<details[ >]" tree))) "one node per workflow")
      (is (= 1 (count (re-seq #"<details open>" tree))) "finished sub-workflows start folded")
      (is (= 6 (count (re-seq #"class=\"tree-event state-" tree))) "one leaf per event")
      (is (re-find (re-pattern (str "(?s)tree-parent.*<details>.*tree-child.*<details>.*tree-grandchild")) tree)
          "sub-workflows are nested inside their parent's node")
      (is (re-find (re-pattern (str "<summary[^>]*><span class=\"tree-verb\">Invoked</span><code>test\\.ns/tree-child</code><a "
                                    "href=\"/workflows/" child-id "\"[^>]*>\u29C9 <code>" child-id "</code></a>"))
                   tree)
          "the name is plain text, so clicking it folds - a link next to it opens the workflow")
      (is (re-find #"<summary[^>]*><span class=\"tree-verb\">Invoked</span><code>test\.ns/tree-parent</code><span" tree)
          "no link on the top-level node - that's the page we're on"))))

(defn- tree-of [id]
  (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
    (subs body (str/index-of body "<ul class=\"tree\">"))))

(deftest timeline-tree-labels-later-starts-as-restarts-test
  ;; Started -> Error -> Started -> Done: the first start is the call, the
  ;; second one a restart.
  (let [id (test-util/insert! "test.ns/tree-restarted")]
    (test-util/finish! id "ERROR" (pr-str {:message "x" :class "java.lang.Exception" :data nil}))
    (test-util/finish! (test-util/restart! id) "DONE" (pr-str :ok))
    (let [tree (tree-of id)
          verbs (map second (re-seq #"<span class=\"tree-verb\">([^<]+)</span>" tree))]
      (is (= ["Invoked" "Called" "Failed" "Restarted" "Completed"] verbs)))))

(deftest timeline-tree-shows-run-state-and-reuse-count-test
  ;; Started -> Done -> Reused -> Reused: the node shows Done (the run's
  ;; outcome), with a badge counting the reuses, and each reuse links to the
  ;; invocation whose result it got and to the caller that got it.
  (let [id (test-util/insert! "test.ns/tree-reused")
        caller1 (test-util/insert! "test.ns/tree-caller1")
        caller2 (test-util/insert! "test.ns/tree-caller2")]
    (test-util/finish! id "DONE" (pr-str :ok))
    (test-util/record-reuse! id caller1)
    (test-util/record-reuse! id caller2)
    (let [tree (tree-of id)]
      (is (str/includes? tree "<summary class=\"tree-node state-DONE\">"))
      (is (re-find #"title=\"result reused 2 times\">\u21BA 2<" tree))
      (is (= 2 (count (re-seq (re-pattern (str "from <a href=\"/workflows/" id "\"")) tree))))
      (is (str/includes? tree (str "for <a href=\"/workflows/" caller1 "\"")))
      (is (str/includes? tree (str "for <a href=\"/workflows/" caller2 "\""))))))

(deftest timeline-tree-top-level-reuse-names-no-producer-test
  ;; Reused by a top-level call: there's no caller to name, and the
  ;; producer is the workflow itself - nothing to add next to the event.
  (let [id (test-util/insert! "test.ns/tree-reused-top")]
    (test-util/finish! id "DONE" (pr-str :ok))
    (test-util/record-reuse! id nil)
    (let [tree (tree-of id)]
      (is (str/includes? tree "tree-event state-REUSED"))
      (is (not (str/includes? tree "tree-reuse\"")))
      (is (not (str/includes? tree "from <a"))))))

(deftest timeline-tree-reuse-within-tree-is-a-leaf-test
  ;; A reuse within the tree is a single REUSED row - a flat leaf, not a
  ;; foldable node holding one Reused event.
  (let [child (test-util/insert! "test.ns/tree-produced")
        parent (test-util/insert! "test.ns/tree-reusing-parent")]
    (test-util/finish! child "DONE" (pr-str :ok))
    (test-util/record-reuse! child parent)
    (let [tree (tree-of parent)]
      (is (re-find (re-pattern (str "<li class=\"tree-leaf state-REUSED\"><code class=\"tree-offset\"[^>]*>[^<]*</code>"
                                    "<span class=\"tree-verb\">Invoked</span><code>test\\.ns/tree-produced</code>"
                                    "<a href=\"/workflows/" child "\""))
                   tree))
      (is (= 1 (count (re-seq #"<details" tree))) "only the reusing parent is foldable")
      (is (not (str/includes? tree "tree-event state-REUSED")) "no Reused event nested in it")
      (is (not (str/includes? tree "from <a")) "the producer is the linked invocation - no need to repeat it")
      (is (not (str/includes? tree "\u21BA")) "a reuse leaf gets no reuse badge"))))

(deftest timeline-tree-reuse-leaf-names-a-since-restarted-producer-test
  (let [child (test-util/insert! "test.ns/tree-produced-then-restarted")
        parent (test-util/insert! "test.ns/tree-reusing-parent2")]
    (test-util/finish! child "DONE" (pr-str :ok))
    (test-util/record-reuse! child parent)
    (let [current (test-util/restart! child)
          tree (tree-of parent)]
      (is (str/includes? tree (str "<a href=\"/workflows/" current "\"")))
      (is (str/includes? tree (str "from <a href=\"/workflows/" child "\""))))))

(deftest timeline-tree-opens-failed-and-running-subtrees-test
  ;; parent (running) > failing (error) > ok (done), and a finished sibling:
  ;; only the nodes on the way to a problem start open.
  (let [parent (test-util/insert! "test.ns/tree-p")
        failing (test-util/insert! "test.ns/tree-failing" :parent parent)
        ok (test-util/insert! "test.ns/tree-ok" :parent failing)
        sibling (test-util/insert! "test.ns/tree-sibling" :parent parent)]
    (test-util/finish! ok "DONE" (pr-str :ok))
    (test-util/finish! sibling "DONE" (pr-str :ok))
    (test-util/finish! failing "ERROR" (pr-str {:message "kaboom" :class "clojure.lang.ExceptionInfo" :data nil}))
    (let [tree (tree-of parent)]
      (is (re-find #"<details open><summary class=\"tree-node state-STARTED\"><span class=\"tree-verb\">Invoked</span><code>test\.ns/tree-p<" tree))
      (is (re-find #"<details open><summary class=\"tree-node state-ERROR\"><span class=\"tree-verb\">Invoked</span><code>test\.ns/tree-failing<" tree))
      (is (re-find #"<details><summary class=\"tree-node state-DONE\"><span class=\"tree-verb\">Invoked</span><code>test\.ns/tree-ok<" tree))
      (is (re-find #"<details><summary class=\"tree-node state-DONE\"><span class=\"tree-verb\">Invoked</span><code>test\.ns/tree-sibling<" tree))
      (is (re-find #"title=\"clojure\.lang\.ExceptionInfo: kaboom\"" tree)
          "the error chip's tooltip carries the exception")
      (is (str/includes? tree "title=\"running for ") "a running node shows how long it has been running"))))

(deftest timeline-tree-shows-offsets-and-durations-test
  (let [id (test-util/insert! "test.ns/tree-timed")]
    (test-util/finish! id "DONE" (pr-str :ok))
    (let [tree (tree-of id)]
      (is (re-find #"<code class=\"tree-offset\" title=\"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3} UTC\">\+00:00\.000</code><span class=\"tree-verb\">Called</span>" tree)
          "the first event is at +00:00.000, its absolute time in the tooltip")
      (is (re-find #"\+\d\d:\d\d\.\d{3}</code><span class=\"tree-verb\">Completed</span>" tree)
          "each event row says what happened")
      (is (re-find #"<span class=\"tree-time\">\d+ms</span>" tree)))))

(deftest page-style-is-not-html-escaped-test
  ;; Browsers don't decode entities inside <style>, so escaped quotes or >
  ;; would silently drop rules (e.g. the timeline tree's fold chevrons).
  (let [id (test-util/insert! "test.ns/styled")
        body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))
        style (second (re-find #"(?s)<style>(.*?)</style>" body))]
    (is (str/includes? style "details[open]>summary::before"))
    (is (not (re-find #"&(?:apos|gt|lt|quot|amp);" style)))))

(deftest workflow-detail-page-404-for-unknown-id-test
  (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/workflows/does-not-exist"))]
    (is (= 404 (:status response)))))

(deftest workflow-detail-page-shows-restart-button-when-restartable-test
  (engine/register! #'http-flaky-fn http-flaky-fn)
  (is (thrown? clojure.lang.ExceptionInfo
        (engine/run-invocation #'http-flaky-fn http-flaky-fn [])))
  (let [id (:invocation_id (first (test-util/find-by-wf-def "tenon.workflow.http-test/http-flaky-fn")))
        body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
    (is (str/includes? body (str "<form action=\"/workflows/" id "/restart\" class=\"restart\" method=\"post\">")))
    (is (str/includes? body "<input name=\"via\" type=\"hidden\" value=\"form\">"))
    (is (str/includes? body "<details class=\"panel-toggle\"><summary class=\"btn\">\u21BB Restart</summary>"))
    (is (not (str/includes? body "<script")) "no script needed")))

(deftest restart-form-redirects-back-to-the-detail-page-test
  (engine/register! #'http-flaky-fn http-flaky-fn)
  (is (thrown? clojure.lang.ExceptionInfo
        (engine/run-invocation #'http-flaky-fn http-flaky-fn [])))
  (let [id (:invocation_id (first (test-util/find-by-wf-def "tenon.workflow.http-test/http-flaky-fn")))]
    (engine/register! #'http-flaky-fn (fn [] :ok))
    (let [response ((http/app engine/*workflow-engine*)
                    (mock/request :post (str "/workflows/" id "/restart") {"via" "form"}))
          current (:invocation_id (engine/get-invocation id))]
      (is (= 303 (:status response)))
      (is (not= id current) "restarted under a new invocation id")
      (is (= (str "/workflows/" current) (get-in response [:headers "Location"]))))))

(deftest restart-form-failure-is-shown-on-the-detail-page-test
  (let [id (test-util/insert! "test.ns/still-running-restart")
        response ((http/app engine/*workflow-engine*)
                  (mock/request :post (str "/workflows/" id "/restart") {"via" "form"}))
        location (get-in response [:headers "Location"])]
    (is (= 303 (:status response)))
    (is (str/starts-with? location (str "/workflows/" id "?restart_error=")))
    (let [[path qs] (str/split location #"\?" 2)
          body (:body ((http/app engine/*workflow-engine*) (assoc (mock/request :get path) :query-string qs)))]
      (is (str/includes? body "<p class=\"wf-summary-note wf-summary-error\">Restart failed: Cannot restart: workflow is not DONE or ERROR</p>")))))

(deftest workflow-detail-page-hides-restart-button-when-pending-test
  (let [id (test-util/insert! "test.ns/still-running")
        body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
    (is (not (str/includes? body "/restart\"")))))

(deftest workflow-detail-page-hides-restart-button-when-wf-def-unregistered-test
  (let [id (test-util/insert! "test.ns/never-registered-anywhere")]
    (test-util/finish! id "DONE" (pr-str :ok))
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
      (is (not (str/includes? body "/restart\""))))))

(deftest workflow-detail-page-shows-ancestry-breadcrumb-test
  (let [root (test-util/insert! "test.ns/crumb-root")
        mid (test-util/insert! "test.ns/crumb-mid" :parent root)
        leaf (test-util/insert! "test.ns/crumb-leaf" :parent mid)
        page #(:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" %))))]
    (is (re-find (re-pattern (str "(?s)<nav class=\"breadcrumb\">"
                                  "<a href=\"/workflows/" root "\">.*crumb-root.*</a>.*\u203A.*"
                                  "<a href=\"/workflows/" mid "\">.*crumb-mid.*</a>.*\u203A.*"
                                  "<span class=\"breadcrumb-current\">.*crumb-leaf.*</nav>"))
                 (page leaf)))
    (is (not (str/includes? (page root) "<nav class=\"breadcrumb\"")) "a top-level workflow has no ancestors to show")))

(deftest workflow-detail-page-summary-duration-is-the-last-attempt-test
  ;; Started at 0, Error at 10s, restarted at 60s, Done at 61.5s: the card
  ;; shows the last attempt's 1.5s, the tree's node the whole run's 61.5s.
  (let [id (test-util/insert! "test.ns/attempts")]
    (test-util/finish! id "ERROR" (pr-str {:message "x" :class "java.lang.Exception" :data nil}))
    (test-util/finish! (test-util/restart! id) "DONE" (pr-str :ok))
    (let [ids (map :id (jdbc/execute! (test-util/ds) ["SELECT id FROM workflow_history ORDER BY id"]
                                      {:builder-fn rs/as-unqualified-lower-maps}))]
      (is (= 4 (count ids)))
      (doseq [[hid at] (map vector ids [0 10000 60000 61500])]
        (jdbc/execute! (test-util/ds) ["UPDATE workflow_history SET state_changed_at = ? WHERE id = ?" at hid])))
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
      (is (str/includes? body "<span class=\"wf-summary-duration\">1.50s</span>"))
      (is (str/includes? body "<span class=\"tree-time\">1m 01s</span>")))))

(deftest workflow-detail-page-tab-title-test
  (let [id (test-util/insert! "test.ns/tab")]
    (test-util/finish! id "ERROR" (pr-str {:message "x" :class "java.lang.Exception" :data nil}))
    (is (str/includes? (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))
                       (str "<title>\u2717 test.ns/tab \u00B7 " id "</title>")))))

(deftest workflow-detail-page-summary-times-are-utc-and-relative-test
  (let [id (test-util/insert! "test.ns/times")
        body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
    (is (re-find #"<dt>Started</dt><dd><span><code>[^<]+ UTC</code> <span class=\"panel-muted\">\(just now\)</span>" body))))

(deftest workflow-detail-page-folds-large-values-test
  (let [id (test-util/insert! "test.ns/big" :params (pr-str [(vec (range 2000))]))
        body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))]
    (is (str/includes? body "<details class=\"fold\">"))
    (is (re-find #"Show all \d+ lines" body))))

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
    (let [id (test-util/insert! "test.ns/prefixed")
          body (:body (app (mock/request :get "/tenon/workflows/pending")))]
      (is (some #(= id (:invocation_id %)) (json/parse-string body true))))))

(deftest wrap-handler-generates-prefixed-links-test
  (let [original (constantly {:status 200 :body "unreachable"})
        app (wrapped-app "/tenon" original)
        id (test-util/insert! "test.ns/prefixed-link")
        dashboard-body (:body (app (mock/request :get "/tenon")))
        detail-body (:body (app (mock/request :get (str "/tenon/workflows/" id))))]
    (is (re-find (re-pattern (str "href=\"/tenon/workflows/" id "\"")) dashboard-body))
    (is (re-find #"action=\"/tenon/\"" dashboard-body))
    (is (re-find (re-pattern (str "href=\"/tenon/workflows/" id "\"")) detail-body)
        "the workflow's own timeline row also links back through the prefix")))

(deftest workflow-detail-page-links-back-to-listing-test
  (let [id (test-util/insert! "test.ns/back")
        body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" id))))
        prefixed (:body ((wrapped-app "/tenon" (constantly nil)) (mock/request :get (str "/tenon/workflows/" id))))]
    (is (str/includes? body "<h1>test.ns/back</h1><a class=\"back\" href=\"/\">\u2190 Workflows</a></header>"))
    (is (str/includes? prefixed "<a class=\"back\" href=\"/tenon/\">"))
    (is (not (str/includes? (:body ((http/app engine/*workflow-engine*) (mock/request :get "/"))) "class=\"back\""))
        "the listing itself has no back link")))

(deftest wrap-handler-with-empty-prefix-behaves-like-root-mount-test
  (let [original (constantly {:status 200 :body "unreachable"})
        app (wrapped-app "" original)]
    (is (re-find #"<h1>Workflows</h1>" (:body (app (mock/request :get "/")))))))

(defn- async-call
  "Calls app's async (3-arity) handler and returns what it responded with,
   or throws what it raised."
  [app request]
  (let [result (promise)]
    (app request #(deliver result {:response %}) #(deliver result {:error %}))
    (let [{:keys [response error]} (deref result 1000 {:error (ex-info "timeout" {})})]
      (if error (throw error) response))))

(deftest wrap-handler-async-delegates-non-prefixed-uris-test
  (let [original (fn [_request respond _raise] (respond {:status 200 :body "from the original app"}))
        app (wrapped-app "/tenon" original)]
    (is (= "from the original app" (:body (async-call app (mock/request :get "/other")))))))

(deftest wrap-handler-async-routes-prefixed-uris-to-tenon-test
  (let [original (fn [_request _respond raise] (raise (ex-info "unreachable" {})))
        app (wrapped-app "/tenon" original)]
    (is (re-find #"<h1>Workflows</h1>" (:body (async-call app (mock/request :get "/tenon")))))))

(deftest app-async-not-found-test
  (let [response (async-call (http/app engine/*workflow-engine*) (mock/request :get "/no/such/route"))]
    (is (= 404 (:status response)))
    (is (= {:error "not found"} (json/parse-string (:body response) true)))))

(deftest app-not-found-test
  (let [response ((http/app engine/*workflow-engine*) (mock/request :get "/no/such/route"))]
    (is (= 404 (:status response)))
    (is (= {:error "not found"} (json/parse-string (:body response) true)))))

(deftest workflow-detail-page-shows-reused-sub-workflow-test
  ;; A child whose result another parent reused shows under that parent's
  ;; timeline too, with its REUSED row labelled.
  (let [parent1 (test-util/insert! "test.ns/reuse-parent1")
        child (test-util/insert! "test.ns/reuse-child" :parent parent1)
        parent2 (test-util/insert! "test.ns/reuse-parent2")]
    (test-util/finish! child "DONE" (pr-str :ok))
    (test-util/record-reuse! child parent2)
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get (str "/workflows/" parent2))))]
      (is (re-find #"test\.ns/reuse-child" body))
      (is (re-find #"class=\"tree-leaf state-REUSED\"" body))
      (is (re-find #">Reused<" body)))))

(deftest dashboard-lists-reused-top-level-call-again-test
  ;; The second (add-numbers 3 4) reuses the first one's stored result -
  ;; it's a call of its own nonetheless, so the dashboard lists it too,
  ;; linking to the workflow whose result it got.
  (let [id (test-util/insert! "test.ns/reused-top" :params (pr-str [3 4]))]
    (test-util/finish! id "DONE" (pr-str 7))
    (test-util/record-reuse! id nil)
    (let [body (:body ((http/app engine/*workflow-engine*) (mock/request :get "/")))]
      (is (= 2 (count (re-seq #"class=\"state-DONE\"" body))))
      (is (= 2 (count (re-seq #"test\.ns/reused-top</code>" body))))
      (is (re-find #"Reused" body) "the reusing call is marked as such"))))

(deftest dashboard-lists-nested-reuse-only-with-all-checkbox-test
  (let [parent-id (test-util/insert! "test.ns/reuse-parent")
        child-id (test-util/insert! "test.ns/reuse-child" :parent parent-id)]
    (test-util/finish! child-id "DONE" (pr-str 1))
    (test-util/record-reuse! child-id parent-id)
    (let [top ((http/app engine/*workflow-engine*) (mock/request :get "/"))
          all ((http/app engine/*workflow-engine*) (mock/request :get "/" {:all "1"}))]
      (is (not (re-find #"test\.ns/reuse-child</code>" (:body top))))
      (is (= 2 (count (re-seq #"test\.ns/reuse-child</code>" (:body all))))))))
