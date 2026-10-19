(ns tenon.workflow.http-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.string :as str]
            [ring.mock.request :as mock]
            [cheshire.core :as json]
            [matcher-combinators.test :refer [match? thrown-match?]]
            [matcher-combinators.matchers :as m]
            [kerodon.core :as k :refer [within]]
            [kerodon.test :refer [has status? some-text? regex?
                                  attr? value? missing? heading? link?]]
            [net.cgrand.enlive-html :as enlive]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [tenon.workflow.http :as http]
            [tenon.workflow :as engine]
            [tenon.workflow.kerodon-util :as ku :refer [count? present? texts? attrs? dd-of panel with-text]]
            [tenon.workflow.test-util :as test-util :refer [temp-db-fixture]]))

(use-fixtures :each temp-db-fixture ku/html5-fixture)

(defn- call [req] ((http/app engine/*workflow-engine*) req))

(defn- request
  "The app's response to a mock request (see ring.mock.request/request)."
  [& args]
  (call (apply mock/request args)))

(defn- visit
  "A kerodon session of the app, having visited uri."
  ([uri] (visit (http/app engine/*workflow-engine*) uri))
  ([app uri] (-> (k/session app) (k/visit uri))))

(defn- href [id] (str "/workflows/" id))

(defn- row-of
  "Selector: the listing's row of workflow id - the one whose first
   cell links to it."
  [id]
  [:table.workflows :tbody
   [:tr (enlive/has [[:td enlive/first-child] [:a (enlive/attr= :href (href id))]])]])

(def ^:private rows [:table.workflows :tbody :tr])

(defn- selected-option
  "Selector: the selected <option> of the <select> named select-name."
  [select-name]
  [[:select (enlive/attr= :name select-name)] [:option (enlive/attr? :selected)]])

(defn- tree-node
  "Selector step: the timeline tree's node summary of workflow wf-name."
  [wf-name]
  [:summary (enlive/has [[:b (with-text wf-name)]])])

(def ^:private http-flaky-fn (fn [] (throw (ex-info "fail" {}))))

(deftest pending-endpoint-test
  (let [id (test-util/insert! "test.ns/crashed" :params (pr-str [1 2]))
        response (request :get "/workflows/pending")
        body (json/parse-string (:body response) true)]
    (is (match? {:status 200} response))
    (is (match? [{:invocation_id id :params [1 2]}] body))))

(deftest pending-endpoint-handles-invalid-params-edn-test
  ;; A single row with unreadable :params must not fail the whole
  ;; response - it should fall back to :params_raw while every other
  ;; (valid) row still comes through normally.
  (let [good-id (test-util/insert! "test.ns/good" :params (pr-str [1 2]))
        bad-id (test-util/insert! "test.ns/bad" :params "(1 2")
        response (request :get "/workflows/pending")
        body (json/parse-string (:body response) true)]
    (is (match? {:status 200} response))
    (is (match? (m/in-any-order [{:invocation_id good-id :params [1 2]}
                                 {:invocation_id bad-id :params_raw "(1 2"}])
                body))))

(deftest pending-endpoint-returns-500-on-unexpected-error-test
  (with-redefs [engine/list-pending (fn [] (throw (ex-info "kaboom" {})))]
    (let [response (request :get "/workflows/pending")]
      (is (match? {:status 500} response)))))

(deftest dashboard-lists-top-level-workflows-test
  (let [id (test-util/insert! "test.ns/top" :params (pr-str [1]))]
    (test-util/finish! id "DONE" (pr-str :ok))
    (let [state (-> (visit "/")
                    (has (status? 200))
                    (has (heading? "Workflows"))
                    (has (count? rows 1))
                    (has (texts? (conj (row-of id) :td :code :span.wf-ns) ["test.ns/"]))
                    (has (texts? (conj (row-of id) :td :code :b) ["top"]))
                    (has (attrs? (conj (row-of id) :a.wf-link) :href [(href id)])))]
      (is (str/starts-with? (get-in state [:response :headers "Content-Type"]) "text/html")))))

(deftest dashboard-excludes-nested-workflows-test
  (let [parent-id (test-util/insert! "test.ns/parent")
        child-id (test-util/insert! "test.ns/child" :parent parent-id)]
    (-> (visit "/")
        (has (present? (row-of parent-id)))
        (has (missing? (row-of child-id))))))

(deftest dashboard-shows-nested-workflows-with-all-checkbox-test
  (let [parent-id (test-util/insert! "test.ns/parent4")
        child-id (test-util/insert! "test.ns/child4" :parent parent-id)]
    (-> (visit "/?all=1")
        (has (present? (row-of parent-id)))
        (has (present? (row-of child-id)) "checked all=1 must include sub-workflows too")
        (has (present? [[:input (enlive/attr= :name "all") (enlive/attr? :checked)]])
             "the checkbox itself must reflect the checked state"))))

(deftest dashboard-defaults-to-all-states-test
  (let [done-id (test-util/insert! "test.ns/done-wf")
        pending-id (test-util/insert! "test.ns/pending-wf")]
    (test-util/finish! done-id "DONE" (pr-str :ok))
    (-> (visit "/")
        (has (present? (row-of done-id)))
        (has (present? (row-of pending-id)))
        (has (texts? (selected-option "state") ["All"]))
        (has (attr? (selected-option "state") :value "")))))

(deftest dashboard-filters-by-state-query-param-test
  (let [done-id (test-util/insert! "test.ns/done-wf2")
        pending-id (test-util/insert! "test.ns/pending-wf2")]
    (test-util/finish! done-id "DONE" (pr-str :ok))
    (-> (visit "/?state=DONE")
        (has (present? (row-of done-id)))
        (has (missing? (row-of pending-id)) "the STARTED row is filtered out of the table")
        (has (attr? (selected-option "state") :value "DONE")))))

(deftest dashboard-filters-by-wf-def-query-param-test
  (let [a-id (test-util/insert! "test.ns/name-a")
        b-id (test-util/insert! "test.ns/name-b")]
    (-> (visit "/?wf_def=test.ns/name-a")
        (has (present? (row-of a-id)))
        (has (missing? (row-of b-id)))
        (has (texts? (selected-option "wf_def") ["test.ns/name-a"]))
        (has (present? [[:select (enlive/attr= :name "wf_def")]
                        [:option (enlive/attr= :value "test.ns/name-b") (enlive/but (enlive/attr? :selected))]])
             "the other name still appears as an unselected dropdown option"))))

(defn- insert-n-workflows! [n prefix]
  (dotimes [i n]
    (test-util/insert! (str "test.ns/" prefix i) :params (pr-str [i]))))

(deftest dashboard-defaults-to-page-size-100-with-no-next-page-link-for-small-result-sets-test
  (insert-n-workflows! 5 "small")
  (-> (visit "/")
      (has (attr? (selected-option "page_size") :value "100"))
      (has (count? rows 5))
      (has (missing? [[:a (enlive/attr-contains :href "before_id")]])
           "fewer rows than the page size - no next page")
      (has (texts? [:div.pager :span.btn.disabled] ["Next page \u2192"]))))

(deftest dashboard-paginates-with-seek-based-next-page-link-test
  (insert-n-workflows! 105 "page")
  (-> (visit "/?page_size=100")
      (has (count? [:table.workflows :tbody :tr.state-STARTED] 100) "capped at the requested page size")
      (has (attr? (selected-option "page_size") :value "100"))
      (has (present? [:div.pager [:a (enlive/attr-contains :href "before_id")]])
           "a next-page link with a before_id cursor is present")
      (k/follow "Next page \u2192")
      (has (status? 200))
      (has (count? [:table.workflows :tbody :tr.state-STARTED] 5) "the remaining rows, none repeated or skipped")
      (has (missing? [[:a (enlive/attr-contains :href "before_id")]])
           "no further next-page link once the last page is reached")))

(deftest workflow-detail-page-shows-params-and-result-test
  (let [id (test-util/insert! "test.ns/detail" :params (pr-str [1 2]))]
    (test-util/finish! id "DONE" (pr-str 42))
    (-> (visit (href id))
        (has (status? 200))
        (within (panel "Params")
          (has (texts? [:span.panel-muted] ["2 arguments"]))
          (has (texts? [:dt] ["%1" "%2"]))
          (has (texts? (conj (dd-of "%1") :pre) ["1"]))
          (has (texts? (conj (dd-of "%2") :pre) ["2"])))
        (within (panel "Result")
          (has (texts? [:span.panel-muted] ["done"]))
          (has (texts? [:pre] ["42"]))))))

(deftest workflow-detail-page-shows-no-arguments-and-pending-result-test
  (let [id (test-util/insert! "test.ns/no-args" :params (pr-str []))]
    (-> (visit (href id))
        (has (texts? (conj (panel "Params") :span.panel-muted) ["no arguments"]))
        (has (texts? (conj (panel "Result") :div.panel-body) ["Pending \u2014 no result yet."])))))

(deftest workflow-detail-page-shows-unreadable-params-raw-test
  (let [id (test-util/insert! "test.ns/unreadable" :params "[#object[Foo 0x1 \"x\"]]")]
    (-> (visit (href id))
        (within (panel "Params")
          (has (texts? [:span.panel-muted] ["unreadable EDN, shown raw:"]))
          (has (texts? [:pre] ["[#object[Foo 0x1 \"x\"]]"]))))))

(deftest workflow-detail-page-shows-error-panel-test
  (let [id (test-util/insert! "test.ns/error-panel")]
    (test-util/finish! id "ERROR" (pr-str (Throwable->map (ex-info "boom" {:k 1}))))
    (-> (visit (href id))
        (within (panel "Error")
          (has (texts? [:div.error-class :code] ["clojure.lang.ExceptionInfo"]))
          (has (texts? [:p.error-message] ["boom"]))
          (has (texts? [:pre] ["{:k 1}"]))))))

(deftest workflow-detail-page-id-line-is-a-real-link-test
  (let [id (test-util/insert! "test.ns/id-link")]
    (-> (visit (href id))
        (has (attrs? (conj (dd-of "Id") :a.wf-link) :href [(href id)]))
        (has (texts? (conj (dd-of "Id") :a.wf-link) [(str "\u29C9 " id)]))
        (within [:style]
          (has (some-text? "text-decoration:underline"))))))

(deftest workflow-detail-page-links-to-parent-workflow-test
  (let [parent-id (test-util/insert! "test.ns/parent-link")
        child-id (test-util/insert! "test.ns/child-link" :parent parent-id)]
    (-> (visit (href child-id))
        (has (attrs? (conj (dd-of "Parent") :a.wf-link) :href [(href parent-id)]))
        (within (dd-of "Parent")
          (k/follow [:a.wf-link]))
        (has (status? 200))
        (has (texts? [:header.page-head :h1] ["test.ns/parent-link"])))))

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
      (-> (visit (href id))
          (has (attr? [:section.wf-summary] :class "wf-summary state-DONE"))
          (has (texts? (dd-of "Attempts") ["2"]))
          (has (missing? (dd-of "Sub-workflows")))
          (has (texts? (dd-of "Reused") ["1 time"]))
          (has (present? (dd-of "Finished")))
          (has (texts? (dd-of "Parent") ["\u2014 (top level)"]))
          (has (texts? [:section.wf-summary :p.wf-summary-note]
                       [(str "Invocation " id " was restarted - showing the workflow's current invocation.")]))
          (has (texts? (dd-of "Id") [(str "\u29C9 " current)]))))))

(deftest workflow-detail-page-hides-parent-link-for-top-level-workflow-test
  (let [id (test-util/insert! "test.ns/top-level-no-parent")]
    (-> (visit (href id))
        (has (texts? (dd-of "Parent") ["\u2014 (top level)"]))
        (has (missing? (conj (dd-of "Parent") :a))))))

(deftest workflow-detail-page-shows-metadata-test
  (let [id (test-util/insert! "test.ns/with-meta" :params (pr-str [1 2]) :metadata (pr-str {:actor-id 42}))]
    (-> (visit (href id))
        (has (heading? "Metadata"))
        (within (panel "Metadata")
          (has (texts? [:dt] [":actor-id"]))
          (has (texts? (conj (dd-of ":actor-id") :pre) ["42"]))))))

(deftest workflow-detail-page-hides-metadata-section-when-absent-test
  (let [id (test-util/insert! "test.ns/no-meta" :params (pr-str [1 2]))]
    (-> (visit (href id))
        (has (missing? (panel "Metadata"))))))

(deftest workflow-detail-page-shows-error-test
  (let [id (test-util/insert! "test.ns/failed")]
    (test-util/finish! id "ERROR" (pr-str {:message "boom" :class "clojure.lang.ExceptionInfo" :data nil}))
    (-> (visit (href id))
        (has (status? 200))
        (has (attr? [:section.wf-summary] :class "wf-summary state-ERROR"))
        (has (texts? (conj (panel "Error") :p.error-message) ["boom"])))))

(deftest workflow-detail-page-shows-full-timeline-test
  ;; A restarted workflow keeps the states of its earlier invocations -
  ;; the timeline must show every one of them, in order, not just the latest.
  (let [id (test-util/insert! "test.ns/timeline")]
    (test-util/finish! id "ERROR" (pr-str {:message "first failure" :class "clojure.lang.ExceptionInfo" :data nil}))
    (test-util/finish! (test-util/restart! id) "DONE" (pr-str :recovered))
    (is (= 4 (count (test-util/timeline id))))
    (-> (visit (href id))
        (has (attrs? [:ul.tree :li.tree-event] :class
                     ["tree-event state-STARTED" "tree-event state-STARTED" "tree-event state-DONE"])
             "the other events, in order")
        (has (texts? [:ul.tree :li.tree-error :.tree-verb] ["Failed"]))
        (has (attr? [:ul.tree :li.tree-error :span.tree-state-chip] :title
                    "clojure.lang.ExceptionInfo: first failure"))
        (has (texts? (conj (panel "Result") :pre) [":recovered"])))))

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
    (is (= 6 (count (engine/full-timeline parent-id)))
        "parent's own 2 events + child's 2 + grandchild's 2")
    (-> (visit (href parent-id))
        (has (link? :href (href child-id)))
        (has (link? :href (href grandchild-id))
             "the grandchild's events are pulled in too, not just direct children")
        (has (texts? [:ul.tree :summary :b] ["parent2" "child2" "grandchild2"]))
        (has (count? [:ul.tree] 1) "a single merged timeline, not a separate sub-workflows section")
        (has (count? [:ul.tree :li.tree-event] 6)))))

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
    (-> (visit (href parent-id))
        (within [:ul.tree]
          (has (count? [:details] 3) "one node per workflow")
          (has (count? [[:details (enlive/attr? :open)]] 1) "finished sub-workflows start folded")
          (has (count? [:li.tree-event] 6) "one leaf per event")
          (has (texts? [:details :> :summary :b] ["tree-parent" "tree-child" "tree-grandchild"]))
          (has (texts? [:details :details :> :summary :b] ["tree-child" "tree-grandchild"])
               "sub-workflows are nested inside their parent's node")
          (has (texts? [:details :details :details :> :summary :b] ["tree-grandchild"]))
          (within [(tree-node "tree-child")]
            (has (texts? [:span.tree-verb] ["Invoked"]))
            (has (texts? [:code :span.wf-ns] ["test.ns/"]))
            (has (missing? [:a :b]) "the name is plain text, so clicking it folds")
            (has (attrs? [:a.wf-link] :href [(href child-id)]) "a link next to it opens the workflow")
            (has (texts? [:a.wf-link] [(str "\u29C9 " child-id)])))
          (within [(tree-node "tree-parent")]
            (has (texts? [:span.tree-verb] ["Invoked"]))
            (has (missing? [:a]) "no link on the top-level node - that's the page we're on"))))))

(deftest timeline-tree-labels-later-starts-as-restarts-test
  ;; Started -> Error -> Started -> Done: the first start is the call, the
  ;; second one a restart.
  (let [id (test-util/insert! "test.ns/tree-restarted")]
    (test-util/finish! id "ERROR" (pr-str {:message "x" :class "java.lang.Exception" :data nil}))
    (test-util/finish! (test-util/restart! id) "DONE" (pr-str :ok))
    (-> (visit (href id))
        (has (texts? [:ul.tree :span.tree-verb] ["Invoked" "Called" "Failed" "Restarted" "Completed"])))))

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
    (-> (visit (href id))
        (within [:ul.tree]
          (has (attrs? [:summary] :class ["state-DONE"]))
          (has (texts? [:summary :span.tree-count] ["\u21BA 2"]))
          (has (attr? [:summary :span.tree-count :span] :title "result reused 2 times"))
          (has (texts? [:span.tree-reuse]
                       [(str "from \u29C9 " id " for \u29C9 " caller1)
                        (str "from \u29C9 " id " for \u29C9 " caller2)]))
          (has (attrs? [:span.tree-reuse :a.wf-link] :href
                       [(href id) (href caller1) (href id) (href caller2)]))))))

(deftest timeline-tree-top-level-reuse-names-no-producer-test
  ;; Reused by a top-level call: there's no caller to name, and the
  ;; producer is the workflow itself - nothing to add next to the event.
  (let [id (test-util/insert! "test.ns/tree-reused-top")]
    (test-util/finish! id "DONE" (pr-str :ok))
    (test-util/record-reuse! id nil)
    (-> (visit (href id))
        (within [:ul.tree]
          (has (count? [:li.tree-event.state-REUSED] 1))
          (has (missing? [:span.tree-reuse]))))))

(deftest timeline-tree-reuse-within-tree-is-a-leaf-test
  ;; A reuse within the tree is a single REUSED row - a flat leaf, not a
  ;; foldable node holding one Reused event.
  (let [child (test-util/insert! "test.ns/tree-produced")
        parent (test-util/insert! "test.ns/tree-reusing-parent")]
    (test-util/finish! child "DONE" (pr-str :ok))
    (test-util/record-reuse! child parent)
    (-> (visit (href parent))
        (within [:ul.tree]
          (within [:li.tree-leaf.state-REUSED]
            (has (present? [:code.tree-offset]))
            (has (texts? [:span.tree-verb] ["Invoked"]))
            (has (texts? [:code :b] ["tree-produced"]))
            (has (attrs? [:a.wf-link] :href [(href child)]))
            (has (texts? [:span.tree-state-chip] ["Reused"]))
            (has (missing? [:span.tree-reuse])
                 "the producer is the linked invocation - no need to repeat it")
            (has (texts? [:span.tree-count] [""]) "a reuse leaf gets no reuse badge"))
          (has (count? [:details] 1) "only the reusing parent is foldable")
          (has (missing? [:li.tree-event.state-REUSED]) "no Reused event nested in it")))))

(deftest timeline-tree-reuse-leaf-names-a-since-restarted-producer-test
  (let [child (test-util/insert! "test.ns/tree-produced-then-restarted")
        parent (test-util/insert! "test.ns/tree-reusing-parent2")]
    (test-util/finish! child "DONE" (pr-str :ok))
    (test-util/record-reuse! child parent)
    (let [current (test-util/restart! child)]
      (-> (visit (href parent))
          (within [:ul.tree :li.tree-leaf.state-REUSED]
            (has (attrs? [:a.wf-link] :href [(href current) (href child)]))
            (has (texts? [:span.tree-reuse] [(str "from \u29C9 " child)])))))))

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
    (-> (visit (href parent))
        (within [:ul.tree]
          (has (texts? [:summary :b] ["tree-p" "tree-failing" "tree-ok" "tree-sibling"]))
          (has (attrs? [:summary] :class ["state-STARTED" "state-ERROR" "state-DONE" "state-ERROR" "state-DONE"]))
          (has (texts? [:li.tree-error :.error-message] ["kaboom"]))
          (has (texts? [[:details (enlive/attr? :open)] :> :summary :b] ["tree-p" "tree-failing"]))
          (has (texts? [[:details (enlive/but (enlive/attr? :open))] :> :summary :b] ["tree-ok" "tree-sibling"]))
          (has (attr? [(tree-node "tree-failing") :span.tree-state-chip] :title "clojure.lang.ExceptionInfo: kaboom")
               "the error chip's tooltip carries the exception")
          (has (present? [(tree-node "tree-p") :span.tree-time [:span (enlive/attr-starts :title "running for ")]])
               "a running node shows how long it has been running")))))

(deftest timeline-tree-shows-offsets-and-durations-test
  (let [id (test-util/insert! "test.ns/tree-timed")]
    (test-util/finish! id "DONE" (pr-str :ok))
    (-> (visit (href id))
        (within [:ul.tree]
          (has (texts? [:li.tree-event :span.tree-verb] ["Called" "Completed"]) "each event row says what happened")
          (has (texts? [[:li.tree-event enlive/first-of-type] :code.tree-offset] ["+00:00.000"])
               "the first event is at +00:00.000")
          (has (present? [[:li.tree-event enlive/first-of-type]
                          [:code.tree-offset (ku/with-attr-matching :title #"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3} UTC")]])
               "its absolute time is in the tooltip")
          (within [[:li.tree-event enlive/last-of-type] :code.tree-offset]
            (has (regex? "\\+\\d\\d:\\d\\d\\.\\d{3}")))
          (within [:summary :span.tree-time]
            (has (regex? "\\d+ms")))))))

(deftest page-style-is-not-html-escaped-test
  ;; Browsers don't decode entities inside <style>, so escaped quotes or >
  ;; would silently drop rules (e.g. the timeline tree's fold chevrons).
  (let [id (test-util/insert! "test.ns/styled")]
    (-> (visit (href id))
        (within [:style]
          (has (some-text? "details[open]>summary::before"))
          (has (regex? "(?s)(?!.*&(?:apos|gt|lt|quot|amp);).*") "no HTML entities")))))

(deftest workflow-detail-page-404-for-unknown-id-test
  (-> (visit "/workflows/does-not-exist")
      (has (status? 404))))

(defn- flaky-invocation!
  "Registers http-flaky-fn, runs it to an ERROR and returns its invocation id."
  []
  (engine/register! #'http-flaky-fn http-flaky-fn)
  (is (thrown? clojure.lang.ExceptionInfo
        (engine/run-invocation #'http-flaky-fn http-flaky-fn [])))
  (:invocation_id (first (test-util/find-by-wf-def "tenon.workflow.http-test/http-flaky-fn"))))

(deftest workflow-detail-page-shows-restart-button-when-restartable-test
  (let [id (flaky-invocation!)]
    (-> (visit (href id))
        (has (texts? [:details.panel-toggle :> :summary.btn] ["\u21BB Restart"]))
        (has (attr? [:details.panel-toggle :form.restart] :action (str (href id) "/restart")))
        (has (attr? [:form.restart] :method "post"))
        (has (texts? [:form.restart [:button (enlive/attr= :type "submit")]] ["Yes, restart"]))
        (has (missing? [:script]) "no script needed"))))

(deftest restart-form-redirects-back-to-the-detail-page-test
  (let [id (flaky-invocation!)
        _ (engine/register! #'http-flaky-fn (fn [] :ok))
        state (-> (visit (href id))
                  (k/press "Yes, restart")
                  (has (status? 303)))
        current (:invocation_id (engine/get-invocation id))]
    (is (not= id current) "restarted under a new invocation id")
    (is (= (href current) (get-in state [:response :headers "Location"])))
    (-> state
        k/follow-redirect
        (has (status? 200))
        (has (attr? [:section.wf-summary] :class "wf-summary state-DONE"))
        (has (texts? (dd-of "Id") [(str "\u29C9 " current)])))))

(deftest restart-form-failure-is-shown-on-the-detail-page-test
  (let [id (test-util/insert! "test.ns/still-running-restart")
        state (-> (k/session (http/app engine/*workflow-engine*))
                  (k/visit (str (href id) "/restart") :request-method :post)
                  (has (status? 303)))]
    (is (str/starts-with? (get-in state [:response :headers "Location"])
                          (str (href id) "?restart_error=")))
    (-> state
        k/follow-redirect
        (has (texts? [:p.wf-summary-note.wf-summary-error]
                     ["Restart failed: Cannot restart: workflow is not DONE or ERROR"])))))

(deftest workflow-detail-page-hides-restart-button-when-pending-test
  (engine/register! #'http-flaky-fn (fn [] :ok))
  (let [id (test-util/insert! "tenon.workflow.http-test/http-flaky-fn" :timeout-ms 60000)]
    (-> (visit (href id))
        (has (missing? [:span.chip.wf-expired]))
        (has (missing? [:span.chip.wf-unregistered]))
        (has (missing? [:form.restart]))
        (has (missing? [:details.panel-toggle])))))

(deftest workflow-detail-page-allows-restarting-expired-started-workflow-test
  (engine/register! #'http-flaky-fn (fn [] :ok))
  (let [id (test-util/insert! "tenon.workflow.http-test/http-flaky-fn" :timeout-ms 60000)]
    (test-util/expire! (test-util/ds) id)
    (let [state (-> (visit (href id))
                    (has (texts? [:div.wf-summary-head :span.chip] ["Started" "Expired"]))
                    (has (missing? [:span.chip.wf-unregistered]))
                    (has (texts? [:details.panel-toggle :> :summary.btn] ["\u21BB Restart"]))
                    (k/press "Yes, restart")
                    (has (status? 303)))
          current (:invocation_id (engine/get-invocation id))]
      (is (not= id current))
      (-> state
          k/follow-redirect
          (has (status? 200))
          (has (attr? [:section.wf-summary] :class "wf-summary state-DONE"))
          (has (missing? [:span.chip.wf-expired]))))))

(deftest workflow-detail-page-shows-warning-badge-when-wf-def-unregistered-test
  (let [id (test-util/insert! "test.ns/never-registered-anywhere")]
    (test-util/finish! id "DONE" (pr-str :ok))
    (-> (visit (href id))
        (has (missing? [:form.restart]))
        (has (texts? [:span.chip.wf-unregistered]
                     ["Unregistered var \u2014 cannot restart in this process"])))))

(deftest workflow-detail-page-shows-ancestry-breadcrumb-test
  (let [root (test-util/insert! "test.ns/crumb-root")
        mid (test-util/insert! "test.ns/crumb-mid" :parent root)
        leaf (test-util/insert! "test.ns/crumb-leaf" :parent mid)]
    (-> (visit (href leaf))
        (within [:nav.breadcrumb]
          (has (attrs? [:a] :href [(href root) (href mid)]))
          (has (texts? [:a] [(str "test.ns/crumb-root " root) (str "test.ns/crumb-mid " mid)]))
          (has (count? [:span.breadcrumb-sep] 2))
          (has (texts? [:span.breadcrumb-current] [(str "test.ns/crumb-leaf " leaf)])))
        (within [:nav.breadcrumb]
          (k/follow [:a (enlive/attr= :href (href mid))]))
        (has (texts? [:header.page-head :h1] ["test.ns/crumb-mid"])))
    (-> (visit (href root))
        (has (missing? [:nav.breadcrumb]) "a top-level workflow has no ancestors to show"))))

(deftest workflow-detail-page-summary-duration-is-the-last-attempt-test
  ;; Started at 0, Error at 10s, restarted at 60s, Done at 61.5s: the card
  ;; shows the last attempt's 1.5s, the tree's node the whole run's 61.5s.
  (let [id (test-util/insert! "test.ns/attempts")]
    (test-util/finish! id "ERROR" (pr-str {:message "x" :class "java.lang.Exception" :data nil}))
    (test-util/finish! (test-util/restart! id) "DONE" (pr-str :ok))
    (let [ids (map :id (jdbc/execute! (test-util/ds) ["SELECT id FROM event ORDER BY id"]
                                      {:builder-fn rs/as-unqualified-lower-maps}))]
      (is (= 4 (count ids)))
      (doseq [[hid at] (map vector ids [0 10000 60000 61500])]
        (jdbc/execute! (test-util/ds) ["UPDATE event SET at = ? WHERE id = ?" at hid])))
    (-> (visit (href id))
        (has (texts? [:span.wf-summary-duration] ["1.50s"]))
        (has (texts? [:ul.tree :summary :span.tree-time] ["1m 01s" ""])))))

(deftest workflow-detail-page-tab-title-test
  (let [id (test-util/insert! "test.ns/tab")]
    (test-util/finish! id "ERROR" (pr-str {:message "x" :class "java.lang.Exception" :data nil}))
    (-> (visit (href id))
        (has (texts? [:head :title] [(str "\u2717 test.ns/tab \u00B7 " id)])))))

(deftest workflow-detail-page-summary-times-are-utc-and-relative-test
  (let [id (test-util/insert! "test.ns/times")]
    (-> (visit (href id))
        (within (dd-of "Started")
          (has (regex? "\\d{4}-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d\\.\\d{3} UTC \\(just now\\)")))
        (has (texts? (conj (dd-of "Started") :span.panel-muted) ["(just now)"])))))

(deftest workflow-detail-page-folds-large-values-test
  (let [id (test-util/insert! "test.ns/big" :params (pr-str [(vec (range 2000))]))]
    (-> (visit (href id))
        (within (panel "Params")
          (has (count? [:details.fold] 1))
          (within [:details.fold :summary :span.fold-more]
            (has (regex? "Show all \\d+ lines \\(\\d+ chars\\)")))))))

(defn- wrapped-app [prefix original-handler]
  (http/wrap-handler engine/*workflow-engine* prefix original-handler))

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
    (-> (visit app "/tenon?state=STARTED")
        (has (status? 200))
        (has (texts? [:h1] ["Workflows"]))
        (has (attr? (selected-option "state") :value "STARTED")))
    (let [id (test-util/insert! "test.ns/prefixed")
          body (:body (app (mock/request :get "/tenon/workflows/pending")))]
      (is (match? [{:invocation_id id}] (json/parse-string body true))))))

(deftest wrap-handler-generates-prefixed-links-test
  (let [original (constantly {:status 200 :body "unreachable"})
        app (wrapped-app "/tenon" original)
        id (test-util/insert! "test.ns/prefixed-link")]
    (-> (visit app "/tenon")
        (has (link? :href (str "/tenon/workflows/" id)))
        (has (attrs? [:form] :action ["/tenon/" "/tenon/"]))
        (k/follow [:a.wf-link])
        (has (status? 200))
        (has (texts? [:header.page-head :h1] ["test.ns/prefixed-link"]))
        (has (attrs? (conj (dd-of "Id") :a) :href [(str "/tenon/workflows/" id)])
             "the workflow's own links go through the prefix too"))))

(deftest workflow-detail-page-links-back-to-listing-test
  (let [id (test-util/insert! "test.ns/back")]
    (-> (visit (href id))
        (within [:header.page-head]
          (has (texts? [:h1] ["test.ns/back"]))
          (has (attrs? [:a.btn.back] :href ["/"]))
          (has (texts? [:a.btn.back] ["\u2190 Workflows"])))
        (k/follow "\u2190 Workflows")
        (has (texts? [:h1] ["Workflows"]))
        (has (missing? [:a.back]) "the listing itself has no back link"))
    (-> (visit (wrapped-app "/tenon" (constantly nil)) (str "/tenon/workflows/" id))
        (has (attrs? [:header.page-head :a.btn.back] :href ["/tenon/"])))))

(deftest wrap-handler-with-empty-prefix-behaves-like-root-mount-test
  (let [original (constantly {:status 200 :body "unreachable"})
        app (wrapped-app "" original)]
    (-> (visit app "/")
        (has (texts? [:h1] ["Workflows"])))))

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
    (is (match? {:status 200 :body "from the original app"}
                (async-call app (mock/request :get "/other"))))))

(deftest wrap-handler-async-routes-prefixed-uris-to-tenon-test
  (let [original (fn [_request _respond raise] (raise (ex-info "unreachable" {})))
        app (wrapped-app "/tenon" original)]
    (-> (visit #(async-call app %) "/tenon")
        (has (status? 200))
        (has (texts? [:h1] ["Workflows"])))))

(deftest app-async-not-found-test
  (let [response (async-call (http/app engine/*workflow-engine*) (mock/request :get "/no/such/route"))]
    (is (match? {:status 404} response))
    (is (match? {:error "not found"} (json/parse-string (:body response) true)))))

(deftest app-not-found-test
  (let [response (request :get "/no/such/route")]
    (is (match? {:status 404} response))
    (is (match? {:error "not found"} (json/parse-string (:body response) true)))))

(deftest workflow-detail-page-shows-reused-sub-workflow-test
  ;; A child whose result another parent reused shows under that parent's
  ;; timeline too, with its REUSED row labelled.
  (let [parent1 (test-util/insert! "test.ns/reuse-parent1")
        child (test-util/insert! "test.ns/reuse-child" :parent parent1)
        parent2 (test-util/insert! "test.ns/reuse-parent2")]
    (test-util/finish! child "DONE" (pr-str :ok))
    (test-util/record-reuse! child parent2)
    (-> (visit (href parent2))
        (within [:ul.tree :li.tree-leaf.state-REUSED]
          (has (texts? [:b] ["reuse-child"]))
          (has (texts? [:span.tree-state-chip] ["Reused"]))))))

(deftest dashboard-lists-reused-top-level-call-again-test
  ;; The second (add-numbers 3 4) reuses the first one's stored result -
  ;; it's a call of its own nonetheless, so the dashboard lists it too,
  ;; linking to the workflow whose result it got.
  (let [id (test-util/insert! "test.ns/reused-top" :params (pr-str [3 4]))]
    (test-util/finish! id "DONE" (pr-str 7))
    (test-util/record-reuse! id nil)
    (-> (visit "/")
        (has (count? [:table.workflows :tbody :tr.state-DONE] 1))
        (has (count? [:table.workflows :tbody :tr.state-REUSED] 1) "the reusing call is marked as such")
        (has (texts? [:table.workflows :tbody :td :b] ["reused-top" "reused-top"]))
        (has (attrs? [:table.workflows :tbody :tr.state-REUSED :a.wf-link] :href [(href id)])
             "it links to the workflow whose result it got")
        (has (texts? [:table.workflows :tbody :tr.state-REUSED [:td enlive/last-child]] ["Reused"])
             "a reuse shows just Reused, not the producer's Done too"))))

(deftest dashboard-lists-nested-reuse-only-with-all-checkbox-test
  (let [parent-id (test-util/insert! "test.ns/reuse-parent")
        child-id (test-util/insert! "test.ns/reuse-child" :parent parent-id)]
    (test-util/finish! child-id "DONE" (pr-str 1))
    (test-util/record-reuse! child-id parent-id)
    (-> (visit "/")
        (has (texts? [:table.workflows :tbody :td :b] ["reuse-parent"])))
    (-> (visit "/?all=1")
        (has (count? [:table.workflows :tbody [:b (with-text "reuse-child")]] 2)))))

(deftest dashboard-row-columns-test
  ;; Id, the name with its namespace muted, how long it ran, and in
  ;; sub-workflow mode the parent.
  (let [parent (test-util/insert! "test.ns/cols-parent")
        child (test-util/insert! "test.ns/cols-child" :parent parent)]
    (test-util/finish! child "DONE" (pr-str 1))
    (-> (visit "/")
        (has (texts? [:table.workflows :thead :th] ["Timestamp" "Id" "Workflow" "Duration" "State"])
             "no parent column for top-level workflows only")
        (within (row-of parent)
          (has (attrs? [:a.wf-link] :href [(href parent)]))
          (has (texts? [:a.wf-link] [(str "\u29C9 " parent)]))
          (has (texts? [:code :span.wf-ns] ["test.ns/"]))
          (has (texts? [:code :b] ["cols-parent"]))
          (has (present? [:td.duration :a [:span (enlive/attr-starts :title "running for ")]]))
          (within [:td.duration]
            (has (regex? "running \\S+")))))
    (-> (visit "/?all=1")
        (has (texts? [:table.workflows :thead :th] ["Timestamp" "Id" "Workflow" "Parent" "Duration" "State"]))
        (within (row-of child)
          (has (texts? [:b] ["cols-child"]))
          (has (attrs? [:a.wf-link] :href [(href child) (href parent)]) "a sub-workflow links to its parent")
          (within [:td.duration]
            (has (regex? "\\d+ms") "a finished one shows how long it ran"))))))

(deftest dashboard-flags-expired-lease-test
  (let [id (test-util/insert! "test.ns/stuck" :timeout-ms 60000)
        ok (test-util/insert! "test.ns/fresh" :timeout-ms 60000)]
    (test-util/expire! (test-util/ds) id)
    (-> (visit "/")
        (has (count? [:span.wf-expired] 1))
        (has (texts? (conj (row-of id) :span.wf-expired) ["lease expired"]))
        (has (missing? (conj (row-of ok) :span.wf-expired))))))

(deftest dashboard-auto-refresh-test
  (-> (visit "/?state=STARTED")
      (has (missing? [[:meta (enlive/attr= :http-equiv "refresh")]]))
      (has (link? "\u21BB Auto-refresh off" "/?state=STARTED&refresh=1"))
      (k/follow "\u21BB Auto-refresh off")
      (has (attr? [[:meta (enlive/attr= :http-equiv "refresh")]] :content "10"))
      (has (value? [:input (enlive/attr= :name "refresh")] "1") "changing a filter keeps refreshing")
      (has (attr? [:form.filters [:input (enlive/attr= :name "refresh")]] :type "hidden"))
      (has (attr? (selected-option "state") :value "STARTED"))
      (has (link? "\u21BB Auto-refresh on" "/?state=STARTED") "the toggle turns it off again, keeping the filters")
      (k/follow "\u21BB Auto-refresh on")
      (has (missing? [[:meta (enlive/attr= :http-equiv "refresh")]]))))
