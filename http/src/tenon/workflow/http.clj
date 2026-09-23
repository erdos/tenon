(ns tenon.workflow.http
  (:require [ring.middleware.json :refer [wrap-json-response]]
            [ring.middleware.params :refer [wrap-params]]
            [hiccup2.core :as hiccup]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [tenon.workflow :as engine]
            [tenon.workflow.db :as db]
            [tenon.workflow.ui :as ui]))

(def ^:dynamic *uri-prefix*
  "The URI prefix of tenon routes."
  "")

(defn- uri
  "Builds a prefix-relative URI from path segments."
  [& segments]
  (str *uri-prefix* "/" (str/join "/" (map name segments))))

(defn- current-ds []
  (:tenon/db engine/*workflow-engine*))

(defn- workflow->response [wf]
  (try
    {:id (:id wf) :wf_def (:wf_def wf) :arguments (edn/read-string (:arguments wf))}
    (catch Throwable _
      {:id (:id wf) :wf_def (:wf_def wf) :arguments_raw (:arguments wf)})))

(defn- restart! [id]
  (try
    (engine/restart-invocation id)
    (let [latest (db/latest-event (current-ds) id)]
      {:status 200 :body {:id id :state (:state latest) :payload (:payload latest)}})
    (catch clojure.lang.ExceptionInfo e
      (if (= ::engine/precondition-failed (:type (ex-data e)))
        {:status 409 :body {:error (ex-message e) :data (dissoc (ex-data e) :type)}}
        {:status 500 :body {:error (ex-message e)}}))
    (catch Throwable e
      {:status 500 :body {:error (ex-message e)}})))

(defn- pending! []
  (try
    {:status 200 :body (mapv workflow->response (engine/list-pending))}
    (catch Throwable e
      {:status 500 :body {:error (ex-message e)}})))

(defn- try-read-edn [s]
  (try (edn/read-string s) (catch Throwable _ ::unreadable)))

(def ^:private hover-script
  "document.addEventListener('mouseover', function (e) {
     var tr = e.target.closest('tr[data-tenon-wf-id]');
     if (!tr) return;
     document.querySelectorAll('tr[data-tenon-wf-id=\"' + CSS.escape(tr.dataset.tenonWfId) + '\"]')
       .forEach(function (el) { el.classList.add('tenon-hl'); });
   });
   document.addEventListener('mouseout', function (e) {
     var tr = e.target.closest('tr[data-tenon-wf-id]');
     if (!tr) return;
     document.querySelectorAll('tr[data-tenon-wf-id=\"' + CSS.escape(tr.dataset.tenonWfId) + '\"]')
       .forEach(function (el) { el.classList.remove('tenon-hl'); });
   });
   function tenonRestart(url) {
     fetch(url, {method: 'POST'})
       .then(function (res) {
         return res.json().then(function (body) {
           if (res.ok) { location.reload(); }
           else { alert('Restart failed: ' + (body && body.error)); }
         });
       })
       .catch(function (err) { alert('Restart failed: ' + err); });
   }")

(defn- layout [title body]
  (->> [:html
        [:head [:title title] [:style ui/page-style]]
        [:body [:h1 title] body
         [:script (hiccup/raw hover-script)]]]
       (hiccup/html {:mode :html})
       (str)))

(defn- workflows-table
  ([rows] (workflows-table rows nil))
  ([rows footer]
   (if (empty? rows)
     [:p "none"]
     [:table
      [:thead [:tr [:th "Timestamp"] [:th "Workflow"] [:th "State"]]]
      [:tbody
       (for [{:keys [id wf_def state created_at]} rows]
         [:tr {:class (str "state-" state)}
          [:td [:a {:href (uri :workflows id)} (ui/timestamp created_at)]]
          [:td [:a {:href (uri :workflows id)} (ui/workflow-name wf_def)]]
          [:td [:a {:href (uri :workflows id)} (ui/state state)]]])]
      (when footer
        [:tfoot [:tr [:td {:colspan 3} footer]]])])))

(def ^:private states ["STARTED" "DONE" "ERROR"])

(defn- query-string [params]
  (->> params
       (remove (comp nil? second))
       (map (fn [[k v]] (str (name k) "=" (java.net.URLEncoder/encode (str v) "UTF-8"))))
       (str/join "&")))

(defn- filter-form [selected-state selected-wf-def show-all?]
  [:form {:method "get" :action (uri)
          :style "display:flex; align-items:center; background: lightsteelblue; margin-bottom:0; padding:4px 8px; border-top-left-radius:6px; border-top-right-radius:6px"}
   (ui/select-filter "state" "State" states selected-state)
   " "
   (ui/select-filter "wf_def" "Workflow" (db/top-level-wf-defs (current-ds) (not show-all?)) selected-wf-def)
   [:label {:style "margin-left:auto"}
    [:input (cond-> {:type "checkbox" :name "all" :value "1" :onchange "this.form.submit()"}
              show-all? (assoc :checked true))]
    "Show subworkflows"]])

(defn- pagination-footer [{:keys [selected-state selected-wf-def show-all? page-size next-cursor]}]
  [:div {:style "display:flex; align-items:center; gap:1em"}
   [:form {:method "get" :action (uri)}
    (when selected-state [:input {:type "hidden" :name "state" :value selected-state}])
    (when selected-wf-def [:input {:type "hidden" :name "wf_def" :value selected-wf-def}])
    (when show-all? [:input {:type "hidden" :name "all" :value "1"}])
    [:label "Page size: "
     [:select {:name "page_size" :onchange "this.form.submit()"}
      (for [ps ui/page-sizes]
        [:option (cond-> {:value ps} (= ps page-size) (assoc :selected true)) ps])]]]
   (if next-cursor
     [:a {:href (str (uri) "?" (query-string {:state selected-state :wf_def selected-wf-def
                                               :all (when show-all? "1") :page_size page-size
                                               :before_ts (:created-at next-cursor)
                                               :before_id (:id next-cursor)}))
          :style "padding:4px 10px; border:1px solid #999; border-radius:4px; background:#eee"}
      "Next page →"]
     [:span {:style "padding:4px 10px; color:#999"} "Next page →"])])

(defn- dashboard-html [selected-state selected-wf-def show-all? page-size before]
  (let [rows (db/top-level-workflows (current-ds)
                                      {:state selected-state :wf-def selected-wf-def
                                       :top-level-only? (not show-all?)
                                       :limit page-size :before before})
        has-more? (> (count rows) page-size)
        page-rows (vec (take page-size rows))
        next-cursor (when has-more?
                      (let [{:keys [created_at id]} (last page-rows)]
                        {:created-at created_at :id id}))]
    (layout "Workflows"
            [:div
             (filter-form selected-state selected-wf-def show-all?)
             (workflows-table page-rows
                               (pagination-footer {:selected-state selected-state
                                                    :selected-wf-def selected-wf-def
                                                    :show-all? show-all?
                                                    :page-size page-size
                                                    :next-cursor next-cursor}))])))

(defn- result-html [latest]
  (case (:state latest)
    "DONE" [:pre (pr-str (try-read-edn (:payload latest)))]
    "ERROR" (let [{:keys [message class data]} (try-read-edn (:payload latest))]
              [:div
               [:p [:b "Exception: "] (str class)]
               [:p message]
               (when data [:pre (pr-str data)])])
    [:p "(pending)"]))

(defn- event-payload-html [{:keys [state payload]}]
  (case state
    "DONE" [:pre (pr-str (try-read-edn payload))]
    "ERROR" (let [{:keys [message class data]} (try-read-edn payload)]
              [:span [:b (str class) ": "] message (when data (str " " (pr-str data)))])
    ""))

(defn- indent [depth]
  (apply str (repeat (* 2 depth) (char 0xA0))))

(defn- full-timeline-table [events]
  (if (empty? events)
    [:p "none"]
    [:table
     [:thead [:tr [:th "Timestamp"] [:th "Workflow"] [:th "State"] [:th "Payload"]]]
     [:tbody
      (for [{:keys [workflow_id wf_def state changed_at depth] :as ev} events]
        [:tr {:class (str "state-" state) :data-tenon-wf-id workflow_id}
         [:td changed_at]
         [:td [:a {:href (uri :workflows workflow_id)} (indent depth) (ui/workflow-name wf_def)]]
         [:td (ui/state state)]
         [:td (event-payload-html ev)]])]]))

(defn- can-restart? [wf latest]
  (and (contains? #{"DONE" "ERROR"} (:state latest))
       (some? (engine/lookup (symbol (:wf_def wf))))))

(defn- workflow-detail-html [id]
  (when-let [wf (db/get-workflow (current-ds) id)]
    (let [latest (db/latest-event (current-ds) id)]
      (layout (:wf_def wf)
              [:div
               [:p [:b "Id: "] (ui/workflow-link (uri :workflows id) id)]
               (when-let [parent-id (:parent_workflow_id wf)]
                 [:p [:b "Parent: "] (ui/workflow-link (uri :workflows parent-id) parent-id)])
               [:p [:b "State: "] (ui/state (:state latest))
                (when (can-restart? wf latest)
                  [:button {:onclick (str "tenonRestart('" (uri :workflows id :restart) "')")} "Restart"])]
               [:h2 "Arguments"]
               [:pre (pr-str (try-read-edn (:arguments wf)))]
               (when (:metadata wf)
                 [:div
                  [:h2 "Metadata"]
                  [:pre (pr-str (try-read-edn (:metadata wf)))]])
               [:h2 "Result"]
               (result-html latest)
               [:h2 "Timeline"]
               (full-timeline-table (db/full-timeline (current-ds) id))]))))

(defn- html-response [body]
  {:status 200 :headers {"Content-Type" "text/html; charset=utf-8"} :body body})

(defn handler
  "Ring handler for tenon's dashboard/detail pages and JSON API. engine is
   an application-state map as built by tenon.workflow/init -
   bound to engine/*workflow-engine* for the duration of the request, so
   every db-touching helper (here and in tenon.workflow, for
   restart-invocation/list-pending) can reach it without engine threaded
   through their own argument lists. prefix is the URI prefix tenon is
   mounted under - request's :uri is expected to already be relative to
   it (i.e. with prefix stripped, as wrap-handler does), and every
   link/URL this handler's HTML generates gets prefix prepended back on
   via *uri-prefix*, also bound here. Pass \"\" to mount unprefixed at the
   app's own root."
  [engine prefix {:keys [request-method uri query-params]}]
  (binding [*uri-prefix* prefix
            engine/*workflow-engine* engine]
    (cond
      (and (= request-method :get) (= uri "/"))
      (let [state (not-empty (get query-params "state"))
            wf-def (not-empty (get query-params "wf_def"))
            show-all? (some? (get query-params "all"))
            {:keys [page-size page_cursor]} (ui/parse-pagination query-params [:before_ts :before_id])
            before (when-let [[ts id] page_cursor] {:created-at ts :id id})]
        (html-response (dashboard-html state wf-def show-all? page-size before)))

      (and (= request-method :get) (= uri "/workflows/pending"))
      (pending!)

      (and (= request-method :post) (re-matches #"/workflows/[^/]+/restart" uri))
      (restart! (second (re-matches #"/workflows/([^/]+)/restart" uri)))

      (and (= request-method :get) (re-matches #"/workflows/[^/]+" uri))
      (if-let [page (workflow-detail-html (second (re-matches #"/workflows/([^/]+)" uri)))]
        (html-response page)
        {:status 404 :body {:error "not found"}})

      :else
      {:status 404 :body {:error "not found"}})))

(defn- strip-prefix
  "request-uri relative to prefix, or nil if request-uri isn't actually
   under prefix. prefix \"\" matches everything (identity - handler's own
   routes already start with \"/\"). request-uri exactly equal to prefix
   (no trailing slash, e.g. hitting \"/tenon\" for prefix \"/tenon\") maps
   to \"/\", tenon's own dashboard route."
  [prefix request-uri]
  (cond
    (= request-uri prefix) "/"
    (str/starts-with? request-uri (str prefix "/")) (subs request-uri (count prefix))
    :else nil))

(defn wrap-handler
  "Wraps original-handler (any ring handler) so a request whose :uri
   falls under tenon-uri-prefix is served by tenon's dashboard/detail
   pages and JSON API instead - with tenon-uri-prefix stripped from :uri
   before tenon routes it, and prepended back onto every link/URL tenon
   generates, so its UI keeps working when mounted under a prefix inside
   an existing app. Any other request passes through to original-handler
   unchanged. This is how tenon's HTML UI integrates into an existing app
   without owning its whole route space.

   engine is an application-state map as built by
   tenon.workflow/init - passed through to handler for every
   request tenon serves."
  [engine tenon-uri-prefix original-handler]
  (let [prefix (if (= "/" tenon-uri-prefix) "" tenon-uri-prefix)]
    (fn [{:keys [uri] :as request}]
      (if-let [relative-uri (strip-prefix prefix uri)]
        (handler engine prefix (assoc request :uri relative-uri))
        (original-handler request)))))

(def ^:private not-found-handler
  (constantly {:status 404 :body {:error "not found"}}))

(defn app
  "The standalone, unprefixed (mounted at \"/\") ring app."
  [engine]
  (-> (wrap-handler engine "" not-found-handler) wrap-params wrap-json-response))
