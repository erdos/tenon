(ns tenon.workflow.http.listing-page
  "The workflow listing page (the dashboard at tenon's root): top-level
   workflows, newest first, filterable by state and workflow, paginated."
  (:require [tenon.workflow :as engine]
            [tenon.workflow.ui :as ui]))

(def ^:private refresh-seconds 10)

(defn- call-duration
  "How long the call ran - from its first start to its outcome, or while
   still STARTED, for how long it has been running. A reuse didn't run."
  [{:event/keys [created-at] :workflow/keys [state-changed-at] :as row} now]
  (case (ui/effective-state row)
    "REUSED" (ui/muted "—")
    "STARTED" (let [d (ui/format-duration (- now created-at))]
                [:span {:title (str "running for " d)} "running " d])
    (ui/format-duration (- state-changed-at created-at))))

(defn- call-state
  "The state chip - Reused for a call that got a stored result instead of
   running - flagged when a STARTED workflow's lease has run out."
  [{:workflow/keys [expires-at] :as row} now]
  (let [state (ui/effective-state row)]
    (list (ui/state state)
          (when (and (= "STARTED" state) expires-at (< expires-at now))
            [:span.wf-expired {:title (str "lease expired " (ui/format-duration (- now expires-at)) " ago")}
             " lease expired"]))))

(defn- workflows-table [rows show-all? now footer]
  (if (empty? rows)
    [:p "none"]
    [:table.workflows
     [:thead [:tr [:th "Timestamp"] [:th "Id"] [:th "Workflow"] (when show-all? [:th "Parent"])
              [:th "Duration"] [:th.state "State"]]]
     [:tbody
      (for [{wf-name :workflow/name :workflow/keys [invocation-id parent-invocation-id] :event/keys [created-at] :as row} rows
            :let [href (ui/uri :workflows invocation-id)]]
        [:tr {:class (str "state-" (ui/effective-state row))}
         [:td [:a {:href href} (ui/timestamp created-at)]]
         [:td (ui/workflow-link invocation-id)]
         [:td [:a {:href href} (ui/workflow-name wf-name)]]
         (when show-all?
           [:td (if parent-invocation-id
                  (ui/workflow-link parent-invocation-id)
                  [:a {:href href} (ui/muted "—")])])
         [:td.duration [:a {:href href} (call-duration row now)]]
         [:td.state [:a {:href href} (call-state row now)]]])]
     [:tfoot [:tr [:td {:colspan (if show-all? 6 5)} footer]]]]))

(def ^:private states ["STARTED" "DONE" "ERROR" "TIMEOUT"])

(defn- listing-params
  "The listing's query params for filters f - page_size only when not the
   default."
  [{:keys [state wf-def show-all? page-size refresh?]}]
  {:state state
   :wf_def wf-def
   :all (when show-all? "1")
   :page_size (when (not= ui/default-page-size page-size) page-size)
   :refresh (when refresh? "1")})

(defn- hidden-inputs [params]
  (for [[k v] params :when (some? v)]
    [:input {:type "hidden" :name (name k) :value v}]))

(defn- filter-form [{:keys [state wf-def show-all? refresh?] :as f}]
  [:form.filters {:method "get" :action (ui/uri)}
   (hidden-inputs (select-keys (listing-params f) [:page_size :refresh]))
   (ui/select-filter "state" "State" states state)
   (ui/select-filter "wf_def" "Workflow" (engine/list-wf-defs (not show-all?)) wf-def)
   [:span.filters-right
    [:label
     [:input (cond-> {:type "checkbox" :name "all" :value "1" :onchange "this.form.submit()"}
               show-all? (assoc :checked true))]
     "Show sub-workflows"]
    [:a {:class (str "btn" (when refresh? " on"))
         :href (ui/uri (listing-params (update f :refresh? not)))
         :title (if refresh?
                  (str "reloading every " refresh-seconds "s - click to stop")
                  (str "reload this page every " refresh-seconds "s"))}
     "↻ Auto-refresh " (if refresh? "on" "off")]]])

(defn- pagination-footer [f next-cursor]
  [:div.pager
   [:form {:method "get" :action (ui/uri)}
    (hidden-inputs (dissoc (listing-params f) :page_size))
    [:label "Page size: "
     [:select {:name "page_size" :onchange "this.form.submit()"}
      (for [ps ui/page-sizes]
        [:option (cond-> {:value ps} (= ps (:page-size f)) (assoc :selected true)) ps])]]]
   (if next-cursor
     [:a.btn.primary {:href (ui/uri (assoc (listing-params f) :before_id next-cursor))} "Next page →"]
     [:span.btn.disabled "Next page →"])])

(defn- dashboard-html [{:keys [state wf-def show-all? page-size refresh?] :as f} before]
  (let [rows (engine/list-invocations {:state state :wf-def wf-def
                                       :top-level-only? (not show-all?)
                                       :limit page-size :before before})
        has-more? (> (count rows) page-size)
        page-rows (vec (take page-size rows))
        next-cursor (when has-more? (:event/call-id (last page-rows)))]
    (ui/layout "Workflows"
               {:refresh (when refresh? refresh-seconds)}
               [:div
                (filter-form f)
                (workflows-table page-rows show-all? (System/currentTimeMillis)
                                 (pagination-footer f next-cursor))])))

(defn html
  "The listing page as an HTML string, for the request's query-params:
   state and wf_def filters, all (also list sub-workflows), page_size,
   before_id (the pagination cursor) and refresh (reload the page
   periodically)."
  [query-params]
  (let [{:keys [page-size page_cursor]} (ui/parse-pagination query-params [:before_id])]
    (dashboard-html {:state (not-empty (get query-params "state"))
                     :wf-def (not-empty (get query-params "wf_def"))
                     :show-all? (some? (get query-params "all"))
                     :page-size page-size
                     :refresh? (some? (get query-params "refresh"))}
                    (some-> page_cursor first parse-long))))
