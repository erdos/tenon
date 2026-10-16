(ns tenon.workflow.http.listing-page
  "The workflow listing page (the dashboard at tenon's root): top-level
   workflows, newest first, filterable by state and workflow, paginated."
  (:require [ring.util.codec :as codec]
            [tenon.workflow :as engine]
            [tenon.workflow.ui :as ui]))

(defn- workflows-table
  ([rows] (workflows-table rows nil))
  ([rows footer]
   (if (empty? rows)
     [:p "none"]
     [:table
      [:thead [:tr [:th "Timestamp"] [:th "Workflow"] [:th "State"]]]
      [:tbody
       (for [{:keys [invocation_id wf_def state created_at reused]} rows]
         [:tr {:class (str "state-" state)}
          [:td [:a {:href (ui/uri :workflows invocation_id)} (ui/timestamp created_at)]]
          [:td [:a {:href (ui/uri :workflows invocation_id)} (ui/workflow-name wf_def)]]
          [:td [:a {:href (ui/uri :workflows invocation_id)} (ui/state state) (when reused [:span " " (ui/state "REUSED")])]]])]
      (when footer
        [:tfoot [:tr [:td {:colspan 3} footer]]])])))

(def ^:private states ["STARTED" "DONE" "ERROR"])

(defn- query-string [params]
  (codec/form-encode (into {} (for [[k v] params :when (some? v)] [(name k) v]))))

(defn- filter-form [selected-state selected-wf-def show-all?]
  [:form {:method "get" :action (ui/uri)
          :style "display:flex; align-items:center; background: lightsteelblue; margin-bottom:0; padding:4px 8px; border-top-left-radius:6px; border-top-right-radius:6px"}
   (ui/select-filter "state" "State" states selected-state)
   " "
   (ui/select-filter "wf_def" "Workflow" (engine/list-wf-defs (not show-all?)) selected-wf-def)
   [:label {:style "margin-left:auto"}
    [:input (cond-> {:type "checkbox" :name "all" :value "1" :onchange "this.form.submit()"}
              show-all? (assoc :checked true))]
    "Show subworkflows"]])

(defn- pagination-footer [{:keys [selected-state selected-wf-def show-all? page-size next-cursor]}]
  [:div {:style "display:flex; align-items:center; gap:1em"}
   [:form {:method "get" :action (ui/uri)}
    (when selected-state [:input {:type "hidden" :name "state" :value selected-state}])
    (when selected-wf-def [:input {:type "hidden" :name "wf_def" :value selected-wf-def}])
    (when show-all? [:input {:type "hidden" :name "all" :value "1"}])
    [:label "Page size: "
     [:select {:name "page_size" :onchange "this.form.submit()"}
      (for [ps ui/page-sizes]
        [:option (cond-> {:value ps} (= ps page-size) (assoc :selected true)) ps])]]]
   (if next-cursor
     [:a {:href (str (ui/uri) "?" (query-string {:state selected-state :wf_def selected-wf-def
                                               :all (when show-all? "1") :page_size page-size
                                               :before_id next-cursor}))
          :style "padding:4px 10px; border:1px solid #999; border-radius:4px; background:#eee"}
      "Next page →"]
     [:span {:style "padding:4px 10px; color:#999"} "Next page →"])])

(defn- dashboard-html [selected-state selected-wf-def show-all? page-size before]
  (let [rows (engine/list-invocations {:state selected-state :wf-def selected-wf-def
                                       :top-level-only? (not show-all?)
                                       :limit page-size :before before})
        has-more? (> (count rows) page-size)
        page-rows (vec (take page-size rows))
        next-cursor (when has-more? (:call_id (last page-rows)))]
    (ui/layout "Workflows"
            [:div
             (filter-form selected-state selected-wf-def show-all?)
             (workflows-table page-rows
                              (pagination-footer {:selected-state selected-state
                                                  :selected-wf-def selected-wf-def
                                                  :show-all? show-all?
                                                  :page-size page-size
                                                  :next-cursor next-cursor}))])))

(defn html
  "The listing page as an HTML string, for the request's query-params:
   state and wf_def filters, all (also list sub-workflows), page_size and
   before_id (the pagination cursor)."
  [query-params]
  (let [state (not-empty (get query-params "state"))
        wf-def (not-empty (get query-params "wf_def"))
        show-all? (some? (get query-params "all"))
        {:keys [page-size page_cursor]} (ui/parse-pagination query-params [:before_id])
        before (some-> page_cursor first parse-long)]
    (dashboard-html state wf-def show-all? page-size before)))
