(ns tenon.workflow.http.details-page
  "The page of a single workflow (/workflows/{id}): its summary, params,
   result and metadata, and the timeline of it and its sub-workflows."
  (:require [clojure.string :as str]
            [tenon.workflow :as engine]
            [tenon.workflow.http.endpoint :as endpoint]
            [tenon.workflow.ui :as ui]))

(defn- params-html [params-str]
  (let [params (ui/read-edn params-str)]
    (cond
      (not (sequential? params)) (ui/panel {:title "Params"} (ui/edn-block params-str))
      (empty? params) (ui/panel {:title "Params" :subtitle "no arguments"})
      :else (ui/panel {:title "Params"
                       :subtitle (ui/pluralize (count params) "argument")}
                      (ui/key-values (for [[i arg] (map-indexed vector params)]
                                       [[:code (str "%" (inc i))] (ui/folded-pre (ui/pretty arg))]))))))

(defn- metadata-body
  "Metadata EDN as key-value rows, one per key - or the raw EDN block if it
   isn't a non-empty map."
  [metadata-str]
  (let [metadata (ui/read-edn metadata-str)]
    (if (and (map? metadata) (seq metadata))
      (ui/key-values (for [[k v] (sort-by (comp str key) metadata)]
                       [[:code (pr-str k)] (ui/folded-pre (ui/pretty v))]))
      (ui/edn-block metadata-str))))

(defn- metadata-html [metadata-str]
  (ui/panel {:title "Metadata"} (metadata-body metadata-str)))

(defmulti ^:private result-panel :workflow/state)

(defmethod result-panel "DONE" [wf]
  (ui/panel {:title "Result" :subtitle "done"} (ui/edn-block (:workflow/result wf))))

(defn- failure-panel [title {result :workflow/result}]
  (ui/panel {:title title}
            (if-let [summary (ui/error-summary (ui/read-edn result))]
              (ui/error-details summary)
              (ui/edn-block result))))

(defmethod result-panel "ERROR" [wf] (failure-panel "Error" wf))

(defmethod result-panel "TIMEOUT" [wf] (failure-panel "Timeout" wf))

(defmethod result-panel :default [_]
  (ui/panel {:title "Result"}
            (ui/muted "Pending \u2014 no result yet.")))

(defn- node-keys
  "The tree node key of each of events: the path of current invocation ids
   from the root down - [:root] for the workflow itself (every depth 0
   event, its own REUSED rows included), the parent's key plus the
   workflow's id below it. A REUSED row hangs under the reusing
   invocation's node, as a node of its own - the reused workflow didn't
   run there."
  [events]
  (let [first-run (reduce (fn [m ev]
                            (cond-> m
                              (not (or (engine/reused? ev) (m (:workflow/invocation-id ev))))
                              (assoc (:workflow/invocation-id ev) ev)))
                          {} events)
        node-key (memoize
                  (fn node-key [{:workflow/keys [depth current-invocation-id parent-invocation-id]}]
                    (if (zero? depth)
                      [:root]
                      (conj (if-let [parent (first-run parent-invocation-id)]
                              (node-key parent)
                              [:root])
                            current-invocation-id))))]
    (mapv node-key events)))

(defn timeline-tree
  "full-timeline events (in chronological order) as a tree of nodes, the
   root being the workflow itself - nil if there are none. A node is
   either a reuse within the tree, {:kind :reuse :index i :event ev} - a
   leaf - or a workflow's run:

     :kind            :run
     :index           position of its first event in events
     :root?           whether it is the root
     :name, :invocation-id  the workflow's name and current invocation id
     :outcome         its latest event that is no REUSED one
     :state           the state of :outcome
     :problem?        whether it or a node below failed or still runs
     :attempts        its number of STARTED events
     :reuses          its number of REUSED events
     :started-at, :last-started-at  epoch ms of its first and latest start
     :ended-at        epoch ms of :outcome, nil while still running
     :items           its events {:kind :event :index i :event ev
                      :restart? r :attempt-started-at t} (r: a STARTED one
                      that is not its first event, t: the latest start
                      before it) and the nodes below it, by :index"
  [events]
  (when (seq events)
    (let [entries (map vector (node-keys events) (range) events)
          by-key (group-by first entries)
          children (group-by pop (remove #{[:root]} (keys by-key)))
          event-items (fn [entries]
                        (let [first-index (second (first entries))]
                          (first (reduce (fn [[items started] [_ i ev]]
                                           [(conj items {:kind :event :index i :event ev
                                                         :restart? (and (engine/started? ev) (not= i first-index))
                                                         :attempt-started-at (:workflow/state-changed-at started)})
                                            (if (engine/started? ev) ev started)])
                                         [[] nil] entries))))
          node (fn node [k]
                 (let [entries (by-key k)
                       [_ index ev] (first entries)
                       evs (map #(nth % 2) entries)
                       run (remove engine/reused? evs)
                       outcome (last run)]
                   (if-not outcome
                     {:kind :reuse :index index :event ev}
                     (let [below (mapv node (children k))
                           starts (filter engine/started? run)]
                       {:kind :run
                        :index index
                        :root? (= [:root] k)
                        :name (:workflow/name ev)
                        :invocation-id (:workflow/current-invocation-id ev)
                        :outcome outcome
                        :state (:workflow/state outcome)
                        :problem? (boolean (or (engine/failed? outcome) (engine/started? outcome)
                                               (some :problem? below)))
                        :attempts (count starts)
                        :reuses (count (filter engine/reused? evs))
                        :started-at (:workflow/state-changed-at (first starts))
                        :last-started-at (:workflow/state-changed-at (last starts))
                        :ended-at (when-not (engine/started? outcome) (:workflow/state-changed-at outcome))
                        :items (vec (sort-by :index (concat (event-items entries) below)))}))))]
      (node [:root]))))

(defn- duration-html
  "How long a run took from from to to - or, with no to, how long it has
   been running (saying so in its tooltip). nil without from."
  [from to]
  (when from
    (if to
      (ui/format-duration from to)
      (let [d (ui/format-duration from (System/currentTimeMillis))]
        [:span {:title (str "running for " d)} d]))))

(defn- error-title
  "\"class: message\" of an ERROR or TIMEOUT event, for a chip's tooltip."
  [ev]
  (let [{:keys [class message]} (ui/error-summary (ui/read-edn (:event/data ev)))]
    (str class ": " message)))

(defn- error-class-html
  "The exception class of an ERROR or TIMEOUT event, its package muted so the class
   name stands out - nil if the saved error has none."
  [ev]
  (when-let [cls (some-> (ui/error-summary (ui/read-edn (:event/data ev))) :class str)]
    (if-let [i (str/last-index-of cls ".")]
      [:code.err-class [:span.wf-ns (subs cls 0 (inc i))] [:span.err-name (subs cls (inc i))]]
      [:code.err-class [:span.err-name cls]])))

(defn- tree-columns
  "The tabular right side of a tree row: a node's duration, reuse count and
   state - always all three cells, possibly empty, so they line up as
   columns across rows of any depth."
  [time reuses state]
  [:span.tree-cols
   [:span.tree-time time]
   [:span.tree-count reuses]
   [:span.tree-state state]])

(defn- tree-offset
  "An event row's leading cells: its offset from the timeline's start (the
   absolute time in its tooltip), then what happened - verb, or one
   derived from the event's state."
  [start ev & [verb]]
  (list
   [:code.tree-offset {:title (str (ui/timestamp-str (:workflow/state-changed-at ev)) " UTC")}
    (ui/format-offset (- (:workflow/state-changed-at ev) start))]
   [:span.tree-verb (or verb
                        (get-in ui/state-info [(:workflow/state ev) :verb])
                        (str (:workflow/state ev)))]))

(defn- error-body
  "The full error of an ERROR or TIMEOUT event as saved (a Throwable->map): class,
   message, ex-data, the chain of causes (via) and the stack trace. Not a
   map - shown as the raw EDN."
  [data-str]
  (let [{:keys [via trace phase] :as error} (ui/read-edn data-str)]
    (if-let [summary (ui/error-summary error)]
      [:div.event-error-body
       (ui/error-details summary
                         (when phase [:div (ui/muted "phase") [:code (pr-str phase)]])
                         (when (next via)
                           [:div (ui/muted "caused by")
                            [:ol.error-via
                             (for [{:keys [type message at]} via]
                               [:li [:code (str type)] (when message (str ": " message))
                                (when at [:div.error-at (ui/frame-str at)])])]])
                         (when (seq trace)
                           [:div (ui/muted "stack trace")
                            (ui/stack-trace trace)]))]
      (ui/edn-block data-str))))

(defmulti ^:private event-fold-body :workflow/state)

(defmethod event-fold-body "ERROR" [ev]
  (when-some [data (:event/data ev)] (error-body data)))

(defmethod event-fold-body "TIMEOUT" [ev]
  (when-some [data (:event/data ev)] (error-body data)))

(defmethod event-fold-body "STARTED" [ev]
  (when-some [data (:event/data ev)]
    [:div.event-error-body (ui/muted "Metadata") (metadata-body data)]))

(defmethod event-fold-body "REUSED" [ev]
  (when-some [data (:event/data ev)]
    [:div.event-error-body (ui/muted "Metadata") (metadata-body data)]))

(defmethod event-fold-body :default [_] nil)

(defn- timeline-tree-event
  "An event of a node's run. A REUSED one (the workflow's own reuses -
   ones within the tree are leaves of their own) names the invocation
   whose result was reused and the caller that reused it - nothing for a
   top-level call, which had no caller. A STARTED one other than the
   node's first event (its run always opens with one) is a restart. A DONE
   one shows how long the attempt took, from its started event (the latest
   before it). One with something to show folds (closed) to it, see
   event-fold-body."
  [start root? {ev :event :keys [restart? attempt-started-at]}]
  (let [row (list
             (tree-offset start ev (when restart? "Restarted"))
             (when (engine/failed? ev) (error-class-html ev))
             (when-let [caller (and (engine/reused? ev) root? (:workflow/parent-invocation-id ev))]
               [:span.tree-reuse
                "from " (ui/workflow-link (:workflow/invocation-id ev))
                " for " (ui/workflow-link caller)])
             (tree-columns
              (when (and attempt-started-at (engine/done? ev))
                (ui/format-duration attempt-started-at (:workflow/state-changed-at ev)))
              nil
              (ui/state-with-tooltip (:workflow/state ev) (when (engine/failed? ev) (error-title ev)))))]
    (if-let [body (event-fold-body ev)]
      [:li {:class (str "tree-error" (when-not (engine/failed? ev) " tree-meta"))}
       [:details
        (into [:summary {:class (str "state-" (:workflow/state ev))}] row)
        body]]
      (into [:li {:class (str "tree-event state-" (:workflow/state ev))}] row))))

(defn- timeline-tree-reuse
  "A reuse within the tree: its single REUSED row, as a leaf. It names the
   producing invocation only when that isn't the reused workflow's current
   one (it was restarted since). With metadata it folds (closed) to it,
   otherwise there is nothing in it to fold."
  [start {wf-name :workflow/name :workflow/keys [current-invocation-id invocation-id] :event/keys [data] :as ev}]
  (let [row (list
             (tree-offset start ev "Invoked")
             (ui/workflow-name wf-name)
             (ui/workflow-link current-invocation-id)
             (when (not= invocation-id current-invocation-id)
               [:span.tree-reuse "from " (ui/workflow-link invocation-id)])
             (tree-columns
              nil
              nil
              (ui/state-with-tooltip "REUSED" nil)))]
    (if data
      [:li.tree-error.tree-meta.state-REUSED
       [:details
        (into [:summary {:class "state-REUSED"}] row)
        [:div.event-error-body (ui/muted "Metadata") (metadata-body data)]]]
      (into [:li.tree-leaf.state-REUSED [:span.tree-bullet "\u2022"]] row))))

(defn- timeline-tree-node
  [start {:keys [kind root? name invocation-id outcome state problem? reuses started-at ended-at items] :as node}]
  (if (= :reuse kind)
    (timeline-tree-reuse start (:event node))
    [:li
     [:details (when (or root? problem?) {:open true})
      [:summary {:class (str "state-" state)}
       [:span.tree-verb "Invoked"]
       (ui/workflow-name name)
       (when-not root?
         (ui/workflow-link invocation-id))
       (tree-columns
        (duration-html started-at ended-at)
        (when (pos? reuses)
          [:span {:title (str "result reused " (ui/pluralize reuses "time"))}
           (str "\u21BA " reuses)])
        (ui/state-with-tooltip state (when (engine/failed? outcome) (error-title outcome))))]
      [:ul
       (for [item items]
         (if (= :event (:kind item))
           (timeline-tree-event start root? item)
           (timeline-tree-node start item)))]]]))

(defn- full-timeline-tree
  "The timeline tree as a foldable one. Nodes whose subtree has a failed or
   still running workflow start open, finished ones folded (the root is
   always open). Event times are offsets from the timeline's start, the
   absolute time in their tooltip."
  [tree events]
  (if tree
    [:ul.tree (timeline-tree-node (reduce min (map :workflow/state-changed-at events)) tree)]
    [:p "none"]))

(defn- can-restart? [wf registered?]
  (and (or (engine/finished? wf) (engine/expired? wf)) registered?))

(defn- workflow-summary
  "The card at the top of the detail page: the workflow's state, how it ran
   and how it relates to other workflows. requested-id is the id in the
   URL - a past invocation of a since restarted workflow shows the current
   one, saying so. Its numbers are those of the timeline tree's root -
   leaving out, as its duration, the time attempts before its latest
   restart took."
  [{wf :workflow {:keys [started-at last-started-at ended-at attempts reuses] :or {attempts 0 reuses 0}} :tree
    :keys [requested-id restart-error registered?]}]
  (let [id (:workflow/invocation-id wf)
        duration (duration-html last-started-at ended-at)
        now (System/currentTimeMillis)]
    (ui/summary-card
     {:state (:workflow/state wf)
      :head [(when (engine/expired? wf) [:span.chip.wf-expired "Expired"])
             (when duration [:span.wf-summary-duration duration])
             (when (can-restart? wf registered?)
               (ui/panel-toggle-btn
                [:button "\u21BB Restart"]
                [:div.restart
                 [:span "Run it again with the same params, under a new invocation id? Recursively also restarts its failed nested workflows, innermost first."]
                 [:form.restart {:method "post" :action (ui/uri :workflows id :restart)}
                  [:button.btn.danger {:type "submit"} "Restart"]]
                 [:form.restart {:method "post" :action (ui/uri :workflows id :restart {:recursive true})}
                  [:button.btn.danger {:type "submit"
                                       :title "Restarts failed nested workflows first, innermost first"}
                   "Restart recursively"]]]))
             ;; When wf_def is not registered in this process, show a warning
             ;; badge so operators know why the Restart buttons are missing.
             (when-not registered?
               [:span.chip.wf-unregistered "Unregistered var \u2014 cannot restart in this process"])]
      :error (when restart-error ["Restart failed: " restart-error])
      :note (when (not= requested-id id)
              ["Invocation " (ui/workflow-id requested-id)
               " was restarted - showing the workflow's current invocation."])}
     (ui/fact "Id" (ui/workflow-link id))
     (ui/fact "Parent" (if-let [parent-id (:workflow/parent-invocation-id wf)]
                         (ui/workflow-link parent-id)
                         "\u2014 (top level)"))
     (ui/fact "Started" (ui/timestamp-with-relative started-at now))
     (ui/fact (if (engine/started? wf) "Last change" "Finished")
              (ui/timestamp-with-relative (:workflow/state-changed-at wf) now))
     (when (and (engine/started? wf) (:workflow/expires-at wf))
       (let [left (- (:workflow/expires-at wf) now)]
         (ui/fact "Lease"
                  (if (pos? left)
                    (str "expires in " (ui/format-duration now (:workflow/expires-at wf)))
                    [:span.wf-expired (str "expired " (ui/format-duration (:workflow/expires-at wf) now) " ago")]))))
     (ui/fact "Attempts" (str attempts))
     (ui/fact "Reused" (if (pos? reuses) (ui/pluralize reuses "time") "never")))))

(defn- ancestors-of
  "The workflows wf runs nested under, top-level first."
  [wf]
  (loop [parent-id (:workflow/parent-invocation-id wf) acc ()]
    (if-let [parent (when (and parent-id (< (count acc) 100))
                      (engine/get-invocation parent-id))]
      (recur (:workflow/parent-invocation-id parent) (cons parent acc))
      acc)))

(def ^:private expand-all-js
  "let d=[...document.querySelectorAll('ul.tree details')],o=d.every(x=>x.open);d.forEach(x=>x.open=!o);this.textContent=o?'Expand all':'Collapse all'")

(defmethod endpoint/model :view/details
  [_ {{requested-id :id} :path-params :keys [query-params]}]
  (when-let [wf (engine/get-invocation requested-id)]
    (let [events (vec (engine/full-timeline (:workflow/invocation-id wf)))]
      {:workflow wf
       :requested-id requested-id
       :restart-error (not-empty (get query-params "restart_error"))
       :events events
       :tree (timeline-tree events)
       :ancestors (vec (ancestors-of wf))
       :registered? (some? (engine/lookup (symbol (:workflow/name wf))))})))

(defmethod endpoint/view :view/details [_ {wf :workflow :keys [events tree ancestors] :as m}]
  (let [id (:workflow/invocation-id wf)]
    (ui/layout (:workflow/name wf)
               {:back [(ui/uri) "Workflows"]
                :tab-title (str (ui/state-symbol (:workflow/state wf)) " " (:workflow/name wf) " \u00B7 " id)}
               [:div
                (when (seq ancestors)
                  (ui/breadcrumb (concat (for [a ancestors]
                                           [[:span (ui/workflow-name (:workflow/name a)) " " (ui/workflow-id (:workflow/invocation-id a))]
                                            (ui/uri :workflows (:workflow/invocation-id a))])
                                         [[[:span (ui/workflow-name (:workflow/name wf)) " " (ui/workflow-id id)] nil]])))
                (workflow-summary m)
                [:div.panels
                 (params-html (:workflow/params wf))
                 (result-panel wf)
                 (when (:workflow/metadata wf) (metadata-html (:workflow/metadata wf)))]
                [:div.page-head
                 [:h2 "Timeline"]
                 (when (seq events)
                   [:div.head-actions
                    [:button.btn {:type "button" :onclick expand-all-js} "Expand all"]])]
                (full-timeline-tree tree events)])))
