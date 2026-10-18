(ns tenon.workflow.http.details-page
  "The page of a single workflow (/workflows/{id}): its summary, params,
   result and metadata, and the timeline of it and its sub-workflows."
  (:require [tenon.workflow :as engine]
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

(defn- metadata-html [metadata-str]
  (let [metadata (ui/read-edn metadata-str)]
    (ui/panel {:title "Metadata"}
              (if (and (map? metadata) (seq metadata))
                (ui/key-values (for [[k v] (sort-by (comp str key) metadata)]
                                 [[:code (pr-str k)] (ui/folded-pre (ui/pretty v))]))
                (ui/edn-block metadata-str)))))

(defn- result-html [wf]
  (case (:state wf)
    "DONE" (ui/panel {:title "Result" :subtitle "done"} (ui/edn-block (:result wf)))
    "ERROR" (let [error (ui/read-edn (:result wf))]
              (ui/panel {:title "Error"}
                        (if (map? error)
                          (let [{:keys [type message data]} (first (:via error))
                                class (or (:class error) type)
                                msg (or (:message error) (:cause error) message)
                                ex-data (or (:data error) data)]
                            (ui/error-details class msg
                                              (when ex-data [:div (ui/muted "ex-data") (ui/folded-pre (ui/pretty ex-data))])))
                          (ui/edn-block (:result wf)))))
    (ui/panel {:title "Result"}
              (ui/muted "Pending \u2014 no result yet."))))

(defn- timeline-tree
  "Groups full-timeline events (in chronological order) into a tree of
   workflow nodes, as a map from node key to {:events [[index event] ...]}.
   A node key is the path of current_invocation_ids from the root down:
   [:root] for the workflow itself (every depth 0 event, its own REUSED
   rows included), the parent's key plus the workflow's id below it. A
   REUSED row hangs under the reusing invocation's node, as a node of its
   own - the reused workflow didn't run there."
  [events]
  (let [indexed (map-indexed vector events)
        by-invocation (->> indexed
                           (remove (fn [[_ ev]] (= "REUSED" (:state ev))))
                           (reduce (fn [m [_ ev]] (update m (:invocation_id ev) #(or % ev))) {}))
        node-key (fn node-key [{:keys [depth current_invocation_id parent_invocation_id]}]
                   (if (zero? depth)
                     [:root]
                     (conj (if-let [parent (by-invocation parent_invocation_id)]
                             (node-key parent)
                             [:root])
                           current_invocation_id)))]
    (reduce (fn [nodes [i ev]] (update-in nodes [(node-key ev) :events] (fnil conj []) [i ev]))
            {} indexed)))

(defn- reused? [ev] (= "REUSED" (:state ev)))

(defn- run-state
  "The outcome of a node's run - its latest state other than REUSED (a
   reuse doesn't change it). A node with nothing but REUSED rows is a reuse
   within the tree: REUSED."
  [events]
  (or (->> events (map second) (remove reused?) last :state) "REUSED"))

(defn- run-duration
  "The node's run from its first STARTED (or, with :last-attempt, its
   latest one - leaving out the time earlier attempts took before a
   restart) to its outcome - or, while still STARTED, how long it has
   been running (saying so in its tooltip). nil for a reuse-only node."
  ([events] (run-duration events :whole-run))
  ([events from]
   (let [run (remove reused? (map second events))
         starts (filter #(= "STARTED" (:state %)) run)]
     (when-let [start (:state_changed_at (if (= :last-attempt from) (last starts) (first starts)))]
       (if (= "STARTED" (:state (last run)))
         (let [d (ui/format-duration (- (System/currentTimeMillis) start))]
           [:span {:title (str "running for " d)} d])
         (ui/format-duration (- (:state_changed_at (last run)) start)))))))

(defn- error-title
  "\"class: message\" of an ERROR event, for a chip's tooltip."
  [ev]
  (let [error (ui/read-edn (:data ev))
        {:keys [type message]} (first (:via error))]
    (str (or (:class error) type) ": " (or (:message error) (:cause error) message))))

(defn- problem?
  "Whether node k's subtree has a workflow that failed or is still running."
  [nodes children k]
  (or (contains? #{"ERROR" "STARTED"} (run-state (get-in nodes [k :events])))
      (some #(problem? nodes children %) (children k))))

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
   [:code.tree-offset {:title (str (ui/timestamp-str (:state_changed_at ev)) " UTC")}
    (ui/format-offset (- (:state_changed_at ev) start))]
   [:span.tree-verb (or verb
                        (case (:state ev)
                          ("STARTED" "REUSED") "Called"
                          "DONE" "Completed"
                          "ERROR" "Failed"
                          (str (:state ev))))]))

(defn- timeline-tree-event
  "An event of a node's run. A REUSED one (the workflow's own reuses -
   ones within the tree are leaves of their own) names the invocation
   whose result was reused and the caller that reused it - nothing for a
   top-level call, which had no caller. A STARTED one other than the
   node's first event (its run always opens with one) is a restart."
  [start ev root? restart?]
  [:li {:class (str "tree-event state-" (:state ev))}
   (tree-offset start ev (when restart? "Restarted"))
   (when-let [caller (and (reused? ev) root? (:parent_invocation_id ev))]
     [:span.tree-reuse
      "from " (ui/workflow-link (:invocation_id ev))
      " for " (ui/workflow-link caller)])
   (tree-columns
    nil
    nil
    (ui/state-with-tooltip (:state ev) (when (= "ERROR" (:state ev)) (error-title ev))))])

(defn- timeline-tree-reuse
  "A reuse within the tree: its single REUSED row, as a leaf - there is
   nothing in it to fold. It names the producing invocation only when that
   isn't the reused workflow's current one (it was restarted since)."
  [start {:keys [current_invocation_id invocation_id wf_def] :as ev}]
  [:li.tree-leaf.state-REUSED
   [:span.tree-bullet "\u2022"]
   (tree-offset start ev "Invoked")
   (ui/workflow-name wf_def)
   (ui/workflow-link current_invocation_id)
   (when (not= invocation_id current_invocation_id)
     [:span.tree-reuse "from " (ui/workflow-link invocation_id)])
   (tree-columns
    nil
    nil
    (ui/state-with-tooltip "REUSED" nil))])

(defn- timeline-tree-node [nodes children start k]
  (let [events (get-in nodes [k :events])
        {:keys [current_invocation_id wf_def]} (second (first events))
        state (run-state events)
        reuses (count (filter (comp reused? second) events))
        duration (run-duration events)
        last-error (->> events (map second) (filter #(= "ERROR" (:state %))) last)
        items (->> (concat (for [[i ev] events] [i :event ev])
                           (for [ck (children k)] [(ffirst (get-in nodes [ck :events])) :node ck]))
                   (sort-by first))]
    (if (= "REUSED" state)
      (timeline-tree-reuse start (second (first events)))
    [:li
     [:details (when (or (= [:root] k) (problem? nodes children k)) {:open true})
      [:summary {:class (str "state-" state)}
       [:span.tree-verb "Invoked"]
       (ui/workflow-name wf_def)
       (when-not (= [:root] k)
         (ui/workflow-link current_invocation_id))
       (tree-columns
        duration
        (when (pos? reuses)
          [:span {:title (str "result reused " (ui/pluralize reuses "time"))}
           (str "\u21BA " reuses)])
        (ui/state-with-tooltip state (when (= "ERROR" state) (some-> last-error error-title))))]
      [:ul
       (for [[i kind x] items]
         (case kind
           :event (timeline-tree-event start x (= [:root] k)
                                       (and (= "STARTED" (:state x)) (not= i (ffirst events))))
           :node (timeline-tree-node nodes children start x)))]]])))

(defn- full-timeline-tree
  "The timeline as a foldable tree. Nodes whose subtree has a failed or
   still running workflow start open, finished ones folded (the root is
   always open). Event times are offsets from the timeline's start, the
   absolute time in their tooltip."
  [events]
  (if (empty? events)
    [:p "none"]
    (let [nodes (timeline-tree events)
          children (->> (keys nodes)
                        (remove #{[:root]})
                        (group-by pop))
          start (reduce min (map :state_changed_at events))]
      [:ul.tree (timeline-tree-node nodes children start [:root])])))

(defn- can-restart? [wf]
  (and (or (#{"DONE" "ERROR"} (:state wf))
           (:expired wf))
       (some? (engine/lookup (symbol (:wf_def wf))))))

(defn- unregistered? [wf]
  (nil? (engine/lookup (symbol (:wf_def wf)))))

(defn- workflow-summary
  "The card at the top of the detail page: the workflow's state, how it ran
   and how it relates to other workflows. requested-id is the id in the
   URL - a past invocation of a since restarted workflow shows the current
   one, saying so. events is its full-timeline."
  [wf requested-id events restart-error]
  (let [id (:invocation_id wf)
        own (filter (comp zero? :depth) events)
        runs (remove reused? own)
        started-at (:state_changed_at (first (filter (comp #{"STARTED"} :state) runs)))
        attempts (count (filter (comp #{"STARTED"} :state) runs))
        reuses (count (filter reused? own))
        duration (run-duration (map vector (range) own) :last-attempt)
        now (System/currentTimeMillis)]
    (ui/summary-card
     {:state (:state wf)
      :head [(when (:expired wf) [:span.chip.wf-expired "Expired"])
             (when duration [:span.wf-summary-duration duration])
             (if (can-restart? wf)
               (ui/panel-toggle-btn
                [:button "\u21BB Restart"]
                [:form.restart {:method "post" :action (ui/uri :workflows id :restart)}
                 [:span "Run it again with the same params, under a new invocation id?"]
                 [:button.btn.danger {:type "submit"} "Yes, restart"]])
               ;; When wf_def is not registered in this process, show a warning
               ;; badge so operators know why the Restart button is missing.
               (when (unregistered? wf)
                 [:span.chip.wf-unregistered "Unregistered var \u2014 cannot restart in this process"]))]
      :error (when restart-error ["Restart failed: " restart-error])
      :note (when (not= requested-id id)
              ["Invocation " (ui/workflow-id requested-id)
               " was restarted - showing the workflow's current invocation."])}
     (ui/fact "Id" (ui/workflow-link id))
     (ui/fact "Parent" (if-let [parent-id (:parent_invocation_id wf)]
                         (ui/workflow-link parent-id)
                         "\u2014 (top level)"))
     (ui/fact "Started" (ui/timestamp-with-relative started-at now))
     (ui/fact (if (= "STARTED" (:state wf)) "Last change" "Finished")
              (ui/timestamp-with-relative (:state_changed_at wf) now))
     (when (and (= "STARTED" (:state wf)) (:expires_at wf))
       (let [left (- (:expires_at wf) now)]
         (ui/fact "Lease"
                  (if (pos? left)
                    (str "expires in " (ui/format-duration left))
                    [:span.wf-expired (str "expired " (ui/format-duration (- left)) " ago")]))))
     (ui/fact "Attempts" (str attempts))
     (ui/fact "Reused" (if (pos? reuses) (ui/pluralize reuses "time") "never")))))

(defn- ancestors-of
  "The workflows wf runs nested under, top-level first."
  [wf]
  (loop [parent-id (:parent_invocation_id wf) acc ()]
    (if-let [parent (when (and parent-id (< (count acc) 100))
                      (engine/get-invocation parent-id))]
      (recur (:parent_invocation_id parent) (cons parent acc))
      acc)))

(defn html
  "The page of the workflow requested-id belongs to as an HTML string, or
   nil if there is no such workflow. restart-error, if not nil, is a
   failed restart's message to show."
  [requested-id restart-error]
  (when-let [wf (engine/get-invocation requested-id)]
    (let [id (:invocation_id wf)
          events (engine/full-timeline id)
          ancestors (ancestors-of wf)]
      (ui/layout (:wf_def wf)
              {:back [(ui/uri) "Workflows"]
               :tab-title (str (ui/state-symbol (:state wf)) " " (:wf_def wf) " \u00B7 " id)}
              [:div
               (when (seq ancestors)
                 (ui/breadcrumb (concat (for [a ancestors]
                                          [[:span (ui/workflow-name (:wf_def a)) " " (ui/workflow-id (:invocation_id a))]
                                           (ui/uri :workflows (:invocation_id a))])
                                        [[[:span (ui/workflow-name (:wf_def wf)) " " (ui/workflow-id id)] nil]])))
               (workflow-summary wf requested-id events restart-error)
               [:div.panels
                (params-html (:params wf))
                (result-html wf)
                (when (:metadata wf) (metadata-html (:metadata wf)))]
               [:h2 "Timeline"]
               (full-timeline-tree events)]))))
