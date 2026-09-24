(ns tenon.workflow.ui
  "HTML UI components")

(defn workflow-id [id]
  [:code (str id)])

(defn workflow-link
  ([id] (workflow-link (str "/workflows/" id) id))
  ([href id]
   [:a {:href href :style "color:#0645ad; text-decoration:underline"} (workflow-id id)]))

(defn workflow-name [name]
  [:code (str name)])

(defn timestamp
  "Renders an epoch-milliseconds timestamp as UTC, e.g. 2026-01-02 03:04:05.678."
  [epoch-ms]
  [:code (when epoch-ms
           (.format (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSS")
                    (java.time.LocalDateTime/ofInstant (java.time.Instant/ofEpochMilli epoch-ms)
                                                       java.time.ZoneOffset/UTC)))])

(def chip-attrs {:style "padding:3px; border-radius: 6px; border: 0.5px dashed rgba(0,0,0,0.4)"})

(defn state [state]
  (case state
    "ERROR" [:span.chip.state-ERROR chip-attrs "Error"]
    "STARTED" [:span.chip.state-STARTED chip-attrs "Started"]
    "DONE" [:span.chip.state-DONE chip-attrs "Done"]
    "REUSED" [:span.chip.state-REUSED chip-attrs "Reused"]
    [:span (str state)]))

(def page-style
  "body{font-family:sans-serif;margin:2em}
   table{border-collapse:collapse;width:100%}
   td,th{padding:6px 10px;border:1px solid #ccc;text-align:left}
   tr.state-DONE{background:#d4f7d4}
   tr.state-DONE span.chip {background:#ccf6cc; border-radius:4px}
   tr.state-STARTED{background:#fdf6c9}
   tr.state-ERROR{background:#f7d4d4}
   tr.state-REUSED{background:#dde8f7}
   tr:hover{filter:brightness(0.95)}
   tr.tenon-hl{outline:2px solid #333}
   a{color:inherit;text-decoration:none}
   td a{display:block}
   pre{background:#f5f5f5;padding:1em;overflow-x:auto;white-space:pre-wrap}")

(def page-sizes [100 500 1000 5000])
(def default-page-size 100)

(defn select-filter [param-name label options selected]
  [:span {:style "margin-right: 1em"}
   [:label {:for param-name} label ": "]
   [:select {:name param-name :id param-name :onchange "this.form.submit()"}
    [:option (cond-> {:value ""} (nil? selected) (assoc :selected true)) "All"]
    (for [o options]
      [:option (cond-> {:value o} (= o selected) (assoc :selected true)) o])]])

(defn parse-pagination [query-params cursor-keys]
  (let [page-size (let [n (try (Long/parseLong (get query-params "page_size" ""))
                               (catch NumberFormatException _ nil))]
                    (if (contains? (set page-sizes) n) n default-page-size))
        cursor-vals (mapv (comp query-params name) cursor-keys)]
    {:page-size   page-size
     :page_cursor (when (every? not-empty cursor-vals) cursor-vals)}))