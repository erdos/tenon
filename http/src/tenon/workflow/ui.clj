(ns tenon.workflow.ui
  "HTML UI components"
  (:require [clojure.edn :as edn]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [hiccup2.core :as hiccup]))

(def ^:dynamic *uri-prefix*
  "The URI prefix of tenon routes - bound per request by
   tenon.workflow.http/handler, prepended to every link uri builds."
  "")

(defn uri
  "Builds a prefix-relative URI from path segments."
  [& segments]
  (str *uri-prefix* "/" (str/join "/" (map #(if (keyword? %) (name %) (str %)) segments))))

(defn pluralize
  "n and word, plural for any n but 1: 1 time, 2 times."
  [n word]
  (str n " " word (when (not= 1 n) "s")))

(defn format-duration
  "Renders a millisecond duration compactly, e.g. 153ms, 1.24s, 2m 03s, 1h 02m."
  [ms]
  (cond
    (< ms 1000) (str ms "ms")
    (< ms 60000) (format "%.2fs" (/ ms 1000.0))
    (< ms 3600000) (format "%dm %02ds" (quot ms 60000) (quot (rem ms 60000) 1000))
    :else (format "%dh %02dm" (quot ms 3600000) (quot (rem ms 3600000) 60000))))

(defn format-offset
  "Renders milliseconds since some start as a clock-style offset:
   +MM:SS.mmm, or +HH:MM:SS.mmm from an hour on - e.g. +00:00.153,
   +02:03.120, +02:38:25.665."
  [ms]
  (let [mm-ss (format "%02d:%02d.%03d" (quot (rem ms 3600000) 60000) (quot (rem ms 60000) 1000) (rem ms 1000))]
    (if (< ms 3600000)
      (str "+" mm-ss)
      (format "+%02d:%s" (quot ms 3600000) mm-ss))))

(defn pretty
  "value pretty-printed as EDN."
  [value]
  (binding [pprint/*print-right-margin* 100]
    (str/trimr (with-out-str (pprint/pprint value)))))

(defn workflow-id [id]
  [:code (str id)])

(defn workflow-link
  "A link to workflow id's page: a link icon in front of the id."
  ([id] (workflow-link (str "/workflows/" id) id))
  ([href id]
   [:a {:href href :style "color:#0645ad; text-decoration:underline"} "\u29C9 " (workflow-id id)]))

(defn workflow-name [name]
  [:code (str name)])

(defn timestamp-str
  "Formats an epoch-milliseconds timestamp as UTC, e.g. 2026-01-02 03:04:05.678."
  [epoch-ms]
  (when epoch-ms
    (.format (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSS")
             (java.time.LocalDateTime/ofInstant (java.time.Instant/ofEpochMilli epoch-ms)
                                                java.time.ZoneOffset/UTC))))

(defn timestamp
  "Renders an epoch-milliseconds timestamp as UTC, e.g. 2026-01-02 03:04:05.678."
  [epoch-ms]
  [:code (timestamp-str epoch-ms)])

(defn relative-time
  "How long before (or after) now epoch-ms is, roughly: just now,
   42s ago, 3 min ago, 5 h ago, 2 days ago - or in 3 min."
  [epoch-ms now]
  (let [diff (- now epoch-ms)
        secs (quot (abs diff) 1000)
        amount (cond
                 (< secs 60) (str secs "s")
                 (< secs 3600) (str (quot secs 60) " min")
                 (< secs 86400) (str (quot secs 3600) " h")
                 :else (pluralize (quot secs 86400) "day"))]
    (cond
      (< (abs diff) 5000) "just now"
      (pos? diff) (str amount " ago")
      :else (str "in " amount))))

(declare muted)

(defn timestamp-with-relative
  "An epoch-milliseconds timestamp in UTC, labelled as such, followed by
   how long ago it was relative to now."
  [epoch-ms now]
  (when epoch-ms
    [:span [:code (timestamp-str epoch-ms) " UTC"] " " (muted "(" (relative-time epoch-ms now) ")")]))

(defn state-symbol
  "A one-character symbol for a state, e.g. for a browser tab's title."
  [state]
  (case state
    "DONE" "\u2713"
    "ERROR" "\u2717"
    "STARTED" "\u25B6"
    "REUSED" "\u21BA"
    "?"))

(defn state [state]
  (case state
    "ERROR" [:span.chip.state-ERROR "Error"]
    "STARTED" [:span.chip.state-STARTED "Started"]
    "DONE" [:span.chip.state-DONE "Done"]
    "REUSED" [:span.chip.state-REUSED "Reused"]
    [:span.chip (str state)]))

(defn state-with-tooltip
  "A state's chip, with title as its tooltip if not nil."
  [state-name title]
  [:span (cond-> {:class "tree-state-chip"} title (assoc :title title)) (state state-name)])

(defn muted
  "Secondary text: small and grey."
  [& content]
  (into [:span.panel-muted] content))

(defn breadcrumb
  "A trail of links, e.g. to a page's ancestors: items are [label href]
   pairs, the last one - the current page - may have a nil href."
  [items]
  [:nav.breadcrumb
   (interpose [:span.breadcrumb-sep "\u203A"]
              (for [[label href] items]
                (if href
                  [:a {:href href} label]
                  [:span.breadcrumb-current label])))])

(defn folded-pre
  "text in a <pre> - or, when it has more than max-lines lines or
   max-chars characters, just the start of it, with the full text a click
   away (a <details>, no script needed)."
  ([text] (folded-pre text {}))
  ([text {:keys [max-lines max-chars] :or {max-lines 15 max-chars 3000}}]
   (let [lines (str/split-lines text)]
     (if (and (<= (count lines) max-lines) (<= (count text) max-chars))
       [:pre text]
       (let [preview (str/join "\n" (take max-lines lines))
             preview (if (> (count preview) max-chars) (subs preview 0 max-chars) preview)]
         [:details.fold
          [:summary
           [:pre.fold-preview preview "\n\u2026"]
           [:span.fold-more (str "Show all " (pluralize (count lines) "line") " (" (count text) " chars)")]
           [:span.fold-less "Show less"]]
          [:pre text]])))))

(defn edn-block
  "An EDN string pretty-printed (folded if large, see folded-pre), or shown
   raw if it can't be read."
  [edn-str]
  (let [value (try (edn/read-string edn-str) (catch Throwable _ ::unreadable))]
    (if (= ::unreadable value)
      [:div (muted "unreadable EDN, shown raw:") (folded-pre (str edn-str))]
      (folded-pre (pretty value)))))

(defn panel-toggle-btn
  "A button that opens panel-component in a popover below it, and closes
   it on a second click - a <details>, no script needed. btn-component is
   a [:button ...] hiccup vector; since a <button> inside the <summary>
   would swallow the click, its classes, attributes and content go on the
   <summary> instead, styled as a button."
  [btn-component panel-component]
  (let [[tag & content] btn-component
        [attrs content] (if (map? (first content))
                          [(first content) (rest content)]
                          [{} content])
        tag-classes (rest (str/split (name tag) #"\."))
        class (str/join " " (concat ["btn"] tag-classes (some-> (:class attrs) vector)))]
    [:details.panel-toggle
     (into [:summary (-> attrs (dissoc :type) (assoc :class class))] content)
     [:div.panel-toggle-panel panel-component]]))

(defn panel
  "A card with a header and a body. title is its h2; subtitle, if any,
   is muted text next to it."
  [{:keys [title subtitle]} & body]
  [:section.panel
   [:div.panel-head [:h2 title] (when subtitle (muted subtitle))]
   (into [:div.panel-body] body)])

(defn key-values
  "A two-column list of [key value] pairs, keys on the left."
  [entries]
  [:dl.kv
   (for [[k v] entries]
     [:div [:dt k] [:dd v]])])

(defn error-details
  "An exception: its class, its message prominently, then any extra content."
  [class message & extra]
  (into [:div
         [:div.error-class [:code (str class)]]
         [:p.error-message message]]
        extra))

(defn fact
  "A labelled value in a summary-card's grid."
  [label & value]
  [:div [:dt label] (into [:dd] value)])

(defn summary-card
  "The card at the top of a page: a header row - the state's chip, then
   the head elements (a button among them goes to the right) - an optional
   highlighted note and error (each a seq of content), then a grid of facts (see fact).
   The state colours its left edge."
  [{state-name :state :keys [head note error]} & facts]
  [:section {:class (str "wf-summary state-" state-name)}
   (into [:div.wf-summary-head (state state-name)] head)
   (when error (into [:p.wf-summary-note.wf-summary-error] error))
   (when note (into [:p.wf-summary-note] note))
   (into [:dl.wf-facts] facts)])

(def page-style
  "body{font-family:sans-serif;margin:2em}
   table{border-collapse:collapse;width:100%}
   td,th{padding:6px 10px;border:1px solid #ccc;text-align:left}
   tr.state-DONE{background:#d4f7d4}
   tr.state-STARTED{background:#fdf6c9}
   tr.state-ERROR{background:#f7d4d4}
   tr.state-REUSED{background:#dde8f7}
   span.chip{display:inline-block;padding:0 .7em;border-radius:999px;font-size:.8em;font-weight:600;
             font-variant-caps:all-small-caps;letter-spacing:.06em;line-height:1.7;white-space:nowrap;
             background:#ced4da;color:#1a1a1a}
   span.chip.state-DONE{background:#69db7c}
   span.chip.state-ERROR{background:#ff8787}
   span.chip.state-STARTED{background:#fcc419}
   span.chip.state-REUSED{background:#91a7ff}
   tr:hover{filter:brightness(0.95)}
   a{color:inherit;text-decoration:none}
   td a{display:block}
   header.page-head{display:flex;align-items:center;gap:1em;flex-wrap:wrap}
   header.page-head h1{min-width:0;overflow-wrap:anywhere}
   a.back{margin-left:auto;white-space:nowrap;padding:4px 12px;border:1px solid rgba(0,0,0,.25);
          border-radius:6px;background:#fff;font-size:.9em}
   a.back:hover{background:#f0f0f0}
   pre{background:#f5f5f5;padding:1em;overflow-x:auto;white-space:pre-wrap}
   section.wf-summary{border:1px solid #ddd;border-left:6px solid #ced4da;border-radius:8px;
                      padding:12px 16px;margin-bottom:1.5em;background:#fafafa}
   section.wf-summary.state-DONE{border-left-color:#69db7c}
   section.wf-summary.state-ERROR{border-left-color:#ff8787}
   section.wf-summary.state-STARTED{border-left-color:#fcc419}
   .wf-summary-head{display:flex;align-items:center;gap:.75em;margin-bottom:10px}
   .wf-summary-head span.chip{font-size:.95em}
   .wf-summary-duration{color:#555}
   .btn,a.back{-webkit-user-select:none;user-select:none}
   .btn{display:inline-block;padding:4px 12px;border:1px solid rgba(0,0,0,.25);border-radius:6px;
        background:#fff;color:inherit;cursor:pointer;font:inherit;font-size:.9em;list-style:none}
   .btn:hover{background:#f0f0f0}
   .btn.danger{background:#ffe3e3;border-color:#ff8787}
   .btn.danger:hover{background:#ffc9c9}
   details.panel-toggle{position:relative}
   details.panel-toggle>summary::-webkit-details-marker{display:none}
   details.panel-toggle[open]>summary{background:#eee}
   .panel-toggle-panel{position:absolute;right:0;top:calc(100% + 6px);z-index:1;width:max-content;max-width:22em;
                       padding:10px 12px;background:#fff;border:1px solid #ccc;border-radius:8px;
                       box-shadow:0 4px 12px rgba(0,0,0,.12)}
   .wf-summary-head details.panel-toggle{margin-left:auto}
   form.restart{display:flex;flex-direction:column;gap:8px;margin:0}
   .wf-summary-error{background:#ffe3e3;color:#a51d1d}
   nav.breadcrumb{margin:-.5em 0 1em;font-size:.9em;color:#555;display:flex;flex-wrap:wrap;gap:.4em;align-items:center}
   nav.breadcrumb a{color:#0645ad;text-decoration:underline}
   nav.breadcrumb .breadcrumb-sep{color:#999}
   nav.breadcrumb .breadcrumb-current{font-weight:600}
   details.fold>summary{list-style:none;cursor:pointer}
   details.fold>summary::-webkit-details-marker{display:none}
   details.fold .fold-preview{margin:0;-webkit-mask-image:linear-gradient(#000 70%,transparent);mask-image:linear-gradient(#000 70%,transparent)}
   details.fold .fold-more,details.fold .fold-less{display:inline-block;margin-top:4px;font-size:.85em;color:#0645ad;text-decoration:underline}
   details.fold .fold-less,details.fold[open] .fold-more,details.fold[open] .fold-preview{display:none}
   details.fold[open] .fold-less{display:inline-block;margin:0 0 4px}
   .wf-summary-note{margin:0 0 10px;padding:6px 10px;background:#fff3bf;border-radius:4px;font-size:.9em}
   dl.wf-facts{display:grid;grid-template-columns:repeat(auto-fill,minmax(13em,1fr));gap:10px 24px;margin:0}
   dl.wf-facts dt{font-size:.75em;text-transform:uppercase;letter-spacing:.06em;color:#777}
   dl.wf-facts dd{margin:2px 0 0}
   dl.wf-facts a{display:inline}
   .wf-expired{color:#c92a2a;font-weight:600}
   div.panels{display:grid;grid-template-columns:repeat(auto-fit,minmax(22em,1fr));gap:16px;align-items:start;margin-bottom:1.5em}
   section.panel{border:1px solid #ddd;border-radius:8px;background:#fff;overflow:hidden;min-width:0}
   .panel-head{display:flex;align-items:baseline;gap:.75em;padding:8px 14px;background:#fafafa;border-bottom:1px solid #eee}
   .panel-head h2{margin:0;font-size:1em}
   .panel-body{padding:10px 14px}
   .panel-body pre{margin:0;padding:8px 10px;font-size:.9em}
   .panel-muted{color:#777;font-size:.85em}
   dl.kv{display:grid;grid-template-columns:max-content minmax(0,1fr);gap:8px 12px;margin:0;align-items:baseline}
   dl.kv>div{display:contents}
   dl.kv dt{color:#555}
   dl.kv dd{margin:0;min-width:0}
   .error-class{color:#555;font-size:.9em}
   .error-message{margin:6px 0 10px;font-size:1.1em;font-weight:600;color:#a51d1d;white-space:pre-wrap}
   ul.tree,ul.tree ul{list-style:none;margin:0;padding:0}
   ul.tree ul{margin-left:.6em;padding-left:1.2em;border-left:1px dashed #aaa}
   ul.tree li{margin:4px 0}
   ul.tree summary{cursor:pointer;padding:4px 8px;border-radius:4px;display:flex;align-items:center;gap:.5em;list-style:none}
   ul.tree summary::-webkit-details-marker{display:none}
   ul.tree summary::before{content:'\\25B8';display:inline-block;width:1em;text-align:center;transition:transform .15s}
   ul.tree details[open]>summary::before{transform:rotate(90deg)}
   ul.tree summary a{display:inline}
   ul.tree details:not([open])>summary{box-shadow:inset 0 0 0 1px rgba(0,0,0,.25)}
   ul.tree li.tree-event{padding:2px 8px;border-radius:4px;display:flex;align-items:center;gap:.5em;min-height:1.6em}
   ul.tree li.tree-leaf{padding:4px 8px;border-radius:4px;display:flex;align-items:center;gap:.5em}
   ul.tree li.tree-leaf::before{content:'\\2022';width:1em;text-align:center;color:#888}
   ul.tree li.tree-leaf a{display:inline}
   ul.tree .tree-cols{margin-left:auto;display:flex;align-items:center;gap:.8em;flex:none}
   ul.tree .tree-cols>span{flex:none;white-space:nowrap}
   ul.tree .tree-time{width:8.5em;text-align:right;color:#555;font-size:.85em;font-variant-numeric:tabular-nums}
   ul.tree .tree-offset{min-width:6.5em;color:#555;font:inherit;font-size:.85em;font-variant-numeric:tabular-nums}
   ul.tree .tree-verb{color:#333;font-size:.9em}
   ul.tree .tree-count{width:3em;text-align:right;color:#333;font-size:.85em}
   ul.tree .tree-state{width:5.5em;text-align:right}
   ul.tree .tree-state span.chip{background:none;padding:0}
   ul.tree .tree-reuse{color:#555;font-size:.85em}
   ul.tree .tree-reuse a{display:inline}
   ul.tree [title]{cursor:help}
   ul.tree summary.state-DONE,ul.tree li.state-DONE{background:#d4f7d4}
   ul.tree summary.state-STARTED,ul.tree li.state-STARTED{background:#fdf6c9}
   ul.tree summary.state-ERROR,ul.tree li.state-ERROR{background:#f7d4d4}
   ul.tree summary.state-REUSED,ul.tree li.state-REUSED{background:#dde8f7}")

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
  (let [page-size (let [n (parse-long (get query-params "page_size" ""))]
                    (if (contains? (set page-sizes) n) n default-page-size))
        cursor-vals (mapv (comp query-params name) cursor-keys)]
    {:page-size   page-size
     :page_cursor (when (every? not-empty cursor-vals) cursor-vals)}))

(defn layout
  "A whole HTML page, as a string, titled title. Options: back, an
   [href label] link shown at the top right to the page this one belongs
   under, and tab-title, the browser tab's title if not title."
  ([title body] (layout title {} body))
  ([title {:keys [back tab-title]} body]
   (->> [:html
         [:head [:title (or tab-title title)] [:style (hiccup/raw page-style)]]
         [:body
          [:header.page-head
           [:h1 title]
           (when-let [[href label] back]
             [:a.back {:href href} "\u2190 " label])]
          body]]
        (hiccup/html {:mode :html})
        (str))))
