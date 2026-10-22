(ns tenon.workflow.ui
  "HTML UI components"
  (:require [clojure.edn :as edn]
            [clojure.main :as main]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [hiccup2.core :as hiccup]
            [ring.util.codec :as codec]))

(def ^:dynamic *uri-prefix*
  "The URI prefix of tenon routes - bound per request by
   tenon.workflow.http/handler, prepended to every link uri builds."
  "")

(defn uri
  "Builds a prefix-relative URI from path segments. A map as the last
   argument is the query string's params - ones with a nil value left
   out, and no ? at all if none is left: (uri :workflows 1 {:a 2 :b nil})
   is \"/workflows/1?a=2\"."
  [& segments]
  (let [[segments params] (if (map? (last segments))
                            [(butlast segments) (last segments)]
                            [segments nil])
        qs (codec/form-encode (into {} (for [[k v] params :when (some? v)] [(name k) v])))]
    (cond-> (str *uri-prefix* "/" (str/join "/" (map #(if (keyword? %) (name %) (str %)) segments)))
      (seq qs) (str "?" qs))))

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

(defn read-edn
  "The value EDN string s holds, or ::unreadable if it can't be read."
  [s]
  (try (edn/read-string s) (catch Throwable _ ::unreadable)))

(defn pretty
  "value pretty-printed as EDN."
  [value]
  (binding [pprint/*print-right-margin* 100]
    (str/trimr (with-out-str (pprint/pprint value)))))

(defn workflow-id [id]
  [:code (str id)])

(defn workflow-link
  "A link to workflow id's page: a link icon in front of the id."
  [id]
  [:a.wf-link {:href (uri :workflows id)} "\u29C9 " (workflow-id id)])

(defn workflow-name
  "A workflow's qualified fn name, its namespace muted so the fn name
   stands out."
  [wf-def]
  (let [wf-def (str wf-def)]
    (if-let [i (str/last-index-of wf-def "/")]
      [:code [:span.wf-ns (subs wf-def 0 (inc i))] [:b (subs wf-def (inc i))]]
      [:code wf-def])))

(def ^:private timestamp-format
  (.withZone (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSS")
             java.time.ZoneOffset/UTC))

(defn timestamp-str
  "Formats an epoch-milliseconds timestamp as UTC, e.g. 2026-01-02 03:04:05.678."
  [epoch-ms]
  (when epoch-ms
    (.format timestamp-format (java.time.Instant/ofEpochMilli epoch-ms))))

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

(defn muted
  "Secondary text: small and grey."
  [& content]
  (into [:span.panel-muted] content))

(defn timestamp-with-relative
  "An epoch-milliseconds timestamp in UTC, labelled as such, followed by
   how long ago it was relative to now."
  [epoch-ms now]
  (when epoch-ms
    [:span [:code (timestamp-str epoch-ms) " UTC"] " " (muted "(" (relative-time epoch-ms now) ")")]))

(def state-info
  {"DONE"    {:symbol "\u2713" :verb "Completed"}
   "ERROR"   {:symbol "\u2717" :verb "Failed"}
   "STARTED" {:symbol "\u25B6" :verb "Called"}
   "REUSED"  {:symbol "\u21BA" :verb "Called"}})

(defn effective-state [{:workflow/keys [reused state]}]
  (if reused "REUSED" state))

(defn state-symbol
  "A one-character symbol for a state, e.g. for a browser tab's title."
  [state]
  (get-in state-info [state :symbol] "?"))

(defn state
  "A state's chip: its name, coloured by the state."
  [state]
  [:span {:class (str "chip state-" state)} (str/capitalize (str state))])

(defn state-with-tooltip
  "A state's chip, with title as its tooltip if not nil."
  [state-name title]
  [:span (cond-> {:class "tree-state-chip"} title (assoc :title title)) (state state-name)])

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
  (let [value (read-edn edn-str)]
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

(defn frame-str
  "A stack frame of a Throwable->map, [class method file line], as a line."
  [[cls method file line]]
  (str cls "." method "(" file (when line (str ":" line)) ")"))

(defn- stack-frame
  "A stack frame of a Throwable->map as a row: the called function (a
   Clojure one demunged, its namespace dimmed) and its file:line. Frames of
   the JVM and Clojure itself are dimmed as a whole."
  [[cls method file line]]
  (let [cls (str cls)
        clj? (str/includes? cls "$")
        fn-name (if clj? (main/demunge cls) (str cls "." method))
        [_ prefix base] (re-matches #"(.*[/.])([^/.]*)" fn-name)
        lib? (re-find #"^(java|javax|jdk|sun|clojure|nrepl)\." cls)]
    [:li {:class (when lib? "frame-lib")}
     [:span.frame-fn (when prefix [:span.frame-ns prefix]) (or base fn-name)]
     [:span.frame-loc file (when (and line (pos? line)) (str ":" line))]]))

(defn stack-trace
  "The stack trace of a Throwable->map (a seq of frames) as a list."
  [trace]
  [:ol.stack-trace (map stack-frame trace)])

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
  "The stylesheet every page inlines."
  (slurp (io/resource "tenon/workflow/ui.css")))

(def page-sizes [100 500 1000 5000])
(def default-page-size 100)

(defn select-filter [param-name label options selected]
  [:label label ": "
   [:select {:name param-name :onchange "this.form.submit()"}
    [:option (cond-> {:value ""} (nil? selected) (assoc :selected true)) "All"]
    (for [o options]
      [:option (cond-> {:value o} (= o selected) (assoc :selected true)) o])]])

(defn parse-pagination [query-params cursor-keys]
  (let [page-size (let [n (parse-long (get query-params "page_size" ""))]
                    (if (contains? (set page-sizes) n) n default-page-size))
        cursor-vals (mapv (comp query-params name) cursor-keys)]
    {:page-size   page-size
     :page_cursor (when (every? not-empty cursor-vals) cursor-vals)}))

(def ^:private saved-theme-js
  "try{document.documentElement.dataset.theme=localStorage.theme||''}catch(e){}")

(def ^:private theme-toggle-js
  "const r=document.documentElement,t={'':'light',light:'dark',dark:''}[r.dataset.theme||''];r.dataset.theme=t;try{localStorage.theme=t}catch(e){}")

(defn layout
  "A whole HTML page, as a string, titled title. Options: back, an
   [href label] link shown at the top right to the page this one belongs
   under, tab-title, the browser tab's title if not title, and refresh,
   seconds after which the browser reloads the page."
  ([title body] (layout title {} body))
  ([title {:keys [back tab-title refresh]} body]
   (->> [:html
         [:head [:title (or tab-title title)]
          (when refresh [:meta {:http-equiv "refresh" :content (str refresh)}])
          [:script (hiccup/raw saved-theme-js)]
          [:style (hiccup/raw page-style)]]
         [:body
          [:header.page-head
           [:h1 title]
           [:div.head-actions
            (when-let [[href label] back]
              [:a.btn.back {:href href} "\u2190 " label])
            [:button.btn.theme-toggle {:type "button" :title "Theme: auto, light or dark" :onclick theme-toggle-js}]]]
          body]]
        (hiccup/html {:mode :html})
        (str))))
