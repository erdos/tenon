(ns tenon.workflow.ui-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [matcher-combinators.test :refer [match?]]
            [kerodon.core :refer [within]]
            [kerodon.test :refer [has text? attr? missing? heading?]]
            [tenon.workflow.kerodon-util :as ku :refer [render count? present? texts? attrs?]]
            [tenon.workflow.ui :as ui]))

(use-fixtures :each ku/html5-fixture)

(deftest uri-test
  (is (= "/" (ui/uri)))
  (is (= "/workflows/12/restart" (ui/uri :workflows 12 :restart)))
  (is (= "/workflows/12?a=1&b=x+y%26z" (ui/uri :workflows 12 {:a 1 :b "x y&z"})))
  (is (= "/?state=DONE" (ui/uri {:state "DONE" :wf_def nil})) "nil params are left out")
  (is (= "/workflows/12" (ui/uri :workflows 12 {:restart_error nil})) "no ? without params")
  (binding [ui/*uri-prefix* "/tenon"]
    (is (= "/tenon/workflows/1?a=1" (ui/uri :workflows 1 {:a 1})))))

(deftest parse-pagination-defaults-page-size-when-absent-test
  (is (match? {:page-size 100 :page_cursor nil?}
              (ui/parse-pagination {} [:before_ts :before_id]))))

(deftest parse-pagination-defaults-page-size-when-not-a-listed-size-test
  (is (match? {:page-size 100} (ui/parse-pagination {"page_size" "3"} [:before_ts :before_id])))
  (is (match? {:page-size 100} (ui/parse-pagination {"page_size" "not-a-number"} [:before_ts :before_id]))))

(deftest parse-pagination-accepts-a-listed-page-size-test
  (is (match? {:page-size 500} (ui/parse-pagination {"page_size" "500"} [:before_ts :before_id]))))

(deftest parse-pagination-builds-cursor-from-named-keys-in-order-test
  (is (match? {:page_cursor ["2026-01-01" "abc"]}
              (ui/parse-pagination {"before_ts" "2026-01-01" "before_id" "abc"}
                                   [:before_ts :before_id]))))

(deftest parse-pagination-treats-a-partial-cursor-as-no-cursor-test
  (is (match? {:page_cursor nil?} (ui/parse-pagination {"before_ts" "2026-01-01"} [:before_ts :before_id])))
  (is (match? {:page_cursor nil?} (ui/parse-pagination {"before_id" "abc"} [:before_ts :before_id]))))

(deftest parse-pagination-works-with-different-cursor-key-names-test
  (is (match? {:page_cursor ["x" "y"]}
              (ui/parse-pagination {"after_a" "x" "after_b" "y"} [:after_a :after_b]))))

(deftest workflow-link-test
  (-> (render (ui/workflow-link 12))
      (has (attr? [:a.wf-link] :href "/workflows/12"))
      (has (texts? [:a.wf-link] ["⧉ 12"]))
      (has (texts? [:a.wf-link :code] ["12"]) "the id is shown as code"))
  (binding [ui/*uri-prefix* "/tenon"]
    (-> (render (ui/workflow-link 12))
        (has (attr? [:a.wf-link] :href "/tenon/workflows/12") "the link honours the mount prefix"))))

(deftest workflow-name-test
  (-> (render (ui/workflow-name "a.b/run"))
      (has (texts? [:code] ["a.b/run"]))
      (has (texts? [:code :span.wf-ns] ["a.b/"]) "the namespace is muted")
      (has (texts? [:code :b] ["run"]) "the fn name stands out"))
  (-> (render (ui/workflow-name "plain"))
      (has (texts? [:code] ["plain"]))
      (has (missing? [:code :span]))
      (has (missing? [:code :b]))))

(deftest state-test
  (-> (render (ui/state "STARTED"))
      (has (attr? [:span.chip] :class "chip state-STARTED"))
      (has (texts? [:span.chip] ["Started"])))
  (-> (render (ui/state nil))
      (has (attr? [:span.chip] :class "chip state-"))
      (has (texts? [:span.chip] [""]))))

(deftest read-stored-test
  (is (= [1 2] (ui/read-stored "[1 2]")))
  (is (= ::ui/unreadable (ui/read-stored "(1 2"))))

(deftest panel-test
  (-> (render (ui/panel {:title "Result" :subtitle "2 items"} [:p "body"]))
      (has (present? (ku/panel "Result")))
      (within [:section.panel :div.panel-head]
        (has (texts? [:h2] ["Result"]))
        (has (texts? [:span.panel-muted] ["2 items"])))
      (has (texts? [:section.panel :div.panel-body :p] ["body"])))
  (-> (render (ui/panel {:title "Params"}))
      (has (heading? "Params"))
      (has (missing? [:span.panel-muted]) "no subtitle")
      (has (texts? [:section.panel :div.panel-body] [""]) "no body")))

(deftest key-values-test
  (-> (render (ui/key-values [["a" "1"] ["b" "2"]]))
      (has (texts? [:dl.kv :dt] ["a" "b"]))
      (has (texts? (ku/dd-of "a") ["1"]))
      (has (texts? (ku/dd-of "b") ["2"]))))

(deftest error-details-test
  (-> (render (ui/error-details {:class "java.lang.Exception" :message "boom"} [:pre "extra"]))
      (has (texts? [:div.error-class :code] ["java.lang.Exception"]))
      (has (texts? [:p.error-message] ["boom"]))
      (has (texts? [:pre] ["extra"]))))

(deftest summary-card-test
  (-> (render (ui/summary-card {:state "DONE" :head [[:span "1s"]] :note ["a " [:b "note"]]}
                               (ui/fact "Id" "7")))
      (has (attr? [:section.wf-summary] :class "wf-summary state-DONE"))
      (has (texts? [:div.wf-summary-head :> :span] ["Done" "1s"]) "the state's chip, then the head")
      (has (attr? [:div.wf-summary-head :span.chip] :class "chip state-DONE"))
      (has (texts? [:p.wf-summary-note] ["a note"]))
      (has (texts? [:p.wf-summary-note :b] ["note"]))
      (has (texts? (ku/dd-of "Id") ["7"])))
  (-> (render (ui/summary-card {:state "DONE"}))
      (has (missing? [:p.wf-summary-note]) "no note")))

(deftest relative-time-test
  (is (= "just now" (ui/relative-time 1000 3000)))
  (is (= "42s ago" (ui/relative-time 0 42000)))
  (is (= "3 min ago" (ui/relative-time 0 (* 3 60 1000))))
  (is (= "5 h ago" (ui/relative-time 0 (* 5 3600 1000))))
  (is (= "1 day ago" (ui/relative-time 0 (* 86400 1000))))
  (is (= "2 days ago" (ui/relative-time 0 (* 2 86400 1000))))
  (is (= "in 3 min" (ui/relative-time (* 3 60 1000) 0))))

(deftest timestamp-with-relative-test
  (-> (render (ui/timestamp-with-relative 0 42000))
      (has (texts? [:code] ["1970-01-01 00:00:00.000 UTC"]))
      (has (texts? [:span.panel-muted] ["(42s ago)"]))
      (within [:body]
        (has (text? "1970-01-01 00:00:00.000 UTC (42s ago)"))))
  (is (nil? (ui/timestamp-with-relative nil 0))))

(deftest breadcrumb-test
  (-> (render (ui/breadcrumb [["A" "/a"] ["B" nil]]))
      (within [:nav.breadcrumb]
        (has (text? "A›B")))
      (has (attrs? [:nav.breadcrumb :a] :href ["/a"]))
      (has (texts? [:nav.breadcrumb :a] ["A"]))
      (has (texts? [:nav.breadcrumb :span.breadcrumb-sep] ["›"]))
      (has (texts? [:nav.breadcrumb :span.breadcrumb-current] ["B"]) "the current page is no link")))

(deftest folded-pre-test
  (-> (render (ui/folded-pre "short"))
      (has (texts? [:pre] ["short"]))
      (has (missing? [:details])))
  (-> (render (ui/folded-pre "1\n2\n3\n4" {:max-lines 2}))
      (has (texts? [:details.fold :summary :pre.fold-preview] ["1 2 …"]) "the first max-lines lines")
      (has (texts? [:details.fold :summary :span.fold-more] ["Show all 4 lines (7 chars)"]))
      (has (texts? [:details.fold :> :pre] ["1 2 3 4"]) "the full text, a click away"))
  (-> (render (ui/folded-pre (apply str (repeat 50 "x")) {:max-chars 10}))
      (has (texts? [:pre.fold-preview] ["xxxxxxxxxx …"]) "a single very long line is cut too")))

(deftest pluralize-test
  (is (= "0 times" (ui/pluralize 0 "time")))
  (is (= "1 time" (ui/pluralize 1 "time")))
  (is (= "2 times" (ui/pluralize 2 "time"))))

(deftest format-duration-test
  (is (= "153ms" (ui/format-duration 153)))
  (is (= "1.24s" (ui/format-duration 1240)))
  (is (= "2m 03s" (ui/format-duration 123000)))
  (is (= "1h 02m" (ui/format-duration 3720000)))
  (is (= "153ms" (ui/format-duration 1000 1153)) "two times: from, to"))

(deftest format-offset-test
  (is (= "+00:00.000" (ui/format-offset 0)))
  (is (= "+00:00.153" (ui/format-offset 153)))
  (is (= "+02:03.120" (ui/format-offset 123120)))
  (is (= "+59:59.999" (ui/format-offset 3599999)))
  (is (= "+01:02:03.000" (ui/format-offset 3723000)))
  (is (= "+02:38:25.665" (ui/format-offset 9505665)))
  (is (= "+100:00:00.000" (ui/format-offset 360000000))))

(deftest pretty-test
  (is (= "{:a 1}" (ui/pretty {:a 1})))
  (is (= "nil" (ui/pretty nil))))

(deftest stored-block-test
  (-> (render (ui/stored-block "[1 2]"))
      (has (texts? [:pre] ["[1 2]"])))
  (-> (render (ui/stored-block "nil"))
      (has (texts? [:pre] ["nil"])))
  (-> (render (ui/stored-block "#object[Foo]"))
      (has (texts? [:div :span.panel-muted] ["unreadable, shown raw:"]))
      (has (texts? [:div :pre] ["#object[Foo]"]))))

(deftest state-with-tooltip-test
  (-> (render (ui/state-with-tooltip "ERROR" "boom"))
      (has (attr? [:span.tree-state-chip] :title "boom"))
      (has (attr? [:span.tree-state-chip :span.chip] :class "chip state-ERROR"))
      (has (texts? [:span.tree-state-chip] ["Error"])))
  (-> (render (ui/state-with-tooltip "DONE" nil))
      (has (attr? [:span.tree-state-chip] :title nil) "no tooltip")
      (has (texts? [:span.tree-state-chip :span.chip.state-DONE] ["Done"]))))

(deftest panel-toggle-btn-test
  (-> (render (ui/panel-toggle-btn [:button "Open"] [:p "panel"]))
      (has (attr? [:details.panel-toggle :> :summary] :class "btn"))
      (has (texts? [:details.panel-toggle :> :summary] ["Open"]))
      (has (texts? [:details.panel-toggle :> :div.panel-toggle-panel :p] ["panel"]))
      (has (missing? [:button])))
  (-> (render (ui/panel-toggle-btn [:button.primary {:class "extra" :title "t" :type "button"} "Go " [:b "!"]]
                                   [:p "panel"]))
      (has (attr? [:summary] :class "btn primary extra") "the button's classes move onto the summary")
      (has (attr? [:summary] :title "t") "so do its attributes")
      (has (attr? [:summary] :type nil) "but its type")
      (has (texts? [:summary] ["Go !"]) "and its content")
      (has (texts? [:summary :b] ["!"]))
      (has (count? [:button] 0))))
