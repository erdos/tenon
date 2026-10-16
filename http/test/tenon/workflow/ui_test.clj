(ns tenon.workflow.ui-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.string :as str]
            [hiccup2.core :as hiccup]
            [tenon.workflow.ui :as ui]))

(deftest uri-test
  (is (= "/" (ui/uri)))
  (is (= "/workflows/12/restart" (ui/uri :workflows 12 :restart)))
  (is (= "/workflows/12?a=1&b=x+y%26z" (ui/uri :workflows 12 {:a 1 :b "x y&z"})))
  (is (= "/?state=DONE" (ui/uri {:state "DONE" :wf_def nil})) "nil params are left out")
  (is (= "/workflows/12" (ui/uri :workflows 12 {:restart_error nil})) "no ? without params")
  (binding [ui/*uri-prefix* "/tenon"]
    (is (= "/tenon/workflows/1?a=1" (ui/uri :workflows 1 {:a 1})))))

(deftest parse-pagination-defaults-page-size-when-absent-test
  (is (= {:page-size 100 :page_cursor nil}
         (ui/parse-pagination {} [:before_ts :before_id]))))

(deftest parse-pagination-defaults-page-size-when-not-a-listed-size-test
  (is (= 100 (:page-size (ui/parse-pagination {"page_size" "3"} [:before_ts :before_id]))))
  (is (= 100 (:page-size (ui/parse-pagination {"page_size" "not-a-number"} [:before_ts :before_id])))))

(deftest parse-pagination-accepts-a-listed-page-size-test
  (is (= 500 (:page-size (ui/parse-pagination {"page_size" "500"} [:before_ts :before_id])))))

(deftest parse-pagination-builds-cursor-from-named-keys-in-order-test
  (is (= ["2026-01-01" "abc"]
         (:page_cursor (ui/parse-pagination {"before_ts" "2026-01-01" "before_id" "abc"}
                                             [:before_ts :before_id])))))

(deftest parse-pagination-treats-a-partial-cursor-as-no-cursor-test
  (is (nil? (:page_cursor (ui/parse-pagination {"before_ts" "2026-01-01"} [:before_ts :before_id]))))
  (is (nil? (:page_cursor (ui/parse-pagination {"before_id" "abc"} [:before_ts :before_id])))))

(deftest parse-pagination-works-with-different-cursor-key-names-test
  (is (= ["x" "y"]
         (:page_cursor (ui/parse-pagination {"after_a" "x" "after_b" "y"} [:after_a :after_b])))))

(deftest workflow-link-defaults-href-to-plain-workflows-path-test
  (is (= "<a href=\"/workflows/abc\" style=\"color:#0645ad; text-decoration:underline\">\u29C9 <code>abc</code></a>"
         (str (hiccup/html (ui/workflow-link "abc"))))))

(deftest workflow-link-accepts-an-explicit-href-test
  (is (= "<a href=\"/tenon/workflows/abc\" style=\"color:#0645ad; text-decoration:underline\">\u29C9 <code>abc</code></a>"
         (str (hiccup/html (ui/workflow-link "/tenon/workflows/abc" "abc"))))))

(deftest workflow-link-looks-like-a-link-test
  (let [html (str (hiccup/html (ui/workflow-link "abc")))]
    (is (re-find #"color:#0645ad" html))
    (is (re-find #"text-decoration:underline" html))))

(defn- html [hiccup] (str (hiccup/html hiccup)))

(deftest panel-test
  (is (= (str "<section class=\"panel\"><div class=\"panel-head\"><h2>Result</h2>"
              "<span class=\"panel-muted\">2 items</span></div><div class=\"panel-body\"><p>body</p></div></section>")
         (html (ui/panel {:title "Result" :subtitle "2 items"} [:p "body"]))))
  (is (= "<section class=\"panel\"><div class=\"panel-head\"><h2>Params</h2></div><div class=\"panel-body\"></div></section>"
         (html (ui/panel {:title "Params"})))
      "no subtitle"))

(deftest key-values-test
  (is (= "<dl class=\"kv\"><div><dt>a</dt><dd>1</dd></div><div><dt>b</dt><dd>2</dd></div></dl>"
         (html (ui/key-values [["a" "1"] ["b" "2"]])))))

(deftest error-details-test
  (is (= (str "<div><div class=\"error-class\"><code>java.lang.Exception</code></div>"
              "<p class=\"error-message\">boom</p><pre>extra</pre></div>")
         (html (ui/error-details "java.lang.Exception" "boom" [:pre "extra"])))))

(deftest summary-card-test
  (is (= (str "<section class=\"wf-summary state-DONE\"><div class=\"wf-summary-head\">"
              "<span class=\"chip state-DONE\">Done</span><span>1s</span></div>"
              "<p class=\"wf-summary-note\">a <b>note</b></p>"
              "<dl class=\"wf-facts\"><div><dt>Id</dt><dd>7</dd></div></dl></section>")
         (html (ui/summary-card {:state "DONE" :head [[:span "1s"]] :note ["a " [:b "note"]]}
                                (ui/fact "Id" "7")))))
  (is (not (re-find #"wf-summary-note" (html (ui/summary-card {:state "DONE"}))))
      "no note"))

(deftest relative-time-test
  (is (= "just now" (ui/relative-time 1000 3000)))
  (is (= "42s ago" (ui/relative-time 0 42000)))
  (is (= "3 min ago" (ui/relative-time 0 (* 3 60 1000))))
  (is (= "5 h ago" (ui/relative-time 0 (* 5 3600 1000))))
  (is (= "1 day ago" (ui/relative-time 0 (* 86400 1000))))
  (is (= "2 days ago" (ui/relative-time 0 (* 2 86400 1000))))
  (is (= "in 3 min" (ui/relative-time (* 3 60 1000) 0))))

(deftest timestamp-with-relative-test
  (is (= "<span><code>1970-01-01 00:00:00.000 UTC</code> <span class=\"panel-muted\">(42s ago)</span></span>"
         (html (ui/timestamp-with-relative 0 42000))))
  (is (nil? (ui/timestamp-with-relative nil 0))))

(deftest breadcrumb-test
  (is (= (str "<nav class=\"breadcrumb\"><a href=\"/a\">A</a><span class=\"breadcrumb-sep\">\u203A</span>"
              "<span class=\"breadcrumb-current\">B</span></nav>")
         (html (ui/breadcrumb [["A" "/a"] ["B" nil]])))))

(deftest folded-pre-test
  (is (= "<pre>short</pre>" (html (ui/folded-pre "short"))))
  (let [folded (html (ui/folded-pre "1\n2\n3\n4" {:max-lines 2}))]
    (is (str/includes? folded "<pre class=\"fold-preview\">1\n2\n\u2026</pre>"))
    (is (str/includes? folded "Show all 4 lines"))
    (is (str/ends-with? folded "<pre>1\n2\n3\n4</pre></details>")))
  (is (str/includes? (html (ui/folded-pre (apply str (repeat 50 "x")) {:max-chars 10}))
                     "<pre class=\"fold-preview\">xxxxxxxxxx\n\u2026</pre>")
      "a single very long line is cut too"))

(deftest pluralize-test
  (is (= "0 times" (ui/pluralize 0 "time")))
  (is (= "1 time" (ui/pluralize 1 "time")))
  (is (= "2 times" (ui/pluralize 2 "time"))))

(deftest format-duration-test
  (is (= "153ms" (ui/format-duration 153)))
  (is (= "1.24s" (ui/format-duration 1240)))
  (is (= "2m 03s" (ui/format-duration 123000)))
  (is (= "1h 02m" (ui/format-duration 3720000))))

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

(deftest edn-block-test
  (is (= "<pre>[1 2]</pre>" (html (ui/edn-block "[1 2]"))))
  (is (= "<pre>nil</pre>" (html (ui/edn-block "nil"))))
  (is (= "<div><span class=\"panel-muted\">unreadable EDN, shown raw:</span><pre>#object[Foo]</pre></div>"
         (html (ui/edn-block "#object[Foo]")))))

(deftest state-with-tooltip-test
  (is (= "<span class=\"tree-state-chip\" title=\"boom\"><span class=\"chip state-ERROR\">Error</span></span>"
         (html (ui/state-with-tooltip "ERROR" "boom"))))
  (is (= "<span class=\"tree-state-chip\"><span class=\"chip state-DONE\">Done</span></span>"
         (html (ui/state-with-tooltip "DONE" nil)))))

(deftest panel-toggle-btn-test
  (is (= (str "<details class=\"panel-toggle\"><summary class=\"btn\">Open</summary>"
              "<div class=\"panel-toggle-panel\"><p>panel</p></div></details>")
         (html (ui/panel-toggle-btn [:button "Open"] [:p "panel"]))))
  (is (= "<summary class=\"btn primary extra\" title=\"t\">Go <b>!</b></summary>"
         (re-find #"<summary.*</summary>"
                  (html (ui/panel-toggle-btn [:button.primary {:class "extra" :title "t" :type "button"} "Go " [:b "!"]]
                                             [:p "panel"]))))
      "the button's classes, attributes (but type) and content move onto the summary"))
