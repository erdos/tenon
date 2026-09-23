(ns tenon.workflow.ui-test
  (:require [clojure.test :refer [deftest is]]
            [hiccup2.core :as hiccup]
            [tenon.workflow.ui :as ui]))

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
  (is (= "<a href=\"/workflows/abc\" style=\"color:#0645ad; text-decoration:underline\"><code>abc</code></a>"
         (str (hiccup/html (ui/workflow-link "abc"))))))

(deftest workflow-link-accepts-an-explicit-href-test
  (is (= "<a href=\"/tenon/workflows/abc\" style=\"color:#0645ad; text-decoration:underline\"><code>abc</code></a>"
         (str (hiccup/html (ui/workflow-link "/tenon/workflows/abc" "abc"))))))

(deftest workflow-link-looks-like-a-link-test
  (let [html (str (hiccup/html (ui/workflow-link "abc")))]
    (is (re-find #"color:#0645ad" html))
    (is (re-find #"text-decoration:underline" html))))
