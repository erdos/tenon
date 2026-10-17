(ns tenon.workflow.kerodon-util
  "Shared helpers for the kerodon based UI tests: an HTML5 parser, a
   kerodon state for a lone hiccup component, enlive selector helpers and
   a few validations kerodon.test lacks - use them with kerodon.test/has."
  (:require [clojure.string :as str]
            [hiccup2.core :as hiccup]
            [kerodon.core :as k]
            [kerodon.test :as kt]
            [net.cgrand.enlive-html :as enlive]
            [net.cgrand.jsoup :as jsoup])
  (:import [org.jsoup Jsoup]))

(defn- html5-parser [reader]
  (jsoup/->nodes (Jsoup/parse ^String (slurp reader))))

(defn html5-fixture
  "Makes kerodon parse pages as HTML5. Enlive's default parser (TagSoup)
   knows HTML 4 only: it drops <section>s and moves a <dl>'s <div>s out
   of it."
  [f]
  (enlive/with-options {:parser html5-parser}
    (f)))

(defn render
  "A kerodon state holding hiccup rendered on its own, to check a UI
   component with kerodon.test validations."
  [hiccup]
  (let [body (str (hiccup/html hiccup))]
    (-> (k/session (constantly {:status 200
                                :headers {"Content-Type" "text/html; charset=utf-8"}
                                :body body}))
        (k/visit "/"))))

(defn text-of
  "The text of enlive node, its whitespace runs collapsed and trimmed."
  [node]
  (-> (apply str (enlive/texts [node])) (str/replace #"\s+" " ") str/trim))

;; selectors

(defn with-text
  "Selector predicate: an element whose text (see text-of) is s."
  [s]
  (enlive/pred #(= s (text-of %))))

(defn with-attr-matching
  "Selector predicate: an element whose attr attribute matches re."
  [attr re]
  (enlive/pred #(some->> (get-in % [:attrs attr]) (re-matches re))))

(defn dd-of
  "Selector: the <dd> after the <dt> labelled label - a key-values or
   summary-card fact's value."
  [label]
  [[:dd (enlive/left [:dt (with-text label)])]])

(defn panel
  "Selector: the panel titled title."
  [title]
  [[:section.panel (enlive/has [[:h2 (with-text title)]])]])

;; validations

(defn select [state selector]
  (enlive/select (:enlive state) selector))

(defmacro count?
  "Validates that selector matches exactly expected elements."
  [selector expected]
  `(kt/validate = #(count (select % ~selector)) ~expected (~'count? ~selector ~expected)))

(defmacro present?
  "Validates that selector matches at least one element."
  [selector]
  `(kt/validate (fn [n# _#] (pos? n#)) #(count (select % ~selector)) nil (~'present? ~selector)))

(defmacro texts?
  "Validates the texts (see text-of) of the elements selector matches,
   in document order."
  [selector expected]
  `(kt/validate = #(mapv text-of (select % ~selector)) ~expected (~'texts? ~selector ~expected)))

(defmacro attrs?
  "Validates attribute attr of the elements selector matches, in document
   order."
  [selector attr expected]
  `(kt/validate = #(mapv (fn [node#] (get-in node# [:attrs ~attr])) (select % ~selector))
                ~expected (~'attrs? ~selector ~attr ~expected)))
