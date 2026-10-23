(ns tenon.workflow.http.endpoint
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [ring.util.response :as response]))

(defmulti model (fn [endpoint _request] endpoint))

(defmulti view (fn [endpoint _model] endpoint))

(defmulti status (fn [endpoint _model] endpoint))

(defmethod status :default [_ _] 200)

(defmulti render-response (fn [mime _endpoint _model] mime))

(defmethod render-response "text/html" [_ endpoint m]
  (let [page (view endpoint m)]
    (if (map? page)
      page
      (-> (response/response page) (response/content-type "text/html; charset=utf-8")))))

(defmethod render-response "application/edn" [_ endpoint m]
  (-> (response/response (pr-str m))
      (response/status (status endpoint m))
      (response/content-type "application/edn; charset=utf-8")))

(defmethod render-response "application/json" [_ endpoint m]
  (-> (response/response (json/generate-string m))
      (response/status (status endpoint m))
      (response/content-type "application/json; charset=utf-8")))

(defn- format-name [mime] (second (str/split mime #"/")))

(defn- supported-mime-types [endpoint]
  (->> (keys (methods render-response))
       (filter string?)
       (remove #(and (= "text/html" %) (not (get-method view endpoint))))
       (sort-by (juxt #(case % "text/html" 0 "application/json" 1 2) identity))))

(defn- media-ranges [accept]
  (for [part (str/split (str/lower-case accept) #",")
        :let [[range & params] (map str/trim (str/split part #";"))
              q (some #(some-> (re-matches #"q=([\d.]+)" %) second parse-double) params)]
        :when (seq range)]
    [range (or q 1.0)]))

(defn- specificity [range mime]
  (cond
    (= range mime) 2
    (= range (str (first (str/split mime #"/")) "/*")) 1
    (= range "*/*") 0))

(defn- quality [ranges mime]
  (some->> (seq (keep (fn [[range q]] (some-> (specificity range mime) (vector q))) ranges))
           (apply max-key first)
           second))

(defn- preferred-mime-type [mime-types accept-header]
  (let [ranges (media-ranges accept-header)]
    (some->> (seq (keep #(let [q (quality ranges %)] (when (and q (pos? q)) [% q])) mime-types))
             (reduce #(if (> (second %2) (second %1)) %2 %1))
             first)))

(defn mime-type [endpoint {:keys [query-params headers]}]
  (let [mime-types (supported-mime-types endpoint)
        fmt (get query-params "format")
        accept (get headers "accept")]
    (or (some #(when (= fmt (format-name %)) %) mime-types)
        (when-not (str/blank? accept) (preferred-mime-type mime-types accept))
        (first mime-types))))

(defn- failure [endpoint code request m]
  (-> (render-response (mime-type endpoint request) endpoint m) (response/status code)))

(defn respond [endpoint request]
  (-> (try
        (if-some [m (when endpoint (model endpoint request))]
          (render-response (mime-type endpoint request) endpoint m)
          (failure :view/not-found 404 request {:error "not found"}))
        (catch Throwable e
          (failure :view/error 500 request {:error (or (ex-message e) (str (class e)))})))
      (response/header "Vary" "Accept")))
