(ns tenon.workflow.http
  (:require [ring.middleware.json :refer [wrap-json-response]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.util.response :as response]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [tenon.workflow :as engine]
            [tenon.workflow.http.details-page :as details-page]
            [tenon.workflow.http.listing-page :as listing-page]
            [tenon.workflow.ui :as ui]))

(defn- workflow->response [wf]
  (try
    {:invocation_id (:invocation_id wf) :wf_def (:wf_def wf) :params (edn/read-string (:params wf))}
    (catch Throwable _
      {:invocation_id (:invocation_id wf) :wf_def (:wf_def wf) :params_raw (:params wf)})))

(defn- error-response [status body]
  (-> (response/response body) (response/status status)))

(defn- restart! [id]
  (try
    (engine/restart-invocation id)
    (let [wf (engine/get-invocation id)]
      (response/response {:invocation_id (:invocation_id wf) :state (:state wf) :result (:result wf)}))
    (catch clojure.lang.ExceptionInfo e
      (if (= ::engine/precondition-failed (:type (ex-data e)))
        (error-response 409 {:error (ex-message e) :data (dissoc (ex-data e) :type)})
        (error-response 500 {:error (ex-message e)})))
    (catch Throwable e
      (error-response 500 {:error (ex-message e)}))))

(defn- restart-from-form!
  "Restarts workflow id from the detail page's restart form, then redirects
   (303) back to the page - with the failure as a restart_error query
   param if it failed, which the page shows."
  [id]
  (let [error (try
                (engine/restart-invocation id)
                nil
                (catch Throwable e (or (ex-message e) (str (class e)))))
        current (or (:invocation_id (some-> id engine/get-invocation)) id)]
    (response/redirect (ui/uri :workflows current {:restart_error error}) :see-other)))

(defn- pending! []
  (try
    (response/response (mapv workflow->response (engine/list-pending)))
    (catch Throwable e
      (error-response 500 {:error (ex-message e)}))))

(defn- html-response [body]
  (-> (response/response body) (response/content-type "text/html; charset=utf-8")))

(defn handler
  "Ring handler for tenon's dashboard/detail pages and JSON API. engine is
   an application-state map as built by tenon.workflow/init -
   bound to engine/*workflow-engine* for the duration of the request, so
   the tenon.workflow functions called here reach it without engine
   threaded through every helper's argument list. prefix is the URI prefix tenon is
   mounted under - request's :uri is expected to already be relative to
   it (i.e. with prefix stripped, as wrap-handler does), and every
   link/URL this handler's HTML generates gets prefix prepended back on
   via ui/*uri-prefix*, also bound here. Pass \"\" to mount unprefixed at the
   app's own root."
  [engine prefix {:keys [request-method uri query-params form-params]}]
  (binding [ui/*uri-prefix* prefix
            engine/*workflow-engine* engine]
    (cond
      (and (= request-method :get) (= uri "/"))
      (html-response (listing-page/html query-params))

      (and (= request-method :get) (= uri "/workflows/pending"))
      (pending!)

      (and (= request-method :post) (re-matches #"/workflows/[^/]+/restart" uri))
      (let [id (parse-long (second (re-matches #"/workflows/([^/]+)/restart" uri)))]
        (if (and id (= "form" (get form-params "via")))
          (restart-from-form! id)
          (restart! id)))

      (and (= request-method :get) (re-matches #"/workflows/[^/]+" uri))
      (if-let [page (details-page/html (parse-long (second (re-matches #"/workflows/([^/]+)" uri)))
                                       (not-empty (get query-params "restart_error")))]
        (html-response page)
        (response/not-found {:error "not found"}))

      :else
      (response/not-found {:error "not found"}))))

(defn- strip-prefix
  "request-uri relative to prefix, or nil if request-uri isn't actually
   under prefix. prefix \"\" matches everything (identity - handler's own
   routes already start with \"/\"). request-uri exactly equal to prefix
   (no trailing slash, e.g. hitting \"/tenon\" for prefix \"/tenon\") maps
   to \"/\", tenon's own dashboard route."
  [prefix request-uri]
  (cond
    (= request-uri prefix) "/"
    (str/starts-with? request-uri (str prefix "/")) (subs request-uri (count prefix))
    :else nil))

(defn wrap-handler
  "Wraps original-handler (any ring handler) so a request whose :uri
   falls under tenon-uri-prefix is served by tenon's dashboard/detail
   pages and JSON API instead - with tenon-uri-prefix stripped from :uri
   before tenon routes it, and prepended back onto every link/URL tenon
   generates, so its UI keeps working when mounted under a prefix inside
   an existing app. Any other request passes through to original-handler
   unchanged. This is how tenon's HTML UI integrates into an existing app
   without owning its whole route space.

   engine is an application-state map as built by
   tenon.workflow/init - passed through to handler for every
   request tenon serves."
  [engine tenon-uri-prefix original-handler]
  (let [prefix (if (= "/" tenon-uri-prefix) "" tenon-uri-prefix)]
    (fn
      ([{:keys [uri] :as request}]
       (if-let [relative-uri (strip-prefix prefix uri)]
         (handler engine prefix (assoc request :uri relative-uri))
         (original-handler request)))
      ([{:keys [uri] :as request} respond raise]
       (if-let [relative-uri (strip-prefix prefix uri)]
         (when-let [resp (try (handler engine prefix (assoc request :uri relative-uri))
                              (catch Throwable t (raise t) nil))]
           (respond resp))
         (original-handler request respond raise))))))

(defn- not-found-handler
  ([_request] (response/not-found {:error "not found"}))
  ([request respond _raise] (respond (not-found-handler request))))

(defn app
  "The standalone, unprefixed (mounted at \"/\") ring app."
  [engine]
  (-> (wrap-handler engine "" not-found-handler) wrap-params wrap-json-response))
