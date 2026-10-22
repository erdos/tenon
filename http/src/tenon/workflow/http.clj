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

(defn- restart!
  "Restarts workflow id from the detail page's restart form - along with
   its failed nested workflows if recursive? - then redirects (303) back to
   the page - with the failure as a restart_error query param if it failed,
   which the page shows."
  [id recursive?]
  (let [error (try
                (if recursive?
                  (engine/restart-invocation-recursively id)
                  (engine/restart-invocation id))
                nil
                (catch Throwable e (or (ex-message e) (str (class e)))))
        current (or (:invocation_id (some-> id engine/get-invocation)) id)]
    (response/redirect (ui/uri :workflows current {:restart_error error}) :see-other)))

(defn- pending! []
  (try
    (response/response (mapv workflow->response (engine/list-pending)))
    (catch Throwable e
      (-> (response/response {:error (ex-message e)}) (response/status 500)))))

(defn- html-response [body]
  (-> (response/response body) (response/content-type "text/html; charset=utf-8")))

(defn handler
  "Ring handler for tenon's dashboard/detail pages, restart form and
   pending-workflows JSON endpoint. engine is
   an application-state map as built by tenon.workflow/init -
   bound to engine/*workflow-engine* for the duration of the request, so
   the tenon.workflow functions called here reach it without engine
   threaded through every helper's argument list. prefix is the URI prefix tenon is
   mounted under - request's :uri is expected to already be relative to
   it (i.e. with prefix stripped, as wrap-handler does), and every
   link/URL this handler's HTML generates gets prefix prepended back on
   via ui/*uri-prefix*, also bound here. Pass \"\" to mount unprefixed at the
   app's own root."
  [engine prefix {:keys [request-method uri query-params]}]
  (binding [ui/*uri-prefix* prefix
            engine/*workflow-engine* engine]
    (let [[_ id restart] (re-matches #"/workflows/(\d+)(/restart)?" uri)
          id (some-> id parse-long)
          get? (= :get request-method)]
      (or (cond
            (and get? (= uri "/")) (html-response (listing-page/html query-params))
            (and get? (= uri "/workflows/pending")) (pending!)
            (and id restart (= :post request-method)) (restart! id (= "true" (get query-params "recursive")))
            (and id get? (not restart)) (some-> (details-page/html nil id (not-empty (get query-params "restart_error")))
                                                html-response))
          (response/not-found {:error "not found"})))))

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
  (let [prefix (if (= "/" tenon-uri-prefix) "" tenon-uri-prefix)
        tenon-handler (-> (fn [request] (handler engine prefix request))
                          wrap-params
                          wrap-json-response)]
    (fn
      ([{:keys [uri] :as request}]
       (if-let [relative-uri (strip-prefix prefix uri)]
         (tenon-handler (assoc request :uri relative-uri))
         (original-handler request)))
      ([{:keys [uri] :as request} respond raise]
       (if-let [relative-uri (strip-prefix prefix uri)]
         (try (respond (tenon-handler (assoc request :uri relative-uri)))
              (catch Throwable t (raise t)))
         (original-handler request respond raise))))))

(defn- not-found-handler
  ([_request] (response/not-found {:error "not found"}))
  ([request respond _raise] (respond (not-found-handler request))))

(defn app
  "The standalone, unprefixed (mounted at \"/\") ring app."
  [engine]
  (wrap-handler engine "" not-found-handler))
