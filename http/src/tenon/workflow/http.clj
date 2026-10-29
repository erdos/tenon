(ns tenon.workflow.http
  (:require [ring.middleware.params :refer [wrap-params]]
            [ring.util.response :as response]
            [clojure.string :as str]
            [tenon.workflow :as engine]
            [tenon.workflow.http.details-page]
            [tenon.workflow.http.endpoint :as endpoint]
            [tenon.workflow.http.listing-page]
            [tenon.workflow.ui :as ui]))

(defmethod endpoint/model :view/pending [_ _]
  {:rows (engine/list-pending)})

(defmethod endpoint/model :view/restart
  [_ {{:keys [id]} :path-params :keys [query-params]}]
  ;; Returns once the restart is recorded: the workflow runs in the
  ;; background, so its own failure is not a failed restart.
  (assoc (try
           {:invocation-id (engine/start-restart id (= "true" (get query-params "recursive")))
            :restart-error nil}
           (catch clojure.lang.ExceptionInfo e
             (when-not (= ::engine/precondition-failed (:type (ex-data e)))
               (throw e))
             {:invocation-id (or (some-> id engine/get-invocation :workflow/invocation-id) id)
              :restart-error (ex-message e)}))
         :requested-id id))

(defmethod endpoint/status :view/restart [_ {:keys [restart-error]}]
  (if restart-error 409 200))

(defmethod endpoint/view :view/restart [_ {:keys [invocation-id restart-error]}]
  (response/redirect (ui/uri :workflows invocation-id {:restart_error restart-error}) :see-other))

(defn- route [{:keys [request-method uri]}]
  (let [[_ id restart] (re-matches #"/workflows/(\d+)(/restart)?" uri)
        id (some-> id parse-long)
        get? (= :get request-method)]
    (cond
      (and get? (= uri "/")) [:view/listing]
      (and get? (= uri "/workflows/pending")) [:view/pending]
      (and id restart (= :post request-method)) [:view/restart {:id id}]
      (and id get? (not restart)) [:view/details {:id id}])))

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
  [engine prefix request]
  (binding [ui/*uri-prefix* prefix
            engine/*workflow-engine* engine]
    (let [[endpoint path-params] (route request)]
      (endpoint/respond endpoint (assoc request :path-params path-params)))))

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
        tenon-handler (wrap-params (fn [request] (handler engine prefix request)))]
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
  ([request] (endpoint/respond nil request))
  ([request respond _raise] (respond (not-found-handler request))))

(defn app
  "The standalone, unprefixed (mounted at \"/\") ring app."
  [engine]
  (wrap-handler engine "" not-found-handler))
