(ns tenon.workflow.server
  "Runs tenon.workflow.http/app under Jetty. Lives under the :test alias
   (not src) so the jetty adapter dep - and starting a server at all -
   stays out of the library proper, while still being available for
   ./run repl and ./run test."
  (:require [ring.adapter.jetty :as jetty]
            [ring.middleware.reload :refer [wrap-reload]]
            [tenon.workflow.http :as http]))

(defn start!
  "engine is an application-state map as built by
   tenon.workflow/init. dirs are the source directories to watch
   (relative to the process's CWD) - changed namespaces under them are
   reloaded before each request, via wrap-reload. Defaults to this
   project's own two source trees, for the common case of running via
   ./run repl/./run test from the repo root."
  [engine {:keys [port dirs] :or {port 3000 dirs ["src" "http/src"]}}]
  (jetty/run-jetty (wrap-reload (http/app engine) {:dirs dirs}) {:port port :join? false}))
