(ns cmr.metadata-db.system
  "Defines functions for creating, starting, and stopping the application. Applications are
  represented as a map of components. Design based on
  http://stuartsierra.com/2013/09/15/lifecycle-composition and related posts."
  (:require [cmr.common.lifecycle :as lifecycle]
            [clojure.string :as string]
            [cmr.common.log :as log :refer (debug info warn error)]
            [cmr.common.nrepl :as nrepl]
            [cmr.common.api.web-server :as web]
            [cmr.oracle.connection :as oracle]
            [cmr.metadata-db.api.routes :as routes]
            [cmr.metadata-db.services.jobs :as mdb-jobs]
            [cmr.common.jobs :as jobs]
            [cmr.oracle.config :as oracle-config]
            [cmr.metadata-db.config :as config]
            [cmr.transmit.config :as transmit-config]
            [cmr.acl.core :as acl]
            [cmr.common.config :as cfg]
            [cmr.message-queue.queue.rabbit-mq :as rmq]
            [cmr.common-app.system :as common-sys]))

;; Design based on http://stuartsierra.com/2013/09/15/lifecycle-composition and related posts

(def ^:private component-order
  "Defines the order to start the components."
  [:db :log :queue-broker :scheduler :web :nrepl])

(def system-holder
  "Required for jobs"
  (atom nil))

(defn create-system
  "Returns a new instance of the whole application."
  ([]
   (create-system "metadata-db"))
  ([connection-pool-name]
   (let [sys {:db (assoc (oracle/create-db (config/db-spec connection-pool-name))
                         :result-set-fetch-size
                         (config/result-set-fetch-size))
              :log (log/create-logger)
              :web (web/create-web-server (transmit-config/metadata-db-port) routes/make-api)
              :nrepl (nrepl/create-nrepl-if-configured (config/metadata-db-nrepl-port))
              :parallel-chunk-size (config/parallel-chunk-size)
              :caches {acl/token-imp-cache-key (acl/create-token-imp-cache)}
              :scheduler (jobs/create-clustered-scheduler `system-holder :db mdb-jobs/jobs)
              :queue-broker (rmq/create-queue-broker (config/rabbit-mq-config))
              :relative-root-url (transmit-config/metadata-db-relative-root-url)}]
     (transmit-config/system-with-connections sys [:echo-rest]))))

(def start
  "Performs side effects to initialize the system, acquire resources,
  and start it running. Returns an updated instance of the system."
  (common-sys/start-fn "Metadata DB" component-order))

(def stop
  "Performs side effects to shut down the system and release its
  resources. Returns an updated instance of the system."
  (common-sys/stop-fn "Metadata DB" component-order))
