(ns cmr.elastic-utils.embedded-elastic-server
  "Used to run an in memory Elasticsearch server."
  (:require
   [clj-http.client :as client]
   [cmr.common.lifecycle :as lifecycle]
   [cmr.common.log :as log :refer [debug info warn error]]
   [cmr.common.util :as util]
   [cmr.elastic-utils.config :as config])
  (:import
   (org.elasticsearch.common.settings ImmutableSettings ImmutableSettings$Builder)
   (org.elasticsearch.common.logging.log4j LogConfigurator)
   (org.elasticsearch.node Node NodeBuilder)))

(comment
  (def settings (create-settings {:http-port 9201
                                  :transport-port 9203
                                  :data-dir"es_data2"}))
  (def env (org.elasticsearch.env.Environment. settings))
  (.configFile env))

(defn- setup-logging
  "Sets up elastic search logging."
  [settings]
  (LogConfigurator/configure settings))

(defn- create-settings
  "Creates an Elastic Search Immutable Settings"
  [{:keys [http-port transport-port ^String data-dir]}]
  (let [^ImmutableSettings$Builder builder (ImmutableSettings/settingsBuilder)]
    (.. builder
        (put "node.name" "embedded-elastic")
        (put "path.conf" ".")
        (put "path.data" data-dir)
        (put "http.port" (str http-port))
        (put "transport.tcp.port" (str transport-port))
        (put "index.store.type" "memory")
        ;; dynamic scripting configurations
        (put "scipt.inline" "on")
        (put "script.search" "on")
        (put "index.gc_deletes" (config/elastic-garbage-collection-interval))
        build)))

(defn- build-node
  "Creates the internal elastic search node that will run."
  [^ImmutableSettings node-settings]
  (let [^NodeBuilder builder (NodeBuilder/nodeBuilder)]
    (.. builder
        (settings node-settings)

        (clusterName "embedded-cluster")

        ;;Is the node going to be allowed to allocate data (shards) to it or not.
        (data true)

        ;;The node is local within a JVM. It will not try to connect to nodes outside
        (local true)

        ;; Starts the node
        node)))

(defrecord ElasticServer
  [
   http-port
   transport-port
   data-dir
   node]


  lifecycle/Lifecycle

  (start
    [this system]
    (debug "Starting elastic server on port" http-port)
    (let [node-settings (create-settings this)
          _ (setup-logging node-settings)
          this (assoc this :node (build-node node-settings))]
      ;; See http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/cluster-health.html
      (client/get
        (format
          "http://localhost:%s/_cluster/health?wait_for_status=yellow&timeout=50s"
          (:http-port this)))
      this))

  (stop
    [this system]
    (when-let [^Node node (:node this)]
      (do
        (.close node)
        (util/delete-recursively (:data-dir this))))
    (assoc this :node nil)))

(defn create-server
  ([]
   (create-server 9200 9300 "data"))
  ([http-port transport-port data-dir]
   (->ElasticServer http-port transport-port data-dir nil)))


(comment

  (def server (create-server))

  (def started-server (lifecycle/start server nil))

  (def stopped-server (lifecycle/stop started-server nil)))
