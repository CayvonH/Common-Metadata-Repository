(ns cmr.redis-utils.redis-cache
  "An implementation of the CMR cache protocol on top of Redis. Mutliple applications
  have multiple caches which use one instance of Redis. Therefore calling reset on
  a Redis cache should not delete all of the data stored. Instead when creating a Redis
  cache the caller must provide a list of keys which should be deleted when calling
  reset on that particular cache. TTL MUST be set if you expect your keys to be evicted automatically."
  (:require
   [clojure.edn :as edn]
   [cmr.common.cache :as cache]
   [cmr.redis-utils.config :as config]
   [cmr.redis-utils.redis :refer [wcar*]]
   [taoensso.carmine :as carmine]))

(defn serialize
  "Serializes v."
  [v]
  (pr-str v))

(defn deserialize
  "Deserializes v."
  [v]
  (when v (edn/read-string v)))

;; Implements the CmrCache protocol by saving data in Redis.
(defrecord RedisCache
  [
   ;; A collection of keys used by this cache. Only these keys will be deleted from the backend
   ;; store on calls to reset.
   keys-to-track

   ;; The time to live for the key in seconds.
   ttl

   ;; Refresh the time to live when GET operations are called on key.
   refresh-ttl?]

  cache/CmrCache
  (get-keys
    [this]
    (vec
     (filter (fn [key]
               (= 1 (wcar* (carmine/exists (serialize key)))))
             keys-to-track)))

  (get-value
    [this key]
    (let [s-key (serialize key)]
      (-> (wcar* :as-pipeline
                 (carmine/get s-key)
                 (when refresh-ttl? (carmine/expire s-key ttl)))
          first
          :value)))

  (get-value
    [this key lookup-fn]
    (let [cache-value (cache/get-value this key)]
      (if-not (nil? cache-value)
        cache-value
        (let [value (lookup-fn)]
          (cache/set-value this key value)
          value))))

  (reset
    [this]
    (doseq [the-key keys-to-track]
      (wcar* (carmine/del (serialize the-key)))))

  (set-value
    [this key value]
    ;; Store value in map to aid deserialization of numbers.
    (let [s-key (serialize key)]
      (wcar* (carmine/set s-key {:value value})
             (when ttl (carmine/expire s-key ttl))))))

(defn create-redis-cache
  "Creates an instance of the redis cache.
  options:
      :keys-to-track
       The keys that are to be managed by this cache. Do note this is not a required
       key however if the desired behavior is to use the CMR caches endpoints to
       view and reset this cache it should be provided. Otherwise you must query redis
       directly to view and resest these keys.
      :ttl
       The time to live for the key in seconds. If nil assumes key will never expire. NOTE:
       The key is not guaranteed to stay in the cache for up to ttl. If the
       cache becomes full any key that is not set to persist will
       be a candidate for eviction.
      :refresh-ttl?
       When a GET operation is called called on the key then the ttl is refreshed
       to the time to live set by the initial cache."
  ([]
   (create-redis-cache nil))
  ([options]
   (->RedisCache (:keys-to-track options)
                 (get options :ttl)
                 (get options :refresh-ttl? false))))
