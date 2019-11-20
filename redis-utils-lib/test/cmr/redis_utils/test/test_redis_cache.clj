(ns cmr.redis-utils.test.test-redis-cache
  "Namespace to test redis cache."
  (:require
   [clojure.test :refer :all]
   [cmr.common.cache :as cache]
   [cmr.redis-utils.redis :refer [wcar*]]
   [cmr.redis-utils.redis-cache :as redis-cache]
   [cmr.redis-utils.test.test-util :as test-util]
   [cmr.redis-utils.config :as config]
   [taoensso.carmine :as carmine]))

(use-fixtures :once test-util/embedded-redis-server-fixture)

(deftest test-redis-cache-with-expire
  (testing "Redis cache with default timeout..."
    (let [rcache (redis-cache/create-redis-cache {:ttl 10000})]
      (cache/set-value rcache "test" "expire")
      (is (= "expire"
             (cache/get-value rcache "test")))
      (is (> (wcar* (carmine/ttl (redis-cache/serialize "test")) 0))))))

(deftest test-redis-cache-with-persistance
  (testing "Redis cache with default timeout..."
    (let [rcache (redis-cache/create-redis-cache)]
      (cache/set-value rcache "test" "persist")
      (is (= "persist"
             (cache/get-value rcache "test")))
      (is (= (wcar* (carmine/ttl (redis-cache/serialize "test")) -1))))))

(deftest test-redis-cache-only-aware-of-keys-to-track
  (testing "Redis cache can only view and reset keys it is told about..."
    (let [cache-1-keys #{:cache-1-a :cache-1-b :cache-1-c :cache-1-d}
          cache-2-keys #{:cache-2-a :cache-2-b :cache-2-c :cache-2-d}
          cache-1 (redis-cache/create-redis-cache {:keys-to-track cache-1-keys})
          cache-2 (redis-cache/create-redis-cache {:keys-to-track cache-2-keys})]
      (doseq [k cache-1-keys]
        (cache/set-value cache-1 k "test"))
      (doseq [k cache-2-keys]
        (cache/set-value cache-2 k "test"))
      (is (= cache-1-keys
             (set (cache/get-keys cache-1))))
      (is (= cache-2-keys
             (set (cache/get-keys cache-2))))
      (cache/reset cache-1)
      (is (empty? (cache/get-keys cache-1)))
      ;; Query redis directly as a sanity check
      (is (empty? (wcar* (carmine/keys "cache-1-*"))))
      (is (= cache-2-keys)
          (set (cache/get-keys cache-2)))
      (cache/reset cache-2)
      (is (empty? (cache/get-keys cache-2)))
      (is (empty? (wcar* (carmine/keys "cache-2-*")))))))
