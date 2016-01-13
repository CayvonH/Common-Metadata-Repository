(ns cmr.metadata-db.int-test.concepts.collection-save-test
  "Contains integration tests for saving collections. Tests saves with various configurations including
  checking for proper error handling."
  (:require [clojure.test :refer :all]
            [clj-http.client :as client]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.local :as l]
            [cmr.metadata-db.int-test.utility :as util]
            [cmr.metadata-db.services.messages :as msg]
            [cmr.metadata-db.services.concept-constraints :as cc]
            [cmr.metadata-db.int-test.concepts.concept-save-spec :as c-spec]))


;;; fixtures
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Set up REG_PROV as regular provider and SMAL_PROV1 as a small provider
(use-fixtures :each (util/reset-database-fixture {:provider-id "REG_PROV" :small false}
                                                 {:provider-id "SMAL_PROV1" :small true}
                                                 {:provider-id "SMAL_PROV2" :small true}))

(defmethod c-spec/gen-concept :collection
  [_ provider-id uniq-num attributes]
  (util/collection-concept provider-id uniq-num attributes))

;; tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest save-collection-test
  (c-spec/general-save-concept-test :collection ["REG_PROV" "SMAL_PROV1"]))

(deftest save-test-with-missing-required-parameters
  (c-spec/save-test-with-missing-required-parameters
    :collection ["REG_PROV" "SMAL_PROV1"] [:concept-type :provider-id :native-id :extra-fields]))

(deftest save-collection-with-same-native-id-test
  (testing "Save collections with the same native-id for two small providers is OK"
    (let [coll1 (util/collection-concept "SMAL_PROV1" 1 {:native-id "foo"})
          coll2 (util/collection-concept "SMAL_PROV2" 2 {:native-id "foo"})]
      (c-spec/save-distinct-concepts-test coll1 coll2))))

(deftest save-collection-post-commit-constraint-violations
  (testing "duplicate entry titles"
    (doseq [provider-id ["REG_PROV" "SMAL_PROV1"]]
      (let [existing-concept-id (str "C1-" provider-id)
            existing-collection (util/collection-concept provider-id 1
                                                         {:concept-id existing-concept-id
                                                          :revision-id 1
                                                          :extra-fields {:entry-title "ET-1"}})
            test-concept-id (str "C2-" provider-id)
            test-collection (util/collection-concept provider-id 2
                                                     {:concept-id test-concept-id
                                                      :revision-id 1
                                                      :extra-fields {:entry-title "ET-1"}})
            _ (util/save-concept existing-collection)
            test-collection-response (util/save-concept test-collection)]

        ;; The collection should be rejected due to another collection having the same entry-title
        (is (= {:status 409,
                :errors [(msg/duplicate-field-msg :entry-title [existing-collection])]}
               (select-keys test-collection-response [:status :errors])))

        ;; We need to verify that the collection which was inserted and failed the post commit
        ;; constraint checks is cleaned up from the database. We do this by verifying that
        ;; the db only contains the original collection.
        (let [found-concepts (util/find-concepts :collection
                                                 {:entry-title "ET-1" :provider-id provider-id})]
          (is (= [existing-collection]
                 (map #(dissoc % :revision-date) (:concepts found-concepts))))))))
  (testing "duplicate entry titles within multiple small providers is OK"
    (let [coll1 (util/collection-concept "SMAL_PROV1" 1
                                         {:concept-id "C1-SMAL_PROV1"
                                          :revision-id 1
                                          :extra-fields {:entry-title "ET-1"}})
          coll2 (util/collection-concept "SMAL_PROV2" 2
                                         {:concept-id "C2-SMAL_PROV2"
                                          :revision-id 1
                                          :extra-fields {:entry-title "ET-1"}})
          _ (util/save-concept coll1)
          {:keys [status]} (util/save-concept coll2)]
      (is (= 201 status))
      (util/verify-concept-was-saved coll1)
      (util/verify-concept-was-saved coll2))))

