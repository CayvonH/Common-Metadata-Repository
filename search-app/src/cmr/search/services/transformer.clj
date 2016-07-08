(ns cmr.search.services.transformer
  "Provides functions for retrieving concepts in a desired format."
  (:require [cmr.metadata-db.services.concept-service :as metadata-db]
            [cmr.common.log :as log :refer (debug info warn error)]
            [cmr.common.mime-types :as mt]
            [cmr.common.services.errors :as errors]
            [cmr.search.services.acl-service :as acl-service]
            [cmr.search.services.acls.acl-helper :as acl-helper]
            [cmr.search.services.acls.acl-results-handler-helper :as acl-rhh]
            [cmr.common.util :as u]
            [cmr.search.services.result-format-helper :as rfh]
            [cmr.search.data.metadata-retrieval.metadata-transformer :as metadata-transformer]
            [cmr.search.data.metadata-retrieval.metadata-cache :as metadata-cache]))


(def transformer-supported-format?
  "The set of formats supported by the transformer."
  #{:echo10 :dif :dif10 :iso19115 :iso-smap})

(defn context->metadata-db-context
  "Converts the context into one that can be used to invoke the metadata-db services."
  [context]
  (assoc context :system (get-in context [:system :embedded-systems :metadata-db])))


(defn- concept->value-map
  "Convert a concept into a map containing :metadata in a desired format, :format, :concept-id,
   :revision-id, and :collection-concept-id if granules."
  [context concept target-format]
  (let [collection-concept-id (get-in concept [:extra-fields :parent-collection-id])
        concept-format (mt/mime-type->format (:format concept))
        _ (when-not concept-format
            (errors/internal-error! "Did not recognize concept format" (pr-str (:format concept))))
        value-map (if (or (contains? #{:xml :native} target-format) ;; xml is also a native format
                          (= target-format concept-format))
                    (select-keys concept [:metadata :concept-id :revision-id :format])
                    (let [metadata (metadata-transformer/transform context concept target-format)]
                      (assoc (select-keys concept [:concept-id :revision-id])
                             :metadata metadata
                             :format (rfh/search-result-format->mime-type target-format))))]
    (if collection-concept-id
      (assoc value-map :collection-concept-id collection-concept-id)
      value-map)))

(defn get-formatted-concept-revisions
  "Get concepts with given concept-id, revision-id pairs in a given format. Does not apply acls to
  the concepts found."
  [context concept-type concepts-tuples target-format allow-missing?]
  (metadata-cache/get-formatted-concept-revisions
   context concept-type concepts-tuples target-format allow-missing?))
  ; (info "Transforming" (count concepts-tuples) "concept(s) to" target-format)
  ; (let [mdb-context (context->metadata-db-context context)
  ;       [t1 concepts] (u/time-execution
  ;                       (doall (metadata-db/get-concepts mdb-context concepts-tuples allow-missing?)))
  ;       [t2 values] (u/time-execution
  ;                     (doall (pmap #(concept->value-map context % target-format) concepts)))]
  ;   (debug "get-concept-revisions time:" t1
  ;          "concept->value-map time:" t2)
  ;   values))

(defn get-latest-formatted-concepts
  "Get latest version of concepts with given concept-ids in a given format. Applies ACLs to the concepts
  found."
  ([context concept-ids target-format]
   (get-latest-formatted-concepts context concept-ids target-format false))
  ([context concept-ids target-format skip-acls?]
   (info "Getting latest version of" (count concept-ids) "concept(s) in" target-format "format")

   (let [mdb-context (context->metadata-db-context context)
         [t1 concepts] (u/time-execution
                         (doall (metadata-db/get-latest-concepts mdb-context concept-ids true)))
         ;; Filtering deleted concepts
         [t2 concepts] (u/time-execution (doall (filter #(not (:deleted %)) concepts)))]

     (if skip-acls?
       ;; Convert concepts to results without acl enforcment
       (let [[t3 values] (u/time-execution
                           (doall (pmap #(concept->value-map context % target-format) concepts)))]
         (debug "get-latest-concepts time:" t1
                "tombstone-filter time:" t2
                "concept->value-map time:" t3)
         values)

       ;; Convert concepts to results with acl enforcment
       (let [[t3 concepts] (u/time-execution (acl-rhh/add-acl-enforcement-fields concepts))
             [t4 concepts] (u/time-execution (acl-service/filter-concepts context concepts))
             [t5 values] (u/time-execution
                           (doall (pmap #(concept->value-map context % target-format) concepts)))]
         (debug "get-latest-concepts time:" t1
                "tombstone-filter time:" t2
                "add-acl-enforcement-fields time:" t3
                "acl-filter-concepts time:" t4
                "concept->value-map time:" t5)
         values)))))

(defn get-formatted-concept
  "Get a specific revision of a concept with the given concept-id in a given format.
  Applies ACLs to the concept found."
  [context concept-id revision-id target-format]
  (info "Getting revision" revision-id "of concept" concept-id "in" target-format "format")
  (let [mdb-context (context->metadata-db-context context)
        [t1 concept] (u/time-execution
                       (metadata-db/get-concept mdb-context concept-id revision-id))
        ;; Throw a service error for deleted concepts
        _ (when (:deleted concept)
            (errors/throw-service-errors
              :bad-request
              [(format
                 "The revision [%d] of concept [%s] represents a deleted concept and does not contain metadata."
                 revision-id
                 concept-id)]))
        [t2 concept] (u/time-execution (acl-rhh/add-acl-enforcement-fields-to-concept concept))
        [t3 [concept]] (u/time-execution (acl-service/filter-concepts context [concept]))
        ;; format concept
        [t4 value] (u/time-execution (when concept (concept->value-map context concept target-format)))]
    (debug "get-concept time:" t1
           "add-acl-enforcement-fields time:" t2
           "acl-filter-concepts time:" t3
           "concept->value-map time:" t4)
    value))
