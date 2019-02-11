(ns cmr.common.validations.json-schema
  "Functions used to perform JSON schema validation. See http://json-schema.org/
  for more details."
  (:require
   [clojure.java.io :as io]
   [cmr.common.log :as log :refer (warn info)]
   [cmr.common.services.errors :as errors]
   [cmr.schema-validation.json-schema :as json-schema])
  (:import
   (org.everit.json.schema ValidationException)
   (org.json JSONException)))

(defn validate-json
  "Performs schema validation using the provided JSON schema and the given
  json string to validate. Uses org.everit.json.schema to perform the
  validation. The JSON schema must be provided as a
  org.everit.json.schema.Schema object and the json-to-validate must
  be a string. Returns a list of the errors found.

  For details, see:
  * [Schema](http://erosb.github.io/everit-json-schema/javadoc/1.11.0/)"
  [^Schema json-schema json-to-validate]
  (try
    (json-schema/validate-json json-schema json-to-validate true)
    (catch JSONException e
      (errors/throw-service-error :bad-request (str "Invalid JSON: " (.getMessage e))))
    (catch ValidationException e
      (let [message (seq (.getAllMessages e))]
        (info (str "UMM Validation error. Full message: " (pr-str message)))
        message))))

(defn validate-json!
  "Validates the JSON string against the given schema. Throws a service error
  if it is invalid."
  [schema json-str]
  (when-let [errors (seq (validate-json schema json-str))]
    (errors/throw-service-errors :bad-request errors)))
