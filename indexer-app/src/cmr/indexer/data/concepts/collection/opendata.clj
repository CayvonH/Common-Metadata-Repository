(ns cmr.indexer.data.concepts.collection.opendata
  "Contains functions to convert collection into opendata related elasticsearch documents"
  (require
   [cmr.indexer.data.concepts.collection.data-center :as data-center]))

(defn- email-contact?
  "Return true if the given person has an email as contact info."
  [person]
  (some #(= "Email" (:Type %))
        (get-in person [:ContactInformation :ContactMechanisms])))

(defn opendata-email-contact
  "Returns the opendata email contact info for the given collection, it is just the first email
  contact info found in the ContactPersons, ContactGroups or DataCenters."
  [collection]
  (let [{:keys [ContactPersons ContactGroups DataCenters]} collection
        contacts (concat ContactPersons ContactGroups (mapcat data-center/data-center-contacts DataCenters))
        email-contact (some #(when (email-contact? %) %) contacts)]
    (when email-contact
      (let [email (some #(when (= "Email" (:Type %)) (:Value %))
                        (get-in email-contact [:ContactInformation :ContactMechanisms]))
            email-contacts (when email [{:type :email :value email}])]
        {:first-name (:FirstName email-contact)
         :middle-name (:MiddleName email-contact)
         :last-name (:LastName email-contact)
         :roles (:Roles email-contact)
         :contacts email-contacts}))))

(defn related-url->opendata-related-url
  "Returns the opendata related url for the given collection related url"
  [related-url]
  (let [{:keys [Description Type Subtype URL]} related-url
        MimeType (get-in related-url [:GetService :MimeType])
        size (get-in related-url [:GetData :Size])]
    {:type Type
     :sub-type Subtype
     :url URL
     :description Description
     :mime-type MimeType
     :size size}))

(def doi-base-url
  "The base DOI URL."
  "https://doi.org")

(defn- doi->url
  "Converts a DOI into a URL if it is not already a URL."
  [doi]
  (if (re-matches #"http.*" doi)
    doi
    (format "%s/%s" doi-base-url doi)))

(defn publication-reference->opendata-reference
  "Returns an opendata reference for the given collection publication reference. Opendata only
  allows a string for a publication reference, so we'll use the DOI of the publication reference."
  [publication-reference]
  (when-let [doi (-> publication-reference :DOI :DOI)]
    (doi->url doi)))

(def citation-keys
  "Subset of keys to retrieve from collection-citations."
  [:Creator
   :DataPresentationForm
   :Editor
   :IssueIdentification
   :OtherCitationDetails
   :ReleasePlace
   :SeriesName])

(defn collection-citation->opendata-citation
  "Returns subset of collection-citation needed for opendata response."
  [collection-citation]
  (not-empty (select-keys collection-citation citation-keys)))
