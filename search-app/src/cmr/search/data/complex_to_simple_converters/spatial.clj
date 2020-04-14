(ns cmr.search.data.complex-to-simple-converters.spatial
  "Contains converters for spatial condition into the simpler executable conditions"
  (:require
   [clojure.string :as str]
   [cmr.common.services.errors :as errors]
   [cmr.common-app.services.search.complex-to-simple :as c2s]
   [cmr.common-app.services.search.elastic-search-index :as idx]
   [cmr.common-app.services.search.group-query-conditions :as gc]
   [cmr.common-app.services.search.query-model :as qm]
   [cmr.orbits.orbits-runtime :as orbits]
   [cmr.search.services.query-helper-service :as query-helper]
   [cmr.spatial.derived :as d]
   [cmr.spatial.mbr :as mbr]
   [cmr.spatial.relations :as sr]
   [cmr.spatial.serialize :as srl])
  (:import
   cmr.spatial.cartesian_ring.CartesianRing
   cmr.spatial.geodetic_ring.GeodeticRing
   cmr.spatial.line_string.LineString
   cmr.spatial.mbr.Mbr
   cmr.spatial.point.Point
   cmr.spatial.polygon.Polygon))

(defn- br->cond
  [prefix {:keys [west north east south] :as br}]
  (letfn [(add-prefix [field]
                      (->> field name (str prefix "-") keyword))
          (range-cond [field from to]
                      (qm/numeric-range-condition (add-prefix field) from to))
          (bool-cond [field value]
                     (qm/boolean-condition (add-prefix field) value))]
    (if (mbr/crosses-antimeridian? br)
      (let [c (range-cond :west -180 west)
            d (range-cond :east -180 east)
            e (range-cond :east east 180)
            f (range-cond :west west 180)
            am-conds (gc/and-conds [(bool-cond :crosses-antimeridian true)
                                    (gc/or-conds [c f])
                                    (gc/or-conds [d e])])
            lon-cond (gc/or-conds [(range-cond :west -180 east)
                                   (range-cond :east west 180)
                                   am-conds])]
        (gc/and-conds [lon-cond
                       (range-cond :north south 90)
                       (range-cond :south -90 north)]))

      (let [north-cond (range-cond :north south 90.0)
            south-cond (range-cond :south -90.0 north)
            west-cond (range-cond :west -180 east)
            east-cond (range-cond :east west 180)
            am-conds (gc/and-conds [(bool-cond :crosses-antimeridian true)
                                    (gc/or-conds [west-cond east-cond])])
            non-am-conds (gc/and-conds [west-cond east-cond])]
        (gc/and-conds [north-cond
                       south-cond
                       (gc/or-conds [am-conds non-am-conds])])))))

(defn- resolve-shape-type
  "Convert the 'type' string from a serialized shape to one of :point, :line, :br, or :polygon.
  These are used by the orbits wrapper library."
  [type]
  (cond
    (re-matches #".*line.*" type) :line
    (re-matches #".*poly.*" type) :polygon
    :else (keyword type)))

(defn- lat-lon-crossings-valid?
  "Returns true only if the latitude/longitude range array returned from echo-orbits has
  longitudes (it always has latitudes)"
  [lat-lon-crossing-ranges]
  ;; If the back-tracking doesn't find a valid range it returns and empty vector for the longitudes,
  ;; but this is paired with the latitudes that were originally sent in.
  (seq (last (first lat-lon-crossing-ranges))))

(defn- orbit-crossings
  "Compute the orbit crossing ranges (max and min longitude) for a single collection
  used to create the crossing conditions for orbital crossing searches.
  The stored-ords parameter is a vector of coordinates (longitude/latitude) of the points for
  the search area (as returned by the shape->stored-ords method of the spatial library.
  The orbit-params paraemter is the set of orbit parameters for a single collection.
  Returns a vector of vectors of doubles representing the ascending and descending crossing ranges."
  [context mbr stored-ords orbit-params]
  ;; Use the orbit parameters to perform orbital back tracking to longitude ranges to be used
  ;; in the search.
  (let [shape-type (resolve-shape-type (name (:type (first stored-ords))))
        coords (map srl/stored->ordinate (:ords (first stored-ords)))
        lat-range [(:south mbr) (:north mbr)]
        orbits-runtime (get-in context [:system orbits/system-key])]
    (let [{:keys [swath-width
                  period
                  inclination-angle
                  number-of-orbits
                  start-circular-latitude]} orbit-params
          start-circular-latitude (or start-circular-latitude 0)
          area-crossing-range-params {:lat-range lat-range
                                      :geometry-type shape-type
                                      :coords coords
                                      :inclination inclination-angle
                                      :period period
                                      :swath-width swath-width
                                      :start-clat start-circular-latitude
                                      :num-orbits number-of-orbits}]
      (when (and shape-type
                 (seq coords))
        (let [asc-crossing (orbits/area-crossing-range
                            orbits-runtime
                            (assoc area-crossing-range-params :ascending? true))
              desc-crossing (orbits/area-crossing-range
                             orbits-runtime
                             (assoc area-crossing-range-params :ascending? false))]
          (when (or (lat-lon-crossings-valid? asc-crossing)
                    (lat-lon-crossings-valid? desc-crossing))
            [asc-crossing desc-crossing]))))))

(defn- range->numeric-range-intersection-condition
  "Create a condtion to test for a numberic range intersection with multiple ranges."
  [ranges]
  (gc/or-conds
    (map (fn [[start-lat end-lat]]
           (qm/numeric-range-intersection-condition
             :orbit-start-clat
             :orbit-end-clat
             start-lat
             end-lat))
         ranges)))

(defn- crossing-ranges->condition
  "Create a search condition for a given vector of crossing ranges."
  [crossing-ranges]
  (gc/or-conds
    (map (fn [[range-start range-end]]
           (qm/numeric-range-condition
             :orbit-asc-crossing-lon
             range-start
             range-end))
         crossing-ranges)))

(defn- lat-lon-crossings-conditions
  "Create the seacrh conditions for a latitude-range / equator crosssing longitude-range returned
  by echo-orbits"
  [context lat-ranges-crossings ascending?]
  (let [orbits-runtime (get-in context [:system orbits/system-key])]
    (gc/or-conds
     (map (fn [lat-range-lon-range]
            (let [lat-range (first (first lat-range-lon-range))
                  [asc-lat-ranges desc-lat-ranges] (orbits/denormalize-latitude-range
                                                    orbits-runtime
                                                    (first lat-range)
                                                    (last lat-range))
                  lat-ranges (if ascending? asc-lat-ranges desc-lat-ranges)
                  lat-conds (range->numeric-range-intersection-condition lat-ranges)
                  crossings (last lat-range-lon-range)]
              (gc/and-conds
               [lat-conds
                (crossing-ranges->condition crossings)])))
          lat-ranges-crossings))))

(defn- orbital-condition
  "Create a condition that will use orbit parameters and orbital back tracking to find matches
  to a spatial search."
  [context shape]
  (let [mbr (sr/mbr shape)
        {:keys [query-collection-ids]} context
        orbit-params (query-helper/collection-orbit-parameters context query-collection-ids true)
        stored-ords (srl/shape->stored-ords shape)
        crossings-map (reduce (fn [memo params]
                                (let [lon-crossings-lat-ranges (orbit-crossings context mbr stored-ords params)]
                                  (if (seq lon-crossings-lat-ranges)
                                    (assoc
                                      memo
                                      (:concept-id params)
                                      lon-crossings-lat-ranges)
                                    memo)))
                              {}
                              orbit-params)]
    (when (seq crossings-map)
      (gc/or-conds
        (map (fn [collection-id]
               (let [[asc-crossings-lat-ranges desc-crossings-lat-ranges]
                     (get crossings-map collection-id)]
                 (gc/and-conds
                   [(qm/string-condition :collection-concept-id collection-id, true, false)
                    (gc/or-conds
                      [;; ascending
                       (lat-lon-crossings-conditions context asc-crossings-lat-ranges true)
                       ;; descending
                       (lat-lon-crossings-conditions context desc-crossings-lat-ranges false)])])))

             (keys crossings-map))))))

(defn- point->elastic-point
  "point to vector of [lon, lat]"
  [^Point point]
  [(.lon point) (.lat point)])

(defmulti shape->geo-condition class)

(defn- point-cond
  "Create a point condition from the given point."
  [^Point point]
  (qm/map->GeoshapeCondition
   {:type :point
    :field :geometries
    :coordinates (point->elastic-point point)
    :relation :intersects}))

(defn- envelope-cond
  "Create an envelope condition with the given extents."
  [west north east south]
  (qm/map->GeoshapeCondition
   {:type :envelope
    :field :geometries
    :coordinates [[west north] [east south]]
    :relation :intersects}))

(defmethod shape->geo-condition Point
  [^Point point]
  ;; Point is on antimeridian. and-cond for -180 and 180.
  (if (= 180 (-> point .lon int Math/abs))
    (gc/or-conds
     [(point-cond point)
      (point-cond (update point :lon (fn [& args] (* -1 (.lon point)))))])
    (point-cond point)))

(defmethod shape->geo-condition Polygon
  [^Polygon polygon]
  (qm/map->GeoshapeCondition
   {:type :polygon
    :field :geometries
    :orientation :counterclockwise
    :coordinates (into [] (for [ring (:rings polygon)]
                            (mapv point->elastic-point (:points ring))))
    :relation :intersects}))

(defmethod shape->geo-condition Mbr
  [^Mbr mbr]
  (envelope-cond (.west mbr) (.north mbr) (.east mbr) (.south mbr)))

(defmethod shape->geo-condition LineString
  [^LineString line-string]
  (qm/map->GeoshapeCondition
   {:type :linestring
    :field :geometries
    :coordinates (mapv point->elastic-point (:points line-string))
    :relation :intersects}))

(defmethod shape->geo-condition :default
  [unknown]
  (errors/internal-error! (str "Do not know how to create geo condition for geometry [" (type unknown) "]")))

(defn- geo-condition
  "Create a geo condition from the supplied CMR spatial lib shape."
  [shape]
  (gc/or-conds
   (remove nil?
           [(shape->geo-condition shape)
            ;; If the shape contains a pole we will force elasticsearch to
            ;; treat all longitudes at the pole as equivalent by drawing a
            ;; a bounding box across the -180, 180 longitude range.
            (when (sr/contains-north-pole? shape)
              (envelope-cond -180 90 180 90))
            (when (sr/contains-south-pole? shape)
              (envelope-cond -180 -90 180 -90))])))

(extend-protocol c2s/ComplexQueryToSimple
  cmr.search.models.query.SpatialCondition
  (c2s/reduce-query-condition
    [{:keys [shape]} context]
    (let [shape (d/calculate-derived shape)
          spatial-geo-cond (geo-condition shape)
          orbital-cond (when (= :granule (:query-concept-type context))
                         (orbital-condition context shape))
          spatial-cond spatial-geo-cond]
      (if orbital-cond
        (gc/or-conds [spatial-cond orbital-cond])
        spatial-cond))))
