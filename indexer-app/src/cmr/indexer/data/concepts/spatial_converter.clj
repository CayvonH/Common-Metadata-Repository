(ns cmr.indexer.data.concepts.spatial-converter
  "Contains functions cartesian spatial shape to densified geodetic ones."
  (:require
   [cmr.common.services.errors :as errors]
   [cmr.spatial.line-segment :as line-segment]
   [cmr.spatial.line-string :as line-string]
   [cmr.spatial.point :as point]
   [cmr.spatial.polygon :as polygon]
   [cmr.spatial.ring-relations :as ring-relations])
  (:import
   (cmr.spatial.line_string LineString)
   (cmr.spatial.mbr Mbr)
   (cmr.spatial.point Point)
   (cmr.spatial.polygon Polygon)))

(def ^:private ^Double densification-dist
  0.1)

(defn- adjust-pole
  "Geodetic polygon cannot be on pole."
  [^Point p]
  (letfn [(adjust-point-lat [adjusted-lat]
            (point/point (.lon p) adjusted-lat (.geodetic_equality p)))]
    (case (int (.lat p))
      90 (adjust-point-lat 89.9999)
      -90 (adjust-point-lat -89.9999)
      p)))

(defn- points->densified-points
  "Take two or more points treat them as line segments and densify them."
  [points]
  (mapcat (fn [idx]
            (let [densified-points
                  (line-segment/densify-line-segment
                   (line-segment/line-segment
                    (adjust-pole (get points idx))
                    (adjust-pole (get points (+ idx 1))))
                   densification-dist
                   true)]
              ;; Remove overlapping points
              (if (= idx 0)
                densified-points
                (rest densified-points))))
          (range (- (count points) 1))))

(defmulti shape->geodetic-shape
  (fn [source-coordinate-system shape]
    (class shape)))

(doseq [shape-type [Point Mbr]]
  (defmethod shape->geodetic-shape shape-type
    [_ shape]
    shape))

(defmethod ^LineString shape->geodetic-shape LineString
  [source-coordinate-system ^LineString line]
  (if (= :geodetic source-coordinate-system)
    line
    (line-string/line-string
     :geodetic
     (points->densified-points (:points line)))))

(defmethod ^Polygon shape->geodetic-shape Polygon
  [source-coordinate-system ^Polygon poly]
  (if (= :geodetic source-coordinate-system)
    poly
    (polygon/polygon
     :geodetic
     (mapv #(->> (:points %)
                 points->densified-points
                 (ring-relations/ring :geodetic))
           (:rings poly)))))

(defmethod shape->geodetic-shape :default
  [unknown]
  (errors/internal-error! (str "Do not know how to transform shape [" (type unknown) "] to geodetic")))
