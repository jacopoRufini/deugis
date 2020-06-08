package org.k2.dataIngestion.transformations.abstractClasses

import java.util

import com.github.davidmoten.geo.GeoHash
import org.apache.hadoop.hbase.client.Put
import org.locationtech.jts.geom.Envelope
import scala.collection.JavaConversions._

/**
  * Class used to identify those classes which have to be mapped and represent 2D geometries (ways and polygons)
  * @param fileName  where the raw data is located
  * @tparam I  I (Input) represent the type of raw dataset
  * @tparam O  O (Output) represents the type of the mappable dataset
  */
abstract class MappableGeometry2D[I,O](fileName: String) extends MappableGeometry[I,O](fileName) {

  /**
    * Possible classes that can be assigned to a geometry
    */
  val classes = Map(
    "GeometryCollection" -> "gc",
    "LineString" -> "ls",
    "Point" -> "p",
    "Polygon" -> "pl",
    "MultiLineString" -> "ml",
    "MultiPoint" -> "mp",
    "MultiPolygon" -> "mg",
    "LinearRing" -> "lr"
  )

  /**
    * Minimum size that the array of hashes must have to be valid
    */
  private val MIN_HASH_LENGTH: Int = 3

  /**
    * Finds the Array[geohash] of a given geometry
    * @param env  bounding box of the geometry
    * @return  Array of geohashes
    */
  @Override protected def geohash(env: Envelope) = {
    var hash: util.Set[String] = null
    var maxHashes = 1
    do {
      try {
        hash = GeoHash.coverBoundingBoxMaxHashes(env.getMaxY, env.getMinX, env.getMinY, env.getMaxX, maxHashes).getHashes
      } catch {
        case e: Exception =>
      }
      maxHashes += 1
    } while (!acceptableHashes(hash))
    hash.toSet.toArray
  }

  /**
    * Checks if the array of hashes is valid
    * @param hashSet set of hashes
    * @return if the array is valid
    */
  protected def acceptableHashes(hashSet: util.Set[String]): Boolean = {
    if (hashSet == null) return false
    for (hash <- hashSet) {
      if (hash.length < MIN_HASH_LENGTH) return false
    }
    true
  }

  /**
    * Utility used to fill a 2DGeometry Row with metadata
    * @param put  object representing the row
    * @param centroid  centroid of the geometry
    * @param envelope  bounding box of the geometry
    * @param wkt  wkt string which represents the structure of the geometry
    * @param geometryClass  the class assigned to the geometry
    * @param tags  tags linked to the geometry
    */
  protected def fillWithMetadata(put: Put, centroid: String, envelope: String, wkt: String, geometryClass: String, tags: Seq[(String, String)]) = {
    put.addColumn(GEOMETRY_FAMILY, bytes("centroid"), centroid.getBytes)
    put.addColumn(GEOMETRY_FAMILY, bytes("envelope"), envelope.getBytes)
    put.addColumn(GEOMETRY_FAMILY, bytes("wkt"), wkt.getBytes)
    put.addColumn(GEOMETRY_FAMILY, bytes("geometryClass"), geometryClass.getBytes)
    fillWithTags(put, tags)
  }

}

