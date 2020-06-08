package org.k2.analysis.utility

import java.util

import com.github.davidmoten.geo.GeoHash
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}
import org.locationtech.jts.io.{WKTReader, WKTWriter}

import scala.collection.JavaConversions._

/**
  * Static utilities
  */
object GeoUtils {

  private final val writer: WKTWriter  = new WKTWriter()

  private final val reader: WKTReader = new WKTReader()

  final val factory: GeometryFactory = new GeometryFactory()

  /**
   * Lunghezza minima richiesta affinchè il geohash sia valido
   */
  private val MIN_HASH_LENGTH: Int = 3

  /**
    * Converts a Geometry object to WKT string
    * @param geometry  object to be converted
    * @return a wkt string
    */
  def write(geometry: Geometry) = {
    writer.write(geometry)
  }

  /**
    * Converts a WKT string to a Geometry object
    * @param string  wkt to be converted
    * @return a Geometry object
    */
  def read(string: String) = {
    if (string == null) factory.createPoint()
    reader.read(string)
  }

  /**
   * Generate a geohash starting from a bounding box
   * @param env  the bounding box used to calculate geohashes
   * @return array of geohashes associated to the bounding box
   */
  def geohash(env: Envelope): Array[String] = {
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
   * Verifies that the set of hashes is valid
   * @param hashSet  set of geohashes
   * @return true if length of hashes > minimum required length of array
   */
  def acceptableHashes(hashSet: util.Set[String]): Boolean = {
    if (hashSet == null) return false
    for (hash <- hashSet) {
      if (hash.length < MIN_HASH_LENGTH) return false
    }
    true
  }

  /**
    * Converts the given string value of Eurostat to Int
    * @param s string to convert
    * @return value converted to Int
    */
  def toInt(s: String): Int = {
    try {
      s.replace(",", ".").toInt
    } catch {
      case e: Exception => 0
    }
  }

  /**
    * Converts the given string value of Eurostat to Double
    * @param s string to convert
    * @return value converted to Double
    */
  def toDouble(s: String): Double = {
    try {
      s.replace(",", ".").toDouble
    } catch {
      case e: Exception => 0.0
    }
  }

  /**
   * Creates buffer from point given a distance
   * @param lng  longitude of starting point
   * @param lat  latitude of starting point
   * @param meters  max distance from point to buffer
   * @return  a buffer represented as a bounding box
   */
  def createBuffer(lng: Double, lat: Double, meters: Int) = {
    val r = 6378137
    val dLat = meters.toDouble/r
    val dLon = meters/(r*Math.cos(Math.PI*lat/180))

    val p1 = new Coordinate(lng + dLon * 180/Math.PI, lat + dLat * 180/Math.PI)
    val p2 = new Coordinate(lng + dLon * 180/Math.PI, lat - dLat * 180/Math.PI)
    val p3 = new Coordinate(lng - dLon * 180/Math.PI, lat - dLat * 180/Math.PI)
    val p4 = new Coordinate(lng - dLon * 180/Math.PI, lat + dLat * 180/Math.PI)

    factory.createPolygon(Array(p1,p2,p3,p4,p1))
  }

  private def rad(coord: Double) = Math.toRadians(coord)

  /**
    * Calculate distance between two points
    * @param lat latitude starting point
    * @param lng longitude starting point
    * @param lng2 latitude ending point
    * @param lat2 longitude ending point
    * @return distance (km)
    */
  def calculateDistanceInKilometer(lng: Double, lat: Double, lng2:Double, lat2:Double): Double = {
    6371.01 * Math.acos(Math.sin(rad(lat)) * Math.sin(rad(lat2)) + Math.cos(rad(lat)) * Math.cos(rad(lat2)) * Math.cos(rad(lng) - rad(lng2)))
  }

}

