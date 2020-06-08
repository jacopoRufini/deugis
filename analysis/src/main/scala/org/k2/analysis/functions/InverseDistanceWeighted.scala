package org.k2.analysis.functions

import java.io.Serializable

import org.apache.hadoop.hbase.CompareOperator
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.spark.sql.Encoders
import org.k2.analysis.models.{Index, Interpolated}
import org.k2.analysis.utility.{GeoUtils, HBaseUtils, Spark}
import org.locationtech.jts.geom.{GeometryFactory, Point, Polygon}
import org.apache.spark.sql.functions._

class InverseDistanceWeighted(private val hbase: HBaseUtils) extends Serializable  {

  @transient private final val sc = Spark.sparkContext

  private val accuWeigthZW = sc.doubleAccumulator("weigthzw")
  private val accuWeigthSW = sc.doubleAccumulator("weigthsw")

  /**
    * Implicit encoder Interpolated
    */
  private implicit def enc1 = Encoders.product[Interpolated]

  /**
    * Implicit encoder Array[Byte]
    */
  private implicit def enc2 = Encoders.BINARY

  /**
    * Finds geometries with stats included in buffer
    * @param lng  longitude of the starting point of buffer
    * @param lat  latitude of the starting point of buffer
    * @param distance  distance from the point to calculate the buffer
    * @param stat  stat used as costraint
    * @param year  year of the stat
    * @return  a dataset of Interpolated values
    */
  def findByStats(lng: Double, lat: Double, distance: Int, stat:String, year:String) = {
    val buffer = GeoUtils.createBuffer(lng, lat, distance)
    val geohashes = GeoUtils.geohash(buffer.getEnvelopeInternal)

    val ids = hbase
      .scanIndexByPrefixes(geohashes)
      .select(explode(col("ids")).as("id"))
      .as[Array[Byte]]

    val filter = new SingleColumnValueFilter(hbase.GEOMETRY_FAMILY, stat.getBytes, CompareOperator.NOT_EQUAL, "".getBytes)
    filter.setFilterIfMissing(true)

    hbase
      .extractFilteredWkts(ids.rdd, filter)
      .filter(g => buffer.contains(GeoUtils.read(g.wkt)))
      .map(row => {
        val centroidGeom = GeoUtils.read(row.centroid).asInstanceOf[Point]
        val distance = GeoUtils.calculateDistanceInKilometer(centroidGeom.getX, centroidGeom.getY, lng, lat)
        val singleStats = row.getStat(stat)

        for (s <- singleStats) {
          if (s.contains(year)) {
            val value = GeoUtils.toDouble(s(1))
            Interpolated(distance, value)
          }
        }
        Interpolated(distance, 0)
      })
  }

  /**
   *
   * @param lng  longitude of the point to be interpolated
   * @param lat  latitude of the point to be interpolated
   * @param distance  distance from the point to calculate the buffer
   * @param stat  stat used as costraint
   * @param year  year of the stat
   * @return tuple containing longitude, latitude and the interpolated value of the point
   */

  def doInterpolation(lng:Double, lat:Double, distance:Int, stat:String, year:String) = {
    findByStats(lng,lat, distance, stat, year).foreach(row => {
      val distance = if (row.distance == 0) row.value else row.distance
      val w = 1.0/scala.math.pow(distance,1)
      accuWeigthSW.add(w)
      accuWeigthZW.add(row.value * w)
    })

    (lng,lat,accuWeigthZW.sum/accuWeigthSW.sum)
  }
}
