package org.k2.analysis.functions

import java.io.Serializable

import org.apache.hadoop.hbase.CompareOperator
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.spark.sql.Encoders
import org.k2.analysis.utility.{GeoUtils, HBaseUtils}
import org.apache.spark.sql.functions._

class FindLocality(private val hbase: HBaseUtils) extends Serializable  {

  /**
    * Implicit encoder Array[Byte]
    */
  private implicit def enc1 = Encoders.BINARY

  /**
   *
   * @param lng longitude of the start point where the buffer will be created
   * @param lat latitude of the start point where the buffer will be created
   * @param conditionKey tag key to search
   * @param conditionValue tag value to search
   * @param distance  distance used to create the buffer starting from (lat,lng) point
   * @return dataset of geometries found inside buffer that have the given tag with the given value
   */
  def findByTags(lng: Double, lat: Double, conditionKey: String, conditionValue:String, distance: Int) = {
    val buffer = GeoUtils.createBuffer(lng, lat, distance)
    val geohashes = GeoUtils.geohash(buffer.getEnvelopeInternal)

    val ids = hbase
      .scanIndexByPrefixes(geohashes)
      .select(explode(col("ids")).as("id"))
      .as[Array[Byte]]

    val filter = new SingleColumnValueFilter(hbase.GEOMETRY_FAMILY, conditionKey.getBytes, CompareOperator.NOT_EQUAL, conditionValue.getBytes)
    filter.setFilterIfMissing(true)

    hbase
      .extractFilteredWkts(ids.rdd, filter)
      .filter(g => buffer.contains(GeoUtils.read(g.wkt)))
  }
}

