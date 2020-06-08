package org.k2.analysis.functions

import java.io.Serializable
import org.k2.analysis.models.Geometry
import org.k2.analysis.utility.{GeoUtils, HBaseUtils, Spark}
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class EventAnalyzer(private val hbase: HBaseUtils) extends Serializable {

  /**
    * Maps hashed ids to the original value
    */
  val mapByteId: mutable.Map[Seq[Byte], String] = mutable.Map[Seq[Byte], String]()

  /**
    * Maps OSM id to the well known locality name
    */
  val mapIdName: mutable.Map[String, String] = mutable.Map[String, String]()

  /**
   * Returns N geometries on a stat for every year passed as param
   * @param arrID the ids of the geometries
   * @param stat the name of the stat
   * @return A map {countryName : [value2010, value2011, value2012...value2019]
   */
  def multiCompareStat(arrID: Array[String], stat: String): mutable.Map[String, ArrayBuffer[Double]] = {
    val result = mutable.Map[String, ArrayBuffer[Double]]()
    val ids = arrID.map(id => {
      result(mapIdName(id)) = ArrayBuffer[Double]()
      hbase.sha1(id)
    })
    val geometries = hbase.getGeometries(ids).rdd.collect()
    geometries.foreach(geom => {
      val id = mapIdName(mapByteId(geom.id))
      val stats = geom.getStat(stat)
      stats.foreach(seq => {
        result(id).append(GeoUtils.toDouble(seq(1)))
      })
    })
    result
  }

  /**
   * Gets geometries that matches the filter
   * @param name the name of the stat
   * @param operation the operation to apply between the param value and the stat value
   * @param value the input value to compare with the stat value
   * @param year the year from which take the stat value
   * @return ( (name, operation, value, year), map[nameCountry: valueOfTheStat] )
   */
  def getStatFromAll(name: String, operation: String, value: Double, year: Int): ((String, String, Double, Int),mutable.Map[String, Double]) = {
    val result = mutable.Map[String, Double]()
    val ids = mapIdName.keys.map(key => hbase.sha1(key))
    val geometries = hbase.getGeometries(ids.toArray)
    val filtered = geometries.filter(geom => filter(geom, name, operation, value, year))
    filtered.rdd.collect.foreach(geom => {
      val yearStr = year.toString
      val index = GeoUtils.toInt(yearStr.substring(yearStr.length - 1))
      val statVal = GeoUtils.toDouble(geom.getStat(name).get(index)(1))
      val id = mapIdName(mapByteId(geom.id))
      result(id) = statVal
    })
    ((name, operation, value, year), result)
  }

  /**
   * Filter function for comparing a geometry on its year-stat value with the value passed as param
   * @param geom the geometry from which take the year-stat value
   * @param name the name of the stat
   * @param operation the operation to apply between the param value and the stat value
   * @param value the input value to compare with the stat value
   * @param year the year from which take the stat value
   * @return true if the year-stat match the value according to the operation
   */
  def filter(geom: Geometry, name: String, operation: String, value: Double, year: Int): Boolean = {
    geom.getStat(name).foreach(tupla => {
      val tuplaYear = GeoUtils.toInt(tupla.head)
      val tuplaValue = GeoUtils.toDouble(tupla(1))
      if (year == 0 || tuplaYear == year) {
        operation match {
          case ">" => return tuplaValue > value
          case "<" => return tuplaValue < value
          case "=" => return tuplaValue == value
        }
      }
    })
    false
  }

  /**
   * Gets the values in the range of years for the stat of a certain geometry
   * @param id the geometry from which take the stat values
   * @param name the name of the stat
   * @param startYear the starting year for the stat (inclusive)
   * @param endYear the ending year for the stat (inclusive)
   * @return map { year: value }
   */
  def getStatInRange(id: String, name: String, startYear: Int, endYear: Int): (String, mutable.Map[String, Double]) = {
    val result = mutable.Map[String, Double]()
    val geom = hbase.getGeometries(Array(hbase.sha1(id)))
    val country = mapIdName(id)
    geom.rdd.collect.foreach(geometry => {
      val stat = geometry.getStat(name)
      if (stat.nonEmpty) {
        stat.foreach(tupla => {
          val year = GeoUtils.toInt(tupla.head)
          val value = GeoUtils.toDouble(tupla(1))
          if (year >= startYear && year <= endYear) {
            result(tupla.head) = value
          }
        })
      }
    })
    (country, result)
  }

  def getStatsInRange(id: String, names: Array[String], yearStart: Int, yearEnd: Int): ( String, mutable.Map[String, mutable.Map[String, Double]]) = {
    val result = mutable.Map[String, mutable.Map[String, Double]]()
    val geometries = hbase.getGeometries(Array(hbase.sha1(id)))
    geometries.rdd.collect.foreach(geom => {
      names.foreach(statName => {
        val yearsMap = mutable.Map[String, Double]()
        geom.getStat(statName).foreach(arr => {
          val statYear = GeoUtils.toInt(arr.head)
          if (statYear >= yearStart && statYear <= yearEnd) yearsMap(arr.head) = GeoUtils.toDouble(arr(1))
        })
        result(statName) = yearsMap
      })
    })
    (mapIdName(id), result)
  }

  /**
    * Loads map that associates hashed ids to their original name
    */
  def loadMapperId(): Unit = {
    val dsMap = Spark.sparkSession.read.format("csv").option("header","true").load("hdfs://192.168.1.164:8020/gis/stats/name_to_id.csv")
    dsMap.rdd.collect.foreach(row => {
      val id = s"r${row.getAs[Long]("id")}"
      val name = row.getAs[String]("name")
      mapByteId(hbase.sha1(id).toSeq) = id
      mapIdName(id) = name
    })
  }

}
