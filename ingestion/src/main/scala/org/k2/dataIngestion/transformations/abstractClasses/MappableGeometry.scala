package org.k2.dataIngestion.transformations.abstractClasses

import org.apache.hadoop.hbase.client.Put
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{Dataset, Encoder, Row}
import org.k2.dataIngestion.entryPoints.HDFSEntryPoint

/**
  * Class used to identify those classes which have to be mapped and represent general geometries
  * @param fileName  where the raw data is located
  * @tparam I  I (Input) represent the type of raw dataset
  * @tparam O  O (Output) represents the type of the mappable dataset
  */
abstract class MappableGeometry[I, O](fileName: String) extends Mappable[O](fileName) {

  /**
    * Constant prefix used to edit OSM ids which are not unique between different geometry types
    */
  protected val prefix: String

  /**
    * The name of the geometry
    */
  protected val geometryName: String

  /**
    * Useless fields contained in raw data which can be easily dropped
    */
  protected val droppable = Array("version", "timestamp", "changeset", "uid", "user_sid")

  /**
    * Utility used to read raw data of a mappable geometry
    * @param path  where raw data is located
    * @return readed dataset
    */
  protected def read(path: String) = ss.read.parquet(path).drop(droppable: _*)

  /**
    * Utility used to distinguish different parquet of a file (node, way, relation)
    * @return the path of the request geometry
    */
  protected def getPath = () => s"${HDFSEntryPoint.host}/gis/pbf/$fileName/$fileName.pbf.$geometryName.parquet"

  /**
    * Represents the implicit encoder of raw data
    */
  protected implicit def rawEncoder: Encoder[I]

  /**
    * Represents the implicit encoder of transformed data
    */
  protected implicit def finalEncoder: Encoder[O]

  /**
    * Utility used to create a column for every tag contained in geometry
    * @param put  put object to be filled
    * @param tags  array of tags linked to the geometry
    */
  protected def fillWithTags(put: Put, tags: Seq[(String, String)]) = {
    tags.foreach(tag => {
      put.addColumn(GEOMETRY_FAMILY, tag._1.getBytes, tag._2.getBytes)
    })
  }

  /**
    * Utility used to isolate the indexing part of a mappable dataset
    * @return transformed dataset
    */
  def extract(): Dataset[Row] = {
    this.dataset
      .select(
        col(s"${geometryName}Id").as("id"),
        explode(col("geohash")).as("geohash")
      )
  }
}


