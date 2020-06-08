package org.k2.dataIngestion.transformations.abstractClasses

import org.apache.hadoop.hbase.TableName

/**
  * Class used to identify those classes which have to be mapped
  * @param fileName  where the raw data is located
  * @tparam O  O (Output) represents the type of the mappable dataset
  */
abstract class Mappable[O](protected var fileName: String) extends Sendable[O] {

  /**
    * Used to load and transform raw data
    * @return the instance of Mappable
    */
  protected def load(): Mappable[O]

  /**
    * Utility used to write the dataset on Hbase
    */
  protected def send(): Unit

  /**
    * Column family used to store geometries
    */
  final val GEOMETRY_FAMILY = "g".getBytes

  /**
    * Reference to geometry table
    */
  @transient final val GEOMETRY_TABLE: TableName = TableName.valueOf("gis_geometry")

  @transient final val job = createJob(GEOMETRY_TABLE)
}