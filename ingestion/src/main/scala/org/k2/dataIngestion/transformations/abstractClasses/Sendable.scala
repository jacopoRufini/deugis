package org.k2.dataIngestion.transformations.abstractClasses

import java.security.MessageDigest
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.Dataset
import org.k2.dataIngestion.entryPoints.{HBaseEntryPoint, SparkEntryPoint}

/**
  * Used to identify those classes which are writable on Hbase
  * @tparam O  O (Output) represents the type of the writable dataset
  */
@SerialVersionUID(100L)
abstract class Sendable[O] extends Serializable {

  @transient protected final val ss = SparkEntryPoint.sparkSession
  @transient protected final val sc = SparkEntryPoint.sparkContext

  /**
    * Reference to the dataset associated to the mapper
    */
  var dataset: Dataset[O]

  /**
    * Job that's used to write rows on Hbase
    */
  protected val job: Job

  /**
    * Map used to identify the right column names on Hbase
    */
  protected val bytes = Map(
    "refs" -> "r".getBytes,
    "stats" -> "s".getBytes,
    "ids" -> "i".getBytes,
    "wkt" -> "w".getBytes,
    "geometryClass" -> "c".getBytes,
    "centroid" -> "b".getBytes,
    "envelope" -> "e".getBytes
  )

  /**
    * Configures the job before it is launched
    * @param table  the table that will be populated
    * @return the configurated job
    */
  protected def createJob(table: TableName) = {
    val job = Job.getInstance(HBaseEntryPoint.configuration)
    job.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, table.getNameAsString)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    job
  }

  /**
    * Utility to obtain the sha-1 hash of a string
    * @param id  the id that has to be hashed
    * @return sha-1 hash
    */
  protected def sha1(id: String) = {
    MessageDigest.getInstance("SHA-1").digest(id.getBytes)
  }
}
