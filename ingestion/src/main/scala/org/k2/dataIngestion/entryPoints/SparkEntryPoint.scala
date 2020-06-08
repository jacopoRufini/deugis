package org.k2.dataIngestion.entryPoints

import org.apache.spark.sql.SparkSession

@SerialVersionUID(100L)
object SparkEntryPoint extends Serializable {

  @transient val sparkSession = SparkSession.builder.appName("DataIngestion").getOrCreate()

  @transient val sparkContext = sparkSession.sparkContext

  /**
    * Used to ready binary type as string from parquet
    */
  sparkSession.sqlContext.setConf("spark.sql.parquet.binaryAsString","true")

}