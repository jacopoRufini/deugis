package org.k2.analysis.utility

import org.apache.spark.sql.SparkSession

object Spark {

  /**
    * Spark session reference
    */
  @transient val sparkSession = SparkSession.builder.appName("Analysis").getOrCreate()

  /**
    * Spark context reference
    */
  @transient val sparkContext = sparkSession.sparkContext
}
