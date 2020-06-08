package org.k2.dataIngestion.transformations.mappers

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.k2.dataIngestion.entryPoints.HDFSEntryPoint
import org.k2.dataIngestion.models.Stat
import org.k2.dataIngestion.transformations.abstractClasses.Mappable
import org.k2.dataIngestion.transformations.utility.Serialization

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Mapper representing European stats
  * @param fileName  where the csv (id -> name of the city) is located
  * @param statsPath  directory where are stored all the stats about europe data
  */
class StatMapper(fileName: String, statsPath: String) extends Mappable[Stat](fileName) {

  @Override protected implicit def finalEncoder = Encoders.product[Stat]

  /**
    * Reference to dataset (id -> name of the city)
    */
  private val nameToId: Dataset[Row] = read(s"${HDFSEntryPoint.host}/$fileName")

  val schema: StructType = {
    new StructType()
      .add("id", StringType)
      .add("stats", MapType(
        StringType,
        ArrayType(StringType)
      ))
  }

  @Override
  var dataset: Dataset[Stat] = _

  /**
    * Array of files which contain stats
    */
  private val stats = HDFSEntryPoint.fileSystem.listStatus(new Path(statsPath)).map(_.getPath.toString)

  /**
    * Utility used to read a csv file from HDFS
    * @param path  where the file is located on HDFS
    * @return reference to readed dataset
    */
  private def read(path: String) = ss.read.format("csv").option("header", "true").load(path)

  private def buildMap(list: Seq[Map[String, Seq[String]]]) = {
    val myMap = mutable.Map[String, ArrayBuffer[Seq[String]]]()
    list.foreach(map => {
      map.keys.foreach(key => {
        if (!myMap.contains(key)) {
          myMap(key) = new ArrayBuffer[Seq[String]]()
        }
        myMap(key) += map(key)
      })
    })
    myMap
  }

  @Override def load(): StatMapper = {
    val a = read(stats(0))
      .join(nameToId,
        col("name").equalTo(col("CITIES")),"right"
      ).drop("CITIES")
      .filter("id is not null")
      .map(row => {
        val id = row.getAs[String]("id")
        val time = row.getAs[String]("TIME")
        val value = row.getAs[String]("Value")
        val indic = row.getAs[String]("INDIC_UR")
        val stats = Map(indic -> Array(time,value))
        Row(id, stats)
      }) (RowEncoder.apply(schema))
      .groupBy("id")
      .agg(collect_list("stats").as("stats"))
      .as[Stat]

    a.show

    this
  }

  @Override protected def send() = {
    val hbasePuts = dataset.rdd.map((row: Stat) => {
      val put = new Put(sha1(s"r$row.id"))
      val map = buildMap(row.stats)
      put.addColumn(GEOMETRY_FAMILY, bytes("stats"), Serialization.serialize(map))
      (new ImmutableBytesWritable(), put)
    })

    hbasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}