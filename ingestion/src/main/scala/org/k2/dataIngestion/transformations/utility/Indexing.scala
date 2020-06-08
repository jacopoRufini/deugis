package org.k2.dataIngestion.transformations.utility

import org.apache.hadoop.hbase.TableName
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.k2.dataIngestion.models.Index
import org.k2.dataIngestion.transformations.abstractClasses.Sendable
import org.k2.dataIngestion.transformations.mappers.{NodeMapper, RelationMapper, WayMapper}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

/**
  * Class used to obtain the indexing table
  * @param nm  reference to mapper of nodes
  * @param wm  reference to mapper of ways
  * @param rm  reference to mapper of relations
  */
class Indexing(private val nm: NodeMapper, private val wm: WayMapper, private val rm: RelationMapper) extends Sendable[Index] {

  /**
    * Column family used to store indexes
    */
  final val INDEXING_FAMILY = "i".getBytes

  /**
    * Reference to indexing table
    */
  @transient final val INDEXING_TABLE = TableName.valueOf("gis_indexing")

  @Override @transient val job = createJob(INDEXING_TABLE)

  @Override var dataset: Dataset[Index] = _

  @Override protected implicit def finalEncoder: Encoder[Index] = Encoders.product[Index]

  /**
    * Extracts the indexing table using nodes, ways and relations
    * @return the indexing table
    */
  def getIndexingTable= {
    this.dataset = nm.extract()
      .union(wm.extract())
      .union(rm.extract())
      .groupBy("geohash")
      .agg(collect_list("id").as("ids"))
      .as[Index]

    this
  }

  /**
    * Utility to write the partial indexing table on HDFS
    * @param dataset  dataset to be saved as parquet file
    * @param path  where the dataset is saved
    */
  def store(dataset: Dataset[Row], path: String)= {
    dataset.write.mode(SaveMode.Overwrite).parquet(path)
  }

  /**
    * Utility to read the partial indexing table from HDFS
    * @param path  from where the dataset has to be loaded
    * @return dataset loaded from path
    */
  def load(path: String) = {
    ss.read.format("parquet").load(path)
  }

  /**
    * Utility used to write the dataset on Hbase
    */
  def send = {
    val data = this.dataset.rdd.map((row: Index) => {
      val put = new Put(row.geohash.getBytes)
      val list = row.ids.map(sha1)
      put.addColumn(INDEXING_FAMILY, bytes("ids"), Serialization.serialize(list))
      (new ImmutableBytesWritable(), put)
    })

    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
