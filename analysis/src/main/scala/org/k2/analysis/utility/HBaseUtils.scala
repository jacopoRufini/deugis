package org.k2.analysis.utility
import java.io._
import java.security.MessageDigest

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, CompareOperator, HBaseConfiguration, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.k2.analysis.models.{Geometry, Index}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Utility class used to interact with HBase
  */
class HBaseUtils extends Serializable {

  /**
    * Reference to the indexing table
    */
  @transient val INDEXING_TABLE = TableName.valueOf("indexing")

  /**
    * Reference to the geometry table
    */
  @transient val GEOMETRY_TABLE = TableName.valueOf("geometry")

  /**
    * Hbase configuration instance
    */
  @transient private val configuration = {
    val conf = HBaseConfiguration.create
    conf.set("hbase.zookeeper.quorum", "192.168.1.164")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf
  }

  /**
    * Hbase connection instance
    */
  @transient private val connection: Connection = ConnectionFactory.createConnection(configuration)

  /**
    * Hbase connector instance
    */
  @transient private val hbaseContext: HBaseContext = new HBaseContext(Spark.sparkContext, configuration)

  @transient private val sc: SparkContext = Spark.sparkContext
  @transient private val ss: SparkSession = Spark.sparkSession

  /**
    * Implicit Encoder[Geometry]
    */
  private implicit def enc1 = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[Geometry]

  /**
    * Implicit Encoder[Index]
    */
  private implicit def enc2 = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[Index]

  /**
    * Implicit Encoder[String]
    */
  private implicit def enc3 = Encoders.STRING

  /**
    * Reference to the indexing family
    */
  val INDEXING_FAMILY ="i".getBytes

  /**
    * Reference to the geometry family
    */
  val GEOMETRY_FAMILY = "g".getBytes

  /**
    * Object used to map Geometry fields to their corresponding names in HBase
    */
  val bytes = Map(
    "wkt" -> "w".getBytes,
    "class" -> "c".getBytes,
    "refs" -> "r".getBytes,
    "stats" -> "s".getBytes,
    "ids" -> "i".getBytes,
    "centroid" -> "b".getBytes,
    "envelope" -> "e".getBytes,
    "empty" -> "".getBytes
  )

  /**
    * Retrieves from HBase a Dataset[Geometry] given a list of ids
    * @param list  list of ids to be retrieved
    * @return  dataset obtained
    */
  def getGeometries(list: Array[Array[Byte]]): Dataset[Geometry] = {
    val rdd = sc.parallelize(list)
    val getRdd = hbaseContext.bulkGet[Array[Byte], Geometry](
      GEOMETRY_TABLE,
      2,
      rdd,
      record => new Get(record),
      (result: Result) => getResult(result))
    ss.createDataset[Geometry](getRdd)
  }

  /**
    * Retries geometries from HBase that satisfy a condition tag
    * @param rdd  rdd of ids that must be retrieved
    * @param key  tag key
    * @param value  tag value
    * @return  Dataset of Geometries that satisfy the condition
    */
  def extractFilteredWkts(rdd: RDD[Array[Byte]], filter: Filter) = {

    val getRdd = hbaseContext.bulkGet[Array[Byte], Geometry](
      GEOMETRY_TABLE,
      20000,
      rdd,
      record => {
        new Get(record)
          .addFamily(GEOMETRY_FAMILY)
          .setCacheBlocks(false)
          .setFilter(filter)
      },
      (result: Result) => getResult(result)
    )
    ss.createDataset[Geometry](getRdd)
  }

  /**
    * Reads the result object from Get operation and converts it to a Geometry
    * @param result  the object to be converted
    * @return a new Geometry object
    */
  private def getResult(result: Result): Geometry = {
    val keyValuesMap = mutable.Map[String, Any](
      "id" -> null,
      "c" -> null, //class
      "w" -> null, //wkt
      "tags" -> mutable.Map[String, String](),
      "b" -> null, //centroid
      "e" -> null, //envelope
      "s" -> null //stats
    )
    if (!result.isEmpty) {
      val iterator = result.listCells().iterator()
      keyValuesMap("id") = result.getRow.toSeq
      while (iterator.hasNext) {
        val cell = iterator.next()
        val colName = Bytes.toString(CellUtil.cloneQualifier(cell))
        val colVal = CellUtil.cloneValue(cell)
        if (keyValuesMap.contains(colName)) {
          var value: Any = null
          if (colName.equals("s")) value = deserialize(CellUtil.cloneValue(cell))
          else value = Bytes.toString(CellUtil.cloneValue(cell))
          keyValuesMap(colName) = value
        } else {
          keyValuesMap("tags") = keyValuesMap("tags").asInstanceOf[mutable.Map[String, String]] += (colName -> Bytes.toString(colVal))
        }

      }
      return Geometry(
        keyValuesMap("id").asInstanceOf[Seq[Byte]],
        keyValuesMap("c").asInstanceOf[String],
        keyValuesMap("w").asInstanceOf[String],
        keyValuesMap("tags").asInstanceOf[scala.collection.mutable.Map[String, String]],
        keyValuesMap("b").asInstanceOf[String],
        keyValuesMap("e").asInstanceOf[String],
        keyValuesMap("s").asInstanceOf[mutable.Map[String, ArrayBuffer[Seq[String]]]]
      )
    }
    null
  }

  /**
    * Utility used to scan the indexing table given n prefixes
    * @param prefixes  array of prefixes to be scanned
    * @return a dataset (geohash -> ids linked to the geohash)
    */
  def scanIndexByPrefixes(prefixes: Array[String]): Dataset[Index] = {
    val filter = new FilterList(FilterList.Operator.MUST_PASS_ONE)
    prefixes.foreach(e => filter.addFilter(new PrefixFilter(e.getBytes)))

    val sortedPrefixes = prefixes.sorted
    val head = sortedPrefixes.head.getBytes
    val str = sortedPrefixes.last
    val nextChar = (str.last+1).asInstanceOf[Char]
    val last = (str.substring(0, str.length-1) + nextChar).getBytes

    val scan = new Scan()
      .addFamily(INDEXING_FAMILY)
      .withStartRow(head)
      .withStopRow(last)
      .setFilter(filter)

    val scanRdd = hbaseContext.hbaseRDD(INDEXING_TABLE, scan).map(e => getIndexResult(e._2))

    ss.createDataset[Index](scanRdd)
  }

  /**
    * Reads the result object from Scan operation and converts it to a Index
    * @param result  the object to be converted
    * @return a new Index object
    */
  private def getIndexResult(result: Result): Index = {
    val keyValuesMap = scala.collection.mutable.Map[String, Any](
      "geohash" -> null,
      "ids" -> new ArrayBuffer[Array[Byte]]())
    if (!result.isEmpty) {
      val iterator = result.listCells().iterator()
      keyValuesMap("geohash") = Bytes.toString(result.getRow)
      while (iterator.hasNext) {
        val cell = iterator.next()
        keyValuesMap("ids") = deserialize(CellUtil.cloneValue(cell)).asInstanceOf[ArrayBuffer[Array[Byte]]]
      }
      return Index(
        keyValuesMap("geohash").asInstanceOf[String],
        keyValuesMap("ids").asInstanceOf[ArrayBuffer[Array[Byte]]]
      )
    }
    null
  }

  /**
    * Utility used to convert a string to sha1 hash
    * @param id  string to be hashed
    * @return sha1 hashed string
    */
  def sha1(id: String): Array[Byte] = {
    val md: MessageDigest = MessageDigest.getInstance("SHA1")
    md.digest(id.getBytes)
  }

  /**
    * Deserialize Array[Byte]
    * @param bytes  Array[Byte] to be deserialized
    * @return deserialized object
    */
  private def deserialize(bytes: Array[Byte]): Any = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close()
    value
  }
}
