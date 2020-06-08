package org.k2.dataIngestion.transformations.mappers

import com.github.davidmoten.geo.GeoHash
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.{Dataset, Encoders}
import org.k2.dataIngestion.models.{Node, RawNode}
import org.k2.dataIngestion.transformations.abstractClasses.MappableGeometry
import org.k2.dataIngestion.transformations.utility.GeoUtils
import org.locationtech.jts.geom.CoordinateXY

/**
  * Mapper representing a Node (Point)
  * @param fileName  where the raw data is located
  */
class NodeMapper(fileName: String) extends MappableGeometry[RawNode,Node](fileName) {

  /**
    * hard-coded geometry class for a node (Point)
    */
  val geometryClass = "p"

  @Override protected val geometryName = "node"

  @Override protected implicit def rawEncoder = Encoders.product[RawNode]

  @Override protected implicit def finalEncoder = Encoders.product[Node]

  @Override var dataset: Dataset[Node] = _

  @Override val prefix = "n"

  /**
    * Finds the wkt associated to a couple (longitude, latitude)
    * @param lon  longitude
    * @param lat  latitude
    * @return wkt associated to the point
    */
  private def wkt(lon: Double, lat: Double): String = {
    GeoUtils.write(
      GeoUtils.factory.createPoint(
        new CoordinateXY(lon, lat)))
  }

  /**
    * Finds the geohash associated to a Point
    * @param lon  longitude
    * @param lat  latitude
    * @returnthe geohash associated to the Point
    */
  private def geohash(lon: Double, lat: Double): Array[String] = {
    Array(GeoHash.encodeHash(lat, lon))
  }

  @Override def load() = {
    val input = read(getPath()).as[RawNode]

    this.dataset = input.map(row => {
      val hash = geohash(row.longitude, row.latitude)
      val nodeWkt = wkt(row.longitude, row.latitude)
      Node(geometryClass, hash, prefix + row.id, nodeWkt, row.tags)
    })

    this
  }

  @Override def send() = {
    val data = dataset
      .filter(e => e.tags.nonEmpty).rdd.map((row: Node) => {
      val put = new Put(sha1(row.nodeId))
      put.addColumn(GEOMETRY_FAMILY, bytes("wkt"), row.nodeWkt.getBytes)
      put.addColumn(GEOMETRY_FAMILY, bytes("geometryClass"), row.geometryClass.getBytes)
      fillWithTags(put, row.tags)
      (new ImmutableBytesWritable(), put)
    })

    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
