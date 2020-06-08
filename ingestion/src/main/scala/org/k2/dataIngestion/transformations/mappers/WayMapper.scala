package org.k2.dataIngestion.transformations.mappers

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.k2.dataIngestion.models.{Node, RawWay, Way, WayMetadata}
import org.apache.spark.sql.{Dataset, Encoders}
import org.k2.dataIngestion.transformations.abstractClasses.MappableGeometry2D
import org.k2.dataIngestion.transformations.utility.GeoUtils
import org.locationtech.jts.geom.{Coordinate, Geometry}
import org.apache.spark.sql.functions._

/**
  * Mapper representing a Way (Line or Polygon)
  * @param fileName  where the raw data is located
  * @param nodes reference to the mapped nodes used to resolve joins
  */
class WayMapper(fileName: String, nodes: Dataset[Node]) extends MappableGeometry2D[RawWay, Way](fileName) {

  @Override protected val geometryName = "way"

  @Override var dataset: Dataset[Way] = _

  @Override val prefix = "w"

  @Override protected implicit def rawEncoder = Encoders.product[RawWay]

  @Override protected implicit def finalEncoder = Encoders.product[Way]

  /**
    * Calculates the metadata associated to the geometry
    * @param seq  sequence of node wkts forming the way
    * @return metadata associated to the geometry
    */
  private def metadata(seq: Seq[String]): WayMetadata = {
    try {
      val coordsArray: Array[Coordinate] = seq.map(e => GeoUtils.read(e).getCoordinate).toArray

      val geometry: Geometry =
        if (isClosed(coordsArray)) {
          GeoUtils.factory.createPolygon(coordsArray)
        } else GeoUtils.factory.createLineString(coordsArray)

      WayMetadata(
        geometry.getGeometryType,
        GeoUtils.write(geometry),
        GeoUtils.write(geometry.getCentroid),
        GeoUtils.write(geometry.getEnvelope),
        geometry.getEnvelopeInternal
      )
    } catch {
      case _:Exception => null
    }
  }

  /**
    * Checks if the way is closed (So, it's a polygon)
    * @param array  array of coordinates which forms the way
    * @return true if the way is a polygon
    */
  private def isClosed(array: Array[Coordinate]): Boolean = {
    array(0).equals(array.last)
  }

  @Override def load() = {
    val input = read(getPath())

    this.dataset = input
      .withColumn("nodes", explode(col("nodes")))
      .join(nodes.drop("geometryClass", "geohash", "tags"),
        concat(lit("n"), col("nodes.nodeId"))
          .equalTo(col("nodeId")))
      .orderBy("id","nodes.index")
      .groupBy("id", "tags")
      .agg(collect_list("nodeWkt").as("wayWkt"))
      .as[RawWay]
      .map(row => {
        val data = metadata(row.wayWkt)
        if (data == null) {
          Way(false, prefix + row.id, row.tags, null, null, null, null, null)
        } else {
          val hash = geohash(data.envelopeObject)
          Way(true, prefix + row.id, row.tags, classes(data.geometryClass), hash, data.centroid, data.envelopeString, data.wkt)
        }
      })
      .filter(_.ready)

    this
  }

  @Override def send() = {
    val data = dataset.rdd.map((row: Way) => {
      val put = new Put(sha1(row.wayId))
      fillWithMetadata(put, row.centroid, row.envelope, row.wkt, row.geometryClass, row.tags)
      (new ImmutableBytesWritable(), put)
    })

    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}