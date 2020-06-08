package org.k2.dataIngestion.transformations.mappers

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.k2.dataIngestion.models._
import org.k2.dataIngestion.transformations.abstractClasses.MappableGeometry2D
import org.k2.dataIngestion.transformations.utility.GeoUtils
import org.locationtech.jts.geom._
import org.locationtech.jts.operation.polygonize.Polygonizer
import scala.collection.JavaConversions._

/**
  * Mapper representing a Relation (Logistic or concrete relation between 2 or more geometries)
  * @param fileName  where the raw data is located
  * @param nodes reference to the mapped nodes used to resolve joins
  * @param ways reference to the mapped ways used to resolve joins
  */
class RelationMapper(fileName: String, nodes: Dataset[Node], ways: Dataset[Way]) extends MappableGeometry2D[RawRelation, Relation](fileName) {

  /**
    * Set of the valid relations that will be taken
    */
  private val validRelations = Set(
    "multipolygon",
    "route",
    "boundary",
    "site",
    "public_transport",
    "street",
    "waterway"
  )

  @Override var dataset: Dataset[Relation] = _

  @Override val prefix = "r"

  @Override protected val geometryName = "relation"

  @Override protected implicit def rawEncoder = Encoders.product[RawRelation]

  @Override protected implicit def finalEncoder = Encoders.product[Relation]

  /**
    * Extracts the type of relation
    */
  private val typeValue = udf((tags: Seq[Row]) => {
    try tags.find(e => e(0).equals("type")).get.getString(1)
    catch {
      case _: Exception => "unknown"
    }
  })

  /**
    * Calculates the metadata associated to the geometry
    * @param seq  sequence of geometries wkts forming the relation
    * @param relType  relation type of the geometry
    * @return metadata associated to the geometry
    */
  private def metadata(seq: Seq[String], relType: String): RelationMetadata = {
    val geometries: Array[Geometry] = seq.map(GeoUtils.read).toArray

    var geometry: Geometry =
      if (relType.equals("multipolygon") || relType.equals("boundary")) {
        getGeometry(geometries)
      } else {
        try {
          new GeometryCollection(geometries, GeoUtils.factory)
        } catch {
          case _: Exception => null
        }
      }

    if (geometry == null) return null

    RelationMetadata(
      geometry.getGeometryType,
      GeoUtils.write(geometry),
      GeoUtils.write(geometry.getCentroid),
      GeoUtils.write(geometry.getEnvelope),
      geometry.getEnvelopeInternal
    )
  }

  @Override private def getGeometry(geometries: Array[Geometry]): Geometry = {
    try {
      val polygonizer: Polygonizer = new Polygonizer
      geometries.foreach(geometry => {
        if (!geometry.getGeometryType.equals("Point")) {
          polygonizer.add(geometry)
        }
      })

      if (polygonizer.getPolygons.size() == 1) {
        GeoUtils.factory.createPolygon(polygonizer.getGeometry.getCoordinates)
      }

      for (e <- polygonizer.getPolygons) {
        val polygon = e.asInstanceOf[Polygon]
        val coordinates = polygon.getCoordinates
        val interiorSize = polygon.getNumInteriorRing

        if (interiorSize == 0) {
          return GeoUtils.factory.createPolygon(coordinates)

        } else {
          val (exterior: LinearRing, interior: Array[LinearRing]) = {
            val arr = new Array[LinearRing](interiorSize)
            for (i <- 0 until interiorSize) {
              arr(i) = polygon.getInteriorRingN(i).asInstanceOf[LinearRing]
            }
            (polygon.getExteriorRing.asInstanceOf[LinearRing], arr)
          }
          return GeoUtils.factory.createPolygon(exterior, interior)
        }
      }
    } catch {
      case e: Exception =>
    }
    null
  }

  @Override def load() = {
    val input = read(getPath())

    this.dataset = input
      .withColumn("relType", typeValue(col("tags")))
      .withColumn("members", explode(col("members")))
      .filter(row => {
        val geometryType = row.getAs[String]("relType")
        val memberType = row.getAs[Row]("members").getString(2)
        validRelations.contains(geometryType) && !memberType.startsWith("R")
      })
      .join(nodes.drop("geometryClass", "geohash", "tags"),
        concat(lit("n"), col("members.id")).equalTo(col("nodeId")),"left"
      )
      .join(ways.drop("ready", "geohash", "tags", "centroid", "envelope", "geometryClass"),
        concat(lit("w"), col("members.id")).equalTo(col("wayId")), "left"
      )
      .groupBy("id", "tags", "relType")
      .agg(
        collect_list(
          when(col("nodeWkt").isNotNull, col("nodeWkt")).
            otherwise(col("wkt"))).as("relWkt")
      )
      .as[RawRelation]
      .map(row => {
        val data = metadata(row.relWkt, row.relType)
        if (data == null) {
          Relation(false, prefix + row.id, row.tags, null, null, null, null, null)
        } else {
          Relation(true, prefix + row.id, row.tags, classes(data.geometryClass), geohash(data.envelopeObject), data.centroid, data.envelopeString, data.wkt)
        }
      })
      .filter(_.ready)

    this
  }

  @Override def send() = {
    val data = dataset.rdd.map((row: Relation) => {
      val put = new Put(sha1(row.relationId))
      fillWithMetadata(put, row.centroid, row.envelope, row.wkt, row.geometryClass, row.tags)
      (new ImmutableBytesWritable(), put)
    })

    data.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
