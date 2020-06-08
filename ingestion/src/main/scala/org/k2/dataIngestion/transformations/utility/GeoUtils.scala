package org.k2.dataIngestion.transformations.utility

import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.locationtech.jts.io.{WKTReader, WKTWriter}

/**
  * Utility used to manipulate spatial data
  */
@SerialVersionUID(100L)
object GeoUtils extends Serializable {

  private final val writer: WKTWriter  = new WKTWriter()

  private final val reader: WKTReader = new WKTReader()

  final val factory: GeometryFactory = new GeometryFactory()

  def write(geometry: Geometry) = {
    writer.write(geometry)
  }

  def read(string: String) = {
    reader.read(string)
  }

}
