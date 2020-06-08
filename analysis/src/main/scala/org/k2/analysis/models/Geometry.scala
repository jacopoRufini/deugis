package org.k2.analysis.models

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class Geometry(id: Seq[Byte],
                    classType: String,
                    wkt: String,
                    tags: scala.collection.Map[String, String],
                    centroid: String,
                    envelope: String,
                    stats: mutable.Map[String, ArrayBuffer[Seq[String]]]) {

  def getStat(key: String): ArrayBuffer[Seq[String]] = {
    try {
      this.stats(key)
    } catch {
      case e: Exception => {
        new ArrayBuffer[Seq[String]]()
      }
    }
  }
}

