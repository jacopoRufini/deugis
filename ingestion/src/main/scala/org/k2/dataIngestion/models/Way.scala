package org.k2.dataIngestion.models

case class Way(ready: Boolean, wayId: String, tags: Seq[(String, String)], geometryClass: String, geohash: Array[String], centroid: String, envelope: String, wkt: String)
