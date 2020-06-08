package org.k2.dataIngestion.models

case class Relation(ready: Boolean, relationId: String, tags: Seq[(String, String)], geometryClass: String, geohash: Array[String], centroid: String, envelope: String, wkt: String)