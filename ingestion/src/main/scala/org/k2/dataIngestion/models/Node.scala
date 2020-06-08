package org.k2.dataIngestion.models

case class Node(geometryClass: String, geohash: Array[String], nodeId: String, nodeWkt: String, tags: Seq[(String, String)])
