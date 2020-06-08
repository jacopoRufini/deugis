package org.k2.dataIngestion.models

case class RawNode(id: Long, longitude: Double, latitude: Double, tags: Seq[(String, String)])