package org.k2.dataIngestion.models

case class RawRelation(id: Long, relWkt: Seq[String], relType: String, tags: Seq[(String, String)])