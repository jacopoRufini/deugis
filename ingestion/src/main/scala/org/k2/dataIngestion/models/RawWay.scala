package org.k2.dataIngestion.models

case class RawWay(id: Long, wayWkt: Seq[String], tags: Seq[(String, String)])
