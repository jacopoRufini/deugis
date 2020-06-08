package org.k2.dataIngestion.models

case class Stat(id: String, stats: Seq[Map[String, Seq[String]]])
