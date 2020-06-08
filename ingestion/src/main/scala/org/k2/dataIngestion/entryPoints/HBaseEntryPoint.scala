package org.k2.dataIngestion.entryPoints

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory

object HBaseEntryPoint {

  /**
    * HBase's host url
    */
  final val host = "192.168.1.164"

  /**
    * Configuration used to connect with HBase
    */
  val configuration = {
      val conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.quorum", host)
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf
  }

  /**
    * Connection used to communicate with HBase
    */
  val connection = ConnectionFactory.createConnection(configuration)
}
