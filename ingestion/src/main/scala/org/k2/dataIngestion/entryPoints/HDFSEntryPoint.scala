package org.k2.dataIngestion.entryPoints

import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.conf.Configuration

object HDFSEntryPoint {

  /**
    * HDFS's host url
    */
  final val host = "hdfs://192.168.1.164:8020"

  /**
    * Reference to the HDFS's file system
    */
  final val fileSystem: FileSystem = FileSystem.get(URI.create(host), new Configuration)

}
