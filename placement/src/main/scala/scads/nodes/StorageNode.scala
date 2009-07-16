package edu.berkeley.cs.scads.nodes

import edu.berkeley.cs.scads.thrift.StorageEngine
import org.apache.log4j.Logger

case class StorageNode(host: String, thriftPort: Int, syncPort: Int) {
  val logger = Logger.getLogger("scads.storagenode")
	def this(host: String, port: Int) = this(host, port, port)
	def syncHost = host + ":" + syncPort

  def useConnection[ReturnType](f: StorageEngine.Client => ReturnType): ReturnType = {
    ConnectionPool.useConnection(this, f)
  }

  def createConnection() = {
    ConnectionPool.checkoutConnection(this)
  }
}