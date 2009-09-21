package edu.berkeley.cs.scads.nodes

import edu.berkeley.cs.scads.thrift.StorageEngine
import org.apache.log4j.Logger

/**
 * Helper class for talking to scads storage engines.  It provides the following features:
 * <ul>
 *   <li>Encapuslation of host/port/syncport connection info for serialization, comparison, etc</li>
 *   <li>Creation / pooling of connections with optional xtrace configured by the system property</li>
 * </ul>
 */
case class StorageNode(host: String, thriftPort: Int, syncPort: Int) {
  val logger = Logger.getLogger("scads.storagenode")
	def this(host: String, port: Int) = this(host, port, port)
	def syncHost = host + ":" + syncPort

  /**
   * Checkout a connection (or create one if none are free) from the connection pool and pass it to the provided function.
   * When the function is complete the connection is returned to the pool.
   * If an error occurs the connection is thrown away as we can not be sure if it is still in a good state.
   */
  def useConnection[ReturnType](f: StorageEngine.Client => ReturnType): ReturnType = {
    ConnectionPool.useConnection(this, f)
  }

  /**
   * Checkout (or create) a connection from the pool.  For use when you don't intend to return the connection to the pool.
   */
  def createConnection() = {
    ConnectionPool.checkoutConnection(this)
  }
}
