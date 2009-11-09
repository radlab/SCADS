package edu.berkeley.cs.scads.nodes

import edu.berkeley.cs.scads.thrift.StorageEngine
import org.apache.log4j.Logger

abstract class AbstractServiceNode[Client] extends ThriftService {
    val host:String
    val thriftPort:Int
    val syncPort:Int

    val logger = Logger.getLogger("scads.storagenode")
	def syncHost = host + ":" + syncPort

    def getHost = host
    def getThriftPort = thriftPort
    def getSyncPort = syncPort

    val connectionPool : AbstractConnectionPool[Client]

  /**
   * Checkout a connection (or create one if none are free) from the connection pool and pass it to the provided function.
   * When the function is complete the connection is returned to the pool.
   * If an error occurs the connection is thrown away as we can not be sure if it is still in a good state.
   */
  def useConnection[ReturnType](f: Client => ReturnType): ReturnType = {
    connectionPool.useConnection(this, f)
  }

  /**
   * Checkout (or create) a connection from the pool.  For use when you don't intend to return the connection to the pool.
   */
  def createConnection():Client = {
    connectionPool.checkoutConnection(this)
  }

}

/**
 * Helper class for talking to scads storage engines.  It provides the following features:
 * <ul>
 *   <li>Encapuslation of host/port/syncport connection info for serialization, comparison, etc</li>
 *   <li>Creation / pooling of connections with optional xtrace configured by the system property</li>
 * </ul>
 */
case class StorageNode(host: String, thriftPort: Int, syncPort: Int) extends AbstractServiceNode[StorageEngine.Client] {
    def this(h:String, p:Int) = this(h,p,p)
    val connectionPool = ConnectionPool
}
