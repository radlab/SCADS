package edu.berkeley.cs.scads.nodes

import edu.berkeley.cs.scads.thrift.{KnobbedDataPlacementServer,StorageEngine}
import org.apache.log4j.Logger
import org.apache.thrift.transport.{TFramedTransport, TSocket}

import org.apache.thrift.protocol.{TProtocol,TBinaryProtocol}
import org.apache.thrift.protocol.XtBinaryProtocol

abstract class ObjectPool[PoolType] {
  def borrowObject(): PoolType
  def returnObject(o: PoolType)
}

class SimpleObjectPool[PoolType](c: () => PoolType) extends ObjectPool[PoolType] {
  val logger = Logger.getLogger("connectionPool")
  val pool = new scala.collection.mutable.ArrayStack[PoolType]
  val creator = c

  def borrowObject(): PoolType = {
    synchronized {
      if(pool.isEmpty) {
        c()
     }
     else {
       pool.pop
     }
    }
  }

  def returnObject(o: PoolType) {
    synchronized {
      pool.push(o)
    }
  }
}

object ConnectionPool extends AbstractConnectionPool[StorageEngine.Client] {
    val creator = (protocol:TProtocol) => { new StorageEngine.Client(protocol) }
}

//object DataPlacementConnectionPool extends AbstractConnectionPool[KnobbedDataPlacementServer.Client] {
//    val creator = (protocol:TProtocol) => { new KnobbedDataPlacementServer.Client(protocol) }
//}

trait ThriftService {
    def getHost:String
    def getThriftPort:Int
    def getSyncPort:Int
}

abstract class AbstractConnectionPool[T] {
  val logger = Logger.getLogger("connectionPool")
  val connections = new scala.collection.mutable.HashMap[ThriftService, SimpleObjectPool[T]]
  val creator: (TProtocol) => T

  def createConnection(node: ThriftService):T  = {
    val transport = new TFramedTransport(new TSocket(node.getHost, node.getThriftPort))
    val protocol = if (System.getProperty("xtrace")!=null) {new XtBinaryProtocol(transport)} else {new TBinaryProtocol(transport)}
    val client = creator(protocol)
    transport.open()
    logger.info("New connection opened to " + node)
    return client
  }

  def findOrCreatePool(node: ThriftService): SimpleObjectPool[T] = {
    connections.get(node) match {
      case Some(p)=> p
      case _ => {
        val p = new SimpleObjectPool[T](() => createConnection(node))
        connections.put(node, p)
        p
      }
    }
  }

  def checkoutConnection(node: ThriftService) =
    findOrCreatePool(node).borrowObject()

  def useConnection[ReturnType](node: ThriftService, f: T => ReturnType): ReturnType = {
    val conn = checkoutConnection(node)
    assert(conn != null)

    val ret = f(conn)
    connections(node).returnObject(conn)
    return ret
  }
}
