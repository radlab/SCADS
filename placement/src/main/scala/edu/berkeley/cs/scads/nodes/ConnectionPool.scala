package edu.berkeley.cs.scads.nodes

import edu.berkeley.cs.scads.thrift.StorageEngine
import org.apache.log4j.Logger
import org.apache.thrift.transport.{TFramedTransport, TSocket}

import org.apache.thrift.protocol.TBinaryProtocol

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

object ConnectionPool {
  val logger = Logger.getLogger("connectionPool")
  val connections = new scala.collection.mutable.HashMap[StorageNode, SimpleObjectPool[StorageEngine.Client]]

  def createConnection(node: StorageNode):StorageEngine.Client  = {
    val transport = new TFramedTransport(new TSocket(node.host, node.thriftPort))
    val protocol = new TBinaryProtocol(transport)
    val client = new StorageEngine.Client(protocol)
    transport.open()
    logger.info("New connection opened to " + node)
    return client
  }

  def findOrCreatePool(node: StorageNode): SimpleObjectPool[StorageEngine.Client] = {
    connections.get(node) match {
      case Some(p)=> p
      case _ => {
        val p = new SimpleObjectPool[StorageEngine.Client](() => createConnection(node))
        connections.put(node, p)
        p
      }
    }
  }

  def checkoutConnection(node: StorageNode) =
    findOrCreatePool(node).borrowObject()

  def useConnection[ReturnType](node: StorageNode, f: StorageEngine.Client => ReturnType): ReturnType = {
    val conn = checkoutConnection(node)
    assert(conn != null)
    
    val ret = f(conn)
    connections(node).returnObject(conn)
    return ret
  }
}