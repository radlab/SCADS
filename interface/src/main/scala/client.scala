package edu.berkeley.cs.scads.thrift

import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.log4j.Logger

import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TProtocol, TBinaryProtocol, XtBinaryProtocol}

class ConnectionPoolManager[ClientType](implicit manifest : scala.reflect.Manifest[ClientType]) {
	val logger = Logger.getLogger("scads.connectionpool")
	val clientConstructor = manifest.erasure.getConstructor(classOf[TProtocol])
	val pools = new scala.collection.mutable.HashMap[(String, Int), ConcurrentLinkedQueue[ClientType]]

	def getPool(host: String, port:Int): ConcurrentLinkedQueue[ClientType] = {
		synchronized {
			pools.get((host, port)) match {
				case Some(pool) => pool
				case None => {
					val newPool = new ConcurrentLinkedQueue[ClientType]
					pools.put((host, port), newPool)
					newPool
				}
			}
		}
	}

	def newClient(host: String, port: Int): ClientType = {
	  val transport = new TFramedTransport(new TSocket(host, port))
    val protocol = if (System.getProperty("xtrace")!=null) {new XtBinaryProtocol(transport)} else {new TBinaryProtocol(transport)}
    transport.open()
 		clientConstructor.newInstance(protocol).asInstanceOf[ClientType]
	}
}

class ThriftClient[ClientType](host: String, port: Int, cpm: ConnectionPoolManager[ClientType]) {
	val pool = cpm.getPool(host, port)

	def useConnection[ReturnType](func: ClientType => ReturnType): ReturnType = {
		val conn = pool.poll() match {
			case null => cpm.newClient(host, port)
			case c => c
		}
		val result = func(conn)
		pool.add(conn)
		return result
	}
}

object StorageNodePool extends ConnectionPoolManager[StorageEngine.Client]
case class StorageNode(host: String, port: Int) extends ThriftClient(host, port, StorageNodePool)
