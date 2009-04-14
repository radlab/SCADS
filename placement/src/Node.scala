import SCADS.RecordSet
import SCADS.Record
import SCADS.ConflictPolicy

import org.apache.thrift.TException
import org.apache.thrift.TProcessor
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TServerTransport
import org.apache.thrift.transport.TServerSocket
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportFactory
import org.apache.thrift.transport.TTransportException
import org.apache.thrift.server.TServer
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.protocol.TBinaryProtocol

case class Node(h: String, p: Int) {
	val host: String = h
	val port: Int = p
}

abstract class StorageNode(h: String, p: Int) extends Node(h,p) {
	def get(namespace: String, key: String): Record
	def get_set(namespace: String, keys: RecordSet): java.util.List[Record]
	def put(namespace: String, rec: Record): Boolean
	def get_responsibility_policy(namespace: String): RecordSet

	def sync_set(ns: String, rs: RecordSet, host:String, policy: ConflictPolicy): Boolean
	def copy_set(ns: String, rs: RecordSet, host:String): Boolean
	def remove_set(ns: String, rs: RecordSet): Boolean
	
}

/**
* Use Thrift to connect over socket to a host:port.
* Intended for clients to connect to servers.
*/
trait ThriftConnection {
	def host: String
	def port: Int
	
	protected val transport = new TSocket(host, port)
	protected val protocol = new TBinaryProtocol(transport)
}

/**
* Use Thrift to start a server listening on a socket at localhost:port.
*/
trait ThriftServer {
	def port: Int
	def processor: TProcessor

	private val serverTransport = new TServerSocket(port)
	private val protFactory = new TBinaryProtocol.Factory(true, true)
	private val server = new TThreadPoolServer(processor, serverTransport,protFactory)
	
	def start = {
		try {
			println("starting server on "+port)
			server.serve
		} catch { case x: Exception => x.printStackTrace }		
	}
	def stop = {
		server.stop // doesn't do shit
	}
}

class StorageThriftNode(host: String, port: Int) extends StorageNode(host,port) with ThriftConnection {
	val client = new SCADS.Storage.Client(protocol)
	
	def get(namespace: String, key: String): Record = {
		transport.open
		try {
			val rec = client.get(namespace,key)
			transport.close
			rec
		} catch { 
		    case x: Exception => { transport.close; null }
		}
	}
	def get_set(namespace: String, keys: RecordSet): java.util.List[Record] = {
		transport.open
		try {
			val recs = client.get_set(namespace,keys)
			transport.close
			recs
		} catch { 
		    case x: Exception => { transport.close; null }
		}
	}
	
	def put(namespace: String, rec: Record): Boolean = {
		transport.open
		try {
			val success = client.put(namespace,rec)
			transport.close
			success
		} catch { 
		    case x: Exception => { transport.close; false }
		}
	}
	
	def get_responsibility_policy(namespace: String): RecordSet = {
		transport.open
		try {
			val recs = client.get_responsibility_policy(namespace)
			transport.close
			recs
		} catch { 
			case x: Exception => { transport.close; null }
		}
	}
	
	def sync_set(ns: String, rs: RecordSet, host:String, policy: ConflictPolicy): Boolean = {
		transport.open
		try {
			val success = client.sync_set(ns,rs,host,policy)
			transport.close
			success
		} catch { 
			case x: Exception => { transport.close; false }
		}
	}
	def copy_set(ns: String, rs: RecordSet, host:String): Boolean = {
		transport.open
		try {
			val success = client.copy_set(ns,rs,host)
			transport.close
			success
		} catch { 
			case x: Exception => { transport.close; false }
		}
	}
	def remove_set(ns: String, rs: RecordSet): Boolean = {
		transport.open
		try {
			val success = client.remove_set(ns,rs)
			transport.close
			success
		} catch { 
			case x: Exception => { transport.close; false }
		}
	}
}