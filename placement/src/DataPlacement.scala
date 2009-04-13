import SCADS.RecordSet
import SCADS.Record

import org.apache.thrift.TException
import org.apache.thrift.TProcessor
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TServerTransport
import org.apache.thrift.transport.TServerSocket
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportFactory
import org.apache.thrift.transport.TTransportException
import org.apache.thrift.server.TServer
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.protocol.TBinaryProtocol

import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

import java.io._
import java.net.{InetAddress,ServerSocket,Socket,SocketException}


/* CLUSTER MEMBERSHIP ----------------------------------------------- */


/* DATA PLACEMENT ---------------------------------------------------- */

class DataPlacementSocket(port: Int) extends SimpleDataPlacement {
	val serverSocket = new ServerSocket(port)
	
	/*
	def start_listening = {
		this.start
		while(true) {
			val clientSocket = serverSocket.accept()
			this !
		}
	}
	
	
	def act = {
		while (true) {
			receive {
				case AddNode(node) => {
					
				}	
			}
		}
	}
	*/
}

class SimpleDataPlacement extends DataPlacement {
	var nodes = new MutableNodeMap

	override def add_node(node: Node) = {
		node match {
			case n:StorageThriftNode => nodes.add(n)
		}
	}
	override def remove_node(node: Node) = {
		node match {
			case n:StorageThriftNode => nodes.remove(n)
		}
	}
	override def get_map: NodeMap = {
		nodes
	}
	override def ask_responsibility(node: Node, namespace: String): RecordSet = {
		node match {
			case n:StorageThriftNode => val rs = n.get_responsibility_policy(namespace); rs
		}
	}
	override def set_responsibility(node: Node, namespace: String, records: RecordSet) = {
		node match {
			case n:StorageThriftNode => {}
		}
	}
	
}

abstract class DataPlacement {
	/* State */
	def nodes: NodeMap		// map of nodes to their record responsibilities for each namespace
	
	
	/* Methods */
	def add_node(node: Node)	// start an entry in the node map with blank responsibility
	def remove_node(node: Node)	// remove all map entries for this node
	def get_map: NodeMap		// return a copy of the NodeMap; used by the Client Library to update itself
	
	def set_responsibility(node: Node, namespace: String, records: RecordSet) 	// assign a set of records to a node
	def ask_responsibility(node: Node, namespace: String): RecordSet 	// ask a node what its responsibility is
	/*
	def move(source: Node, target: Node, namespace: String, records: RecordSet) // move a responsibility from one node to another
	def copy(source: Node, target: Node, namespace: String, records: RecordSet) // copy a responsibility from one node to another	
	*/
}
abstract class DatePlacementPeer extends DataPlacement { // (?)
	/* State */
	def finger_table: Map[DataPlacement, RecordSet]	// map of other data placements and what record sets they manage
	
	/* Methods */
	def locate(records: RecordSet): Map[DataPlacement,RecordSet]	// which data placement	knows about these records
}

/* HELPER CLASSES ------------------------------------------------------ */

case class Node(h: String, p: Int) {
	val host: String = h
	val port: Int = p
}

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

trait ThriftConnection {
	def host: String
	def port: Int
	
	protected val transport = new TSocket(host, port)
	protected val protocol = new TBinaryProtocol(transport)

}

class StorageThriftNode(host: String, port: Int) extends Node(host,port) with ThriftConnection {
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
}

/*

// partial implementation -------------------------------

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

// keep connections to storage nodes open?
case class Node(h: String, p: Int) {
	val host: String = h
	val port: Int = p

	val transport = new TSocket(host, port)
	val protocol = new TBinaryProtocol(transport)
	val node_client = new SCADS.Storage.Client(protocol)
	
	def open = {
		transport.open()
	}
	def close ={
		transport.close()
	}
	def client ={
		node_client
	}
	//override def equals(other: Any): Boolean = {
	//	other.isInstanceOf[Node] && other.asInstanceOf[Node].host==this.host && other.asInstanceOf[Node].port==this.port
	//}
}

// for read-only use of Map
trait NodeMap {
	var nodes: Map[Node,Map[String,SCADS.RecordSet]]

	// find nodes responsible for this key in this String
	def lookup(ns: String, key: String): List[Node] = {
		var found_nodes = new HashSet[Node]
		nodes.foreach({ case (node,ns_rs) => {
			val node_rs = ns_rs.get(ns)
			if (this.inSet(key,node_rs)) {found_nodes += node}
		} })
		found_nodes.toList
	}
	
	def inSet(key: String, rset: SCADS.RecordSet): Boolean = {
		if (key >= rset.range.start_key && key <= rset.range_end_key) { return true }
		else { return false }
	}
	
	def set_cover(ns: String, rset: SCADS.RecordSet): Map[Node,SCADS.RangeSet] = {
		// TODO: this should be intelligent, e.g. min nodes to query or optimize for response time
		nodes.foreach({ case (node,ns_rs)=> {
			val node_rs = ns_rs.get(ns)
			null
			// TODO	
		} })
	}

}

// for read-write use of Map
trait NodeMapManager extends NodeMap {
	def add_node(host: String, port: Int) = {
		nodes += ( new Node(host,port) -> null )
	}    
	def remove_node(host: String, port: Int) = {
		//val to_remove = nodes.filterKeys(n=>n.host==host && n.port==port)
		// TODO 
	}
	def remove_node(host: String) = {
		//val to_remove = nodes.filterKeys(n=>n.host==host)
		// TODO
	}
}


abstract class DataPlacement extends NodeMapManager {


	def copy(ns: String, source: Node, target: Node, resp: SCADS.RecordSet): Boolean = {
		source.open
		target.open
		
		val success = false
		try {
			success = source.client.copy_set(ns,resp,target)
			source.close
			target.close
		} catch {
			case e:NotImplemented => e.printStackTrace()
			source.close
			target.close
		}
		success
	}
	def move(ns: String, source: Node, target: Node, resp: SCADS.RecordSet): Boolean = {
		source.open
		target.open
		
		val success_copy = true
		val success_remove = true
		try {
			success_copy=source.client.copy_set(ns,resp,target)
			success_move=source.client.remove(ns,resp)
			source.close
			target.close
		} catch {
			case e:NotImplemented => e.printStackTrace()
			source.close
			target.close
		}
		success_copy && success_move
	}
	def get_responsibility(ns: String, node: Node): SCADS.RecordSet = {
		// TODO
	}
	def set_responsibility(ns: String, node: Node) = {
		// TODO 
	}
	def ask_responsibility(ns: String, node: Node): SCADS.RecordSet = {
		node.open
		node.client.get_responsibility_policy(ns)
		node.close
	}	
	
	// return the whole map on inquiry
	def find(ns: String, key: String): Map[Node,SCADS.RecordSet] = {
		this.nodes
	}
	
}	


abstract class ClientLibrary(host: String, port: Int) extends NodeMap {

	val dp = new DataPlacement("localhost",4000)

	// get the key, contacting cached location or first asking DP if necessary
	def get(ns: String, key: String): Record = {
		// try local first
		try {
			val node= this.lookup(ns,key).toArray(0) // just read the first one for now
			node.open
			val record = node.client.get(ns,key)
			node.close
		} catch {
			case e:NotResponsible => {
				node.close
				set_map(dp.find(ns,key))
				node.open
				val record = node.client.get(ns,key)
				node.close
			}
			node.close
		}
		record
	}
	
	// determine set of nodes to interrogate, then do as many appropriate get_sets
	def get_set(ns: String, rset: SCADS.RecordSet): List[Record] = {
		var records = new HashSet[Record]
		val query_nodes = nodes.set_cover(ns,rset)
		query_nodes.foreach( {case (node,range_set)=> {
			try {
				node.open
				val subset = node.client.get_set(ns,range_set)
				node.close
			} catch {
				case e:NotResponsible => {
					node.close
					set_map(dp.find(ns,key))
					node.open
					val subset = node.client.get_set(ns,range_set)
					node.close
				}
				node.close
			}
			records += subset
		}
		})
		records.toList
	}
	
	// put key to all storage nodes, first asking DP for location if necessary
	def put(ns: String, key: String,value: RecordValue): Boolean = {
		val put_nodes= this.lookup(ns,key)
		put_nodes.foreach({ case(node)=>{
			try {
				node.open
				node.client.put(ns,key)
				node.close
			} catch {
				case e:NotResponsible => {
					node.close
					set_map(dp.find(ns,key))
					node.open
					node.client.put(ns,key)
					node.close
				}
				node.close
			}
		}})
		true
	}
	
	def set_map(m: Map[String,Map[Node,SCADS.RecordSet]]) = {
		this.nodes = m
	}
	
	
}
*/




