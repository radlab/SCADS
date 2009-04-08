package SCADS.DataPlacement

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
	var nodes: Map[Node,Map[NameSpace,RecordSet]]

	// find nodes responsible for this key in this namespace
	def lookup(ns: NameSpace, key: RecordKey): List[Node] = {
		var found_nodes = new HashSet[Node]
		nodes.foreach({ case (node,ns_rs)=>{
			val node_rs = ns_rs.get(ns)
			if (this.inSet(key,node_rs)) {found_nodes += node}
		} })
		found_nodes.toList
	}
	
	def inSet(key: RecordKey, rset: RecordSet): Boolean = {
		if (key >= rset.range.start_key && key <= rset.range_end_key) { return true }
		else { return false }
	}
	
	def set_cover(ns: NameSpace, rset: RecordSet): Map[Node,RangeSet] = {
		// TODO: this should be intelligent, e.g. min nodes to query or optimize for response time
		nodes.foreach({ case (node,ns_rs)=>{
			val node_rs = ns_rs.get(ns)
			
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
		val to_remove = nodes.filterKeys(n=>n.host==host && n.port==port)
		// TODO 
	}
	def remove_node(host: String) = {
		val to_remove = nodes.filterKeys(n=>n.host==host)
		// TODO
	}
}

abstract class DataPlacement(host: String, port: Int) with NodeMapManager{


	def copy(ns: NameSpace, source: Node, target: Node, resp: RecordSet): Boolean = {
		source.open
		target.open
		
		val success
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
	def move(ns: NameSpace, source: Node, target: Node, resp: RecordSet): Boolean = {
		source.open
		target.open
		
		val success_copy
		val success_remove
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
	def get_responsibility(ns: NameSpace, node: Node): RecordSet = {
		// TODO
	}
	def set_responsibility(ns: NameSpace, node: Node) = {
		// TODO 
	}
	def ask_responsibility(ns: NameSpace, node: Node): RecordSet = {
		node.open
		node.client.get_responsibility_policy(ns)
		node.close
	}	
	
	// return the whole map on inquiry
	def find(ns: NameSpace, key: RecordKey): Map[Node,RecordSet] = {
		this.nodes
	}
	
}	


abstract class ClientLibrary(host: String, port: Int) with NodeMap {

	val dp = new DataPlacement("localhost",4000)

	// get the key, contacting cached location or first asking DP if necessary
	def get(ns: NameSpace, key: RecordKey): Record = {
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
	def get_set(ns: NameSpace, rset: RecordSet): List[Record] = {
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
	def put(ns: NameSpace, key: RecordKey,value: RecordValue): Boolean = {
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
	
	def set_map(m: Map[NameSpace,Map[Node,RecordSet]]) = {
		this.nodes = m
	}
	
	
}





