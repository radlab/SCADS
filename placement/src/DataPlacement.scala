/* CLUSTER MEMBERSHIP ----------------------------------------------- */

abstract class ClusterMembership(dp: DataPlacement) {
	/* State */
	val placement = dp					// instance of Data Placement class
	def members: Map[Node,String] 		// list of nodes and their status
	
	private def add_node (node: Node)		// add to cluster list
	private def add_node_dp (node: Node)	// notify dp of addition
	
	private def remove_node(node: Node)		// remove from cluster list
	private def remove_node_dp(node: Node)  // notify dp of removal 

}
abstract class SimpleCluster(dp: DataPlacement) extends ClusterMembership {
	/* Methods */
	def join(node: Node) 	// a node joins the cluster; cluster membership informs its placement instance
	def leave(node: Node) 	// a node leaves, cluster memberships informs placement
}
abstract class MulticastCluster(dp: DatePlacement) extends ClusterMembership {
	
}
trait HeartBeat {
	/* State */
	def beat_interval: Int  // how often to ping a node in the cluster
	def timeout: Int		// when is a node considered dead
	
	/* Methods */
	def ping(node: Node) 	// send a message to the node; update node's status, perhaps call leave()
}

/* DATA PLACEMENT ---------------------------------------------------- */

abstract class DataPlacement {
	/* State */
	def nodes: NodeMap		// map of nodes to their record responsibilities for each namespace
	
	/* Methods */
	add_node(node: Node)	// start an entry in the node map with blank responsibility
	remove_node(node: Node)	// remove all map entries for this node
	get_map: NodeMap		// return a copy of the NodeMap; used by the Client Library to update itself
	
	set_responsibility(node: Node, namespace: String, records: RecordSet) 	// assign a set of records to a node
	ask_responsibility(node: Node, namespace: String, records: RecordSet) 	// ask a node what its responsibility is
	
	move(source: Node, target: Node, namespace: String, records: RecordSet) // move a responsibility from one node to another
	copy(source: Node, target: Node, namespace: String, records: RecordSet) // copy a responsibility from one node to another	
}
abstract class DatePlacementPeer extends DataPlacement { // (?)
	/* State */
	def finger_table: Map[DataPlacement, RecordSet]	// map of other data placements and what record sets they manage
	
	/* Methods */
	def locate(records: RecordSet): List[DataPlacement,RecordSet]	// which data placement	knows about these records
}

/* CLIENT LIBRARY ------------------------------------------------------ */

abstract class ClientLibrary(dp: DataPlacement) {
	/* State */
	val placement = dp		// instance of Data Placement class
	def nodes: NodeMap		// read-only version of map of node responsibilities
	
	/* Methods */
	// get the Records matching the key, ask DP for map if necessary; calls get() on node
	def get(namespace: String, key: String): Record
	
	// determine set of nodes to interrogate, then do as many appropriate get_sets from nodes; gets map from DP if necessary
	def get_set(namespace: String, keys: RecordSet): List[Record] 
	
	// put key to responsible storage node(s), asking DP for location if necessary
	def put(namespace: String, key: String, value: RecordValue): Boolean 
}

abstract class ROWAClientLibrary extends ClientLibrary(dp: DataPlacement) {
	/* Methods */
	def get(namespace: String, key: String): Record 						// read from one node
	def get_set(namespace: String, keys: RecordSet): List[Record]			// read from one node
	def put(namespace: String, key: String, value: RecordValue): Boolean 	// write to all responsible nodes
}
abstract class QuorumClientLibrary(read_q: Int, write_q: Int) extends ClientLibrary(dp: DataPlacement) {
	/* State */
	def read_q: Int 	// how many nodes to read from before returning result
	def write_q: Int	// how many nodes to write to before returning success

	/* Methods */
	def get(namespace: String, key: String): Record 						// read from quorum nodes
	def get_set(namespace: String, keys: RecordSet): List[Record]			// read from quorum nodes
	def put(namespace: String, key: String, value: RecordValue): Boolean 	// write to quorum responsible nodes
}

/* HELPER CLASSES ------------------------------------------------------ */

case class Node(h: String, p: Int) {
	val host: String = h
	val port: Int = p
}

abstract class NodeMap {
	/* State */
	var nodes: Map[Node,Map[String,RecordSet]] // maps a node to its responsibility within each namespace

	/* Methods */
	def lookup(ns: String, key: String): List[Node] // find nodes responsible for this key in this String 
	def lookup_set(ns: String, rset: RecordSet): Map[Node,RecordSet] // find nodes and what to get from each one
}
abstract class MutableNodeMap extends NodeMap {
	/* Methods */
	def add_node(host: String, port: Int)
	def remove_node(host: String, port: Int)
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




