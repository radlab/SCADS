import SCADS.RecordSet
import SCADS.Record
import SCADS.NotResponsible
import SCADS.ClientLibrary

import org.apache.thrift.TException
import org.apache.thrift.TProcessor
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TServerTransport
import org.apache.thrift.transport.TServerSocket
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TTransportFactory
import org.apache.thrift.transport.TTransportException
import org.apache.thrift.server.TServer
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.protocol.TBinaryProtocol

import scala.collection.mutable.HashSet

trait KeySpaceProvider {
	def getKeySpace(ns: String)
	def refreshKeySpace()
}

class SimpleClientLibrary(dp: DataPlacement) extends ROWAClientLibrary(dp) {
	var nodes = new NodeMap

	// get the key, contacting cached location or first asking DP if necessary
	override def get(ns: String, key: String): Record = {
		// try local first
		try {
			val node= nodes.lookup(ns,key).toArray(0) // just read the first one for now
			val record = node.get(ns,key)
			record
		} catch {
			case e:NotResponsible => {
				set_map(dp.get_map)
				val node= nodes.lookup(ns,key).toArray(0) // just read the first one for now
				val record = node.get(ns,key)
				record
			}
		}
	}

	// determine set of nodes to interrogate, then do as many appropriate get_sets
	override def get_set(ns: String, rset: SCADS.RecordSet): java.util.List[Record]  = {
		var records = new HashSet[Record]
		val query_nodes = nodes.lookup_set(ns,rset)
		
		query_nodes.foreach( {case (node,range_set)=> {
			try {
				val subset = node.get_set(ns,range_set)
				val iter = subset.iterator()
				while (iter.hasNext()) {
					records += iter.next()
				}
			} catch {
				case e:NotResponsible => {
					set_map(dp.get_map)
					val subset = node.get_set(ns,range_set)
					val iter = subset.iterator()
					while (iter.hasNext()) {
						records += iter.next()
					}
				}
			}
		}
		})
		java.util.Arrays.asList(records.toArray: _*) 
	}

	// put key to all storage nodes, first asking DP for location if necessary
	override def put(ns: String, rec: Record): Boolean = {
		val key = rec.getKey()
		val put_nodes= nodes.lookup(ns,key)
		put_nodes.foreach({ case(node)=>{
			try {
				node.put(ns,rec)
			} catch {
				case e:NotResponsible => {
					set_map(dp.get_map)
					node.put(ns,rec)
				}
			}
		}})
		true // TODO: make this accurate
	}

	def set_map(m: NodeMap) = {
		this.nodes = m
	}

}

class ClientLibraryServer(p: Int) extends ThriftServer {
	val port = p
	private val dp = new SimpleDataPlacement
	val processor = new SCADS.ClientLibrary.Processor(new SimpleClientLibrary(dp))
}


abstract class ClientLibrary(dp: DataPlacement) extends SCADS.ClientLibrary.Iface {
	/* State */
	val placement = dp		// instance of Data Placement class
	def nodes: NodeMap		// read-only version of map of node responsibilities
	
	/* Methods */
	// get the Records matching the key, ask DP for map if necessary; calls get() on node
	def get(namespace: String, key: String): Record
	
	// determine set of nodes to interrogate, then do as many appropriate get_sets from nodes; gets map from DP if necessary
	def get_set(namespace: String, keys: RecordSet): java.util.List[Record] 
	
	// put key to responsible storage node(s), asking DP for location if necessary
	def put(namespace: String, rec:Record): Boolean 
}

abstract class ROWAClientLibrary(dp: DataPlacement) extends ClientLibrary(dp) {
	/* Methods */
	def get(namespace: String, key: String): Record 						// read from one node
	def get_set(namespace: String, keys: RecordSet): java.util.List[Record]			// read from one node
	def put(namespace: String, rec:Record): Boolean 	// write to all responsible nodes
}



