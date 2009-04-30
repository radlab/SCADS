import SCADS.RecordSet
import SCADS.RecordSetType
import SCADS.RangeSet
import SCADS.Record
import SCADS.NotResponsible

import java.util.Comparator

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet


trait KeySpaceProvider {
	def getKeySpace(ns: String):KeySpace
	def refreshKeySpace()
}

trait LocalKeySpaceProvider extends KeySpaceProvider {
	var ns_map = new HashMap[String,KeySpace]
	
	override def getKeySpace(ns: String): KeySpace = { ns_map(ns) }
	override def refreshKeySpace() = {}
	
	def add_namespace(ns: String): Boolean = {
		this.add_namespace(ns,null)
		true
	}
	def add_namespace(ns: String, ks: SimpleKeySpace): Boolean = {
		ns_map.update(ns,ks)
		true
	}
	
	def getMap: HashMap[String,KeySpace] = ns_map
}


/*
abstract class ClientLibrary extends SCADS.ClientLibrary.Iface {
	
	def get(namespace: String, key: String): Record
	def get_set(namespace: String, keys: RecordSet): java.util.List[Record]
	def put(namespace: String, rec:Record): Boolean 
}
*/

class RecordComparator extends java.util.Comparator[SCADS.Record] {
	def compare(o1: SCADS.Record, o2: SCADS.Record): Int = {
		if (o1.key < o2.key) -1
		else if (o1.key > o2.key) 1
		else 0 // equal
	}
}

class LocalROWAClientLibrary extends ROWAClientLibrary with LocalKeySpaceProvider

abstract class ROWAClientLibrary extends SCADS.ClientLibrary.Iface with KeySpaceProvider with ThriftConversions {

	/**
	* Read value from one node. Uses local map. 
	* Does update from KeySpaceProvider if local copy is out of date.
	*/
	override def get(namespace: String, key: String): Record = {
		val ns_keyspace = getKeySpace(namespace)
		try {
			val potentials = ns_keyspace.lookup(key)
			if ( potentials.hasNext ) {
				val node = potentials.next // just get the first node
				val record = node.getClient().get(namespace,key)
				record
			}
			else throw new NoNodeResponsibleException
		} catch {
			case e:NotResponsible => {
				this.refreshKeySpace()
				val record = this.get(namespace,key) // recursion, TODO: needs to be bounded
				record
			}
		}
	}
	
	/**
	* Read values from one node. Uses local map.
	* Does update from KeySpaceProvider if local copy is out of date.
	*/
	override def get_set(namespace: String, keys: RecordSet): java.util.List[Record] = {
		var records = new HashSet[Record]
		val ns_keyspace = getKeySpace(namespace)
		val target_range = new KeyRange(keys.range.start_key, keys.range.end_key)
		var ranges = Set[KeyRange]()

		// determine which ranges to ask from which nodes
		// assumes no gaps in range, but someone should tell user if entire range isn't covered
		val potentials = ns_keyspace.lookup(target_range)
		val query_nodes = this.get_set_queries(potentials,target_range)		

		// now do the getting
		query_nodes.foreach( {case (node,keyrange)=> {			
			ranges += keyrange
			val rset = this.keyRangeToScadsRangeSet(keyrange)

			try {			
				val records_subset = node.getClient().get_set(namespace,rset)
				val iter = records_subset.iterator()
				while (iter.hasNext()) { records += iter.next() }
				
			} catch {
				case e:NotResponsible => {
					this.refreshKeySpace()
					val records_subset = node.getClient().get_set(namespace,rset)
					val iter = records_subset.iterator()
					while (iter.hasNext()) { records += iter.next() }
				}
			}
		}
		})
		
		// make sure desired range was actually covered by what we gots
		if ( !ns_keyspace.isCovered(target_range,ranges) ) { 
			//throw new NonCoveredRangeException // do we ever reach here?
		}	 
		// sort an array
		val records_array = records.toArray
		java.util.Arrays.sort(records_array,new RecordComparator()) 
		java.util.Arrays.asList(records_array: _*) // shitty, but convert to java array
	}
	
	private def get_set_queries(nodes: Map[StorageNode, KeyRange], target_range: KeyRange): HashMap[StorageNode, KeyRange] = {
		var resultmap = new HashMap[StorageNode, KeyRange]
		
		var start = target_range.start
		val end = target_range.end
		
		var done = false // have we found everything we can get?
		var nodes_used = Set[StorageNode]() // which nodes we've checked so far, assumes nodes have only one range
		while (!done) {
			val node_tuple = this.find_node_at_start(nodes.filter((entry)=> !nodes_used.contains(entry._1)),start)
			if (node_tuple._2.end==null || node_tuple._2.end > end) resultmap += node_tuple._1 -> KeyRange(node_tuple._2.start,end)
			else resultmap += node_tuple._1 -> node_tuple._2
			start = node_tuple._2.end
			nodes_used += node_tuple._1
			if ( (start==null ) || (start >= end) ) { done = true }
		}
		resultmap
	}

	private def find_node_at_start(nodes: Map[StorageNode,KeyRange], start: String): (StorageNode,KeyRange) = {
		// nodes that start at or before target start, null target start needs a null start
		var potential_nodes = Map[StorageNode,KeyRange]()
		if (start == null) { potential_nodes = nodes.filter((entry) => entry._2.start==null) }
		else { potential_nodes = nodes.filter((entry) => entry._2.start==null || entry._2.start <= start) } 
	
		if ( !potential_nodes.elements.hasNext ) throw new NonCoveredRangeException
		var chosen_node = potential_nodes.elements.next // init to first one?
		var end = chosen_node._2.end
		
		potential_nodes.foreach((entry) => {
			if (entry._2.end==null || entry._2.end > end) {
				chosen_node = entry
				end = chosen_node._2.end
			}
		})
		val range_covered = new KeyRange(start,end)
		(chosen_node._1, range_covered)
	}
	
	/**
	* Write records to all responsible nodes.
	* Does update from KeySpaceProvider if local copy is out of date.
	*/
	override def put(namespace: String, rec:Record): Boolean = {
		val key = rec.getKey()
		val ns_keyspace = getKeySpace(namespace)
		val put_nodes = ns_keyspace.lookup(key)
		if ( !put_nodes.hasNext ) throw new NoNodeResponsibleException
		var total_success = true
		
		put_nodes.foreach({ case(node)=>{
			try {
				val success = node.getClient().put(namespace,rec)
				total_success && success 
			} catch {
				case e:NotResponsible => {
					this.refreshKeySpace()
					val success = this.put(namespace,rec) // recursion may redo some work
					total_success && success
				}
			}
		}})
		total_success
	}
}

