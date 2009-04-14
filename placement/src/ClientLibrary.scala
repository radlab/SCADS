import SCADS.RecordSet
import SCADS.RangeSet
import SCADS.Record
import SCADS.NotResponsible
import SCADS.ClientLibrary

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

trait KeySpaceProvider {
	def getKeySpace(ns: String)
	def refreshKeySpace()
}

abstract class ClientLibrary extends SCADS.ClientLibrary.Iface {
	def ns_map: Map[String,KeySpace]
	
	def add_namespace(ns: String): Boolean = {
		this.add_namespace(ns,null)
		true
	}
	def add_namespace(ns: String, ks: KeySpace): Boolean = {
		ns_map.update(ns,ks)
		true
	}
	
	def get(namespace: String, key: String): Record
	def get_set(namespace: String, keys: RecordSet): java.util.List[Record]
	def put(namespace: String, rec:Record): Boolean 
}

class ROWAClientLibrary extends ClientLibrary with KeySpaceProvider {
	var ns_map = new HashMap[String,InefficientKeySpace]
	var dp_map = new HashMap[String,DataPlacement]
	
	/**
	* Asks key space provider for latest keyspace for the specified namespace.
	* Updates the local copy's keyspace.
	*/
	def getKeySpace(ns: String) = {
		val ks = dp_map.get(ns).keySpace
		ns_map.update(ns,ks)
	}
	
	/**
	* Asks key space provider for all known namespaces.
	* Updates all the keyspaces.
	*/
	def refreshKeySpace() = {
		ns_map.foreach({ case(ns,ks) => {
			this.getKeySpace(ns)
		}})
	}

	/**
	* Read value from one node. Uses local map. 
	* Does update from KeySpaceProvider if local copy is out of date.
	*/
	override def get(namespace: String, key: String): Record = {
		val ns_keyspace = ns_map.get(namespace)
		try {
			val node = ns_keyspace.lookup(key).elements.next // just get the first node
			val record = node.get(ns,key)
			record
		} catch {
			case e:NotResponsible => {
				this.getKeySpace(namespace)
				val record = this.get(namespace,key) // recursion, will this work?
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
		val ns_keyspace = ns_map.get(namespace)
		val target_range = new KeyRange(keys.range.start_key, keys.range.end_key)

		// determine which ranges to ask from which nodes
		// assumes no gaps in range, but someone should tell user if entire range isn't covered
		val query_nodes = this.get_set_queries(ns_keyspace.lookup(target_range))

		// now do the getting
		query_nodes.foreach( {case (node,keyrange)=> {
			val rset = new RecordSet(3,new RangeSet(keyrange.start,keyrange.end,0,0),null)
			try {
				val records_subset = node.get_set(namespace,rset)
				records ++= records_subset.iterator()
			} catch {
				case e:NotResponsible => {
					// TODO
				}
			}
		}
		})
		java.util.Arrays.asList(records.toArray: _*) // shitty, but convert to java array
	}
	
	private def get_set_queries(nodes: Map[Node, KeyRange]): Map[Node, KeyRange] = {
	
	}
	
	/**
	* Write records to all responsible nodes.
	* Does update from KeySpaceProvider if local copy is out of date.
	*/
	override def put(namespace: String, rec:Record): Boolean = {
		val key = rec.getKey()
		val ns_keyspace = ns_map.get(namespace)
		val put_nodes = ns_keyspace.lookup(key)
		
		put_nodes.foreach({ case(node)=>{
			try {
				val success = node.put(ns,rec)
				success
			} catch {
				case e:NotResponsible => {
					this.getKeySpace(namespace)
					this.put(namespace,rec) // recursion may redo some work
				}
			}
		}})
	}
}


class ClientLibraryServer(p: Int) extends ThriftServer {
	val port = p
	val processor = new SCADS.ClientLibrary.Processor(new ROWAClientLibrary)
}

