import SCADS.RecordSet
import SCADS.Record

import scala.collection.mutable.HashMap


class NodeMap {
	/* State */
	var nodes = new HashMap[StorageThriftNode,Map[String,RecordSet]]  // maps a node to its responsibility within each namespace

	/* Methods */
	def add(node: StorageThriftNode) = {
		nodes += node -> null
	}
	def remove(node: StorageThriftNode) = {
		nodes - node
	}
	def lookup(ns: String, key: String): List[StorageThriftNode] = {// find nodes responsible for this key in this String 
		null
	}
	def lookup_set(ns: String, rset: RecordSet): Map[StorageThriftNode,RecordSet] = { // find nodes and what to get from each one
		null
	}
}
abstract class MutableNodeMap extends NodeMap {
	/* Methods */
	def add_node(host: String, port: Int) = {}
	def remove_node(host: String, port: Int) = {}
}