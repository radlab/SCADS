import SCADS.RecordSet
import SCADS.Record

import scala.collection.mutable.HashMap

case class KeyRange(start: String, end: String)

class NodeMap {
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
	def add(node: StorageThriftNode) = {
		nodes += node -> null
	}
	def add(node: StorageThriftNode,namespace: String, rs: RecordSet) = {
		//if nodes.contains(node)
	}
	def remove(node: StorageThriftNode) = {
		nodes - node
	}
}
