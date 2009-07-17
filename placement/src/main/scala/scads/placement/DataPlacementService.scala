package edu.berkeley.cs.scads.placement

import edu.berkeley.cs.scads.keys.Key
import edu.berkeley.cs.scads.keys.KeyRange
import edu.berkeley.cs.scads.nodes.StorageNode

trait DataPlacementService {
	def lookup(ns: String): Map[StorageNode, KeyRange]
	def lookup(ns: String, node: StorageNode): KeyRange 
	def lookup(ns: String, key: Key):List[StorageNode]
	def lookup(ns: String, range: KeyRange): Map[StorageNode, KeyRange]
	def refreshPlacement
}

trait SimpleDataPlacementService extends DataPlacementService {	
	var space = Map[String, Map[StorageNode, KeyRange]]()

	def lookup(ns: String): Map[StorageNode, KeyRange] = space.get(ns).getOrElse(Map[StorageNode,KeyRange]())
	def lookup(ns: String, node: StorageNode): KeyRange = space(ns).get(node).getOrElse(KeyRange.EmptyRange)

	def lookup(ns: String, key: Key):List[StorageNode] ={ // no null checking yet
		space(ns).toList.filter((pair) => pair._2.includes(key)).map((pair) => pair._1)
	}

	def lookup(ns: String, range: KeyRange): Map[StorageNode, KeyRange] = // no null checking yet
		space(ns).filter((pair) => (pair._2 & range) != KeyRange.EmptyRange)
	
	def refreshPlacement = {}
}
