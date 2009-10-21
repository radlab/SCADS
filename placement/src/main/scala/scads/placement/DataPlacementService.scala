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

	def lookup(ns: String, key: Key):List[StorageNode] ={
		if (space.contains(ns)) space(ns).toList.filter((pair) => pair._2.includes(key)).map((pair) => pair._1)
		else List[StorageNode]()
	}

	def lookup(ns: String, range: KeyRange): Map[StorageNode, KeyRange] = {
		if (space.contains(ns)) space(ns).filter((pair) => (pair._2 & range) != KeyRange.EmptyRange)
		else Map[StorageNode,KeyRange]()
	}

	def printSpace(ns: String):String = {
		if (!space.contains(ns)) "No mappings for "+ns
		else {
			val ns_space = space(ns)
			if(!ns_space.isEmpty)
				ns+"\n==============\n"+ ns_space.map((pair) => pair._1 + " => " + pair._2).reduceLeft((a,b) => a + "\n" + b)
			else
				"Empty"
		}
	}

	def refreshPlacement = {}
}
