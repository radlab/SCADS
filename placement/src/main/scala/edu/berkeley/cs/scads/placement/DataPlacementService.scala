package edu.berkeley.cs.scads.placement

import edu.berkeley.cs.scads.keys.Key
import edu.berkeley.cs.scads.keys.KeyRange
import edu.berkeley.cs.scads.nodes.StorageNode

abstract class DataPlacementService {
	def lookup(ns: String): Map[StorageNode, KeyRange]
	def lookup(ns: String, node: StorageNode): KeyRange 
	def lookup(ns: String, key: Key):List[StorageNode]
	def lookup(ns: String, range: KeyRange): Map[StorageNode, KeyRange]
}