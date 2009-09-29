package edu.berkeley.cs.scads

import edu.berkeley.cs.scads.storage.TestableScalaStorageEngine
import edu.berkeley.cs.scads.placement.DataPlacementService
import edu.berkeley.cs.scads.nodes.{StorageNode, TestableStorageNode}
import edu.berkeley.cs.scads.keys.KeyRange
import edu.berkeley.cs.scads.keys.{Key, MinKey, MaxKey}


class TestCluster extends DataPlacementService {
	val n = new TestableScalaStorageEngine()
	val range = new KeyRange(MinKey, MaxKey)

	def lookup(ns: String): Map[StorageNode, KeyRange] = Map(n -> range)
	def lookup(ns: String, node: StorageNode): KeyRange = {assert(node == n); range}
	def lookup(ns: String, key: Key):List[StorageNode] = List(n)
	def lookup(ns: String, range: KeyRange): Map[StorageNode, KeyRange] = Map(n -> range)
	def refreshPlacement = true
}
