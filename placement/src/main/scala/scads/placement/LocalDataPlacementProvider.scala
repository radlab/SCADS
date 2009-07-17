package edu.berkeley.cs.scads.placement

import edu.berkeley.cs.scads.thrift.DataPlacement
import edu.berkeley.cs.scads.nodes.StorageNode
import edu.berkeley.cs.scads.keys.KeyRange

trait LocalDataPlacementProvider extends SimpleDataPlacementService {

	def add_namespace(ns: String): Boolean = {
		add_namespace(ns,null)
	}
	def add_namespace(ns: String, mapping:Map[StorageNode,KeyRange]): Boolean = {
		space += (ns -> mapping)
		space.contains(ns)
	}
}
