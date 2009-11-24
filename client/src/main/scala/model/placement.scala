package edu.berkeley.cs.scads.model

import edu.berkeley.cs.scads.thrift.StorageNode
import edu.berkeley.cs.scads.storage.TestableScalaStorageEngine

abstract class ClusterPlacement {
  def locate(namespace: String, key: String): List[StorageNode]
}

class TestCluster extends ClusterPlacement{
	val n = List(new TestableScalaStorageEngine())

  def locate(namespace: String, key: String): List[StorageNode] = n
}
