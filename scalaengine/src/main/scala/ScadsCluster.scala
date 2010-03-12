package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import org.apache.avro.Schema
import org.apache.zookeeper.CreateMode

class ScadsCluster(root: ZooKeeperProxy#ZooKeeperNode) {
	val namespaces = root.getOrCreate("namespaces")

  /* If namespace exists, just return false, otherwise, create namespace and return true */
  def createNamespaceIfNotExists(ns: String, keySchema: Schema, valueSchema: Schema): Boolean = {
    val nsRoot = 
      try {
        namespaces.get(ns)
      } catch {
        case nse:java.util.NoSuchElementException =>
          null
        case exp =>
          throw exp
      }
    if (nsRoot == null) {
      createNamespace(ns,keySchema,valueSchema)
      true
    } else
      false
  }

	def createNamespace(ns: String, keySchema: Schema, valueSchema: Schema): Unit = {
		val nsRoot = namespaces.createChild(ns, "", CreateMode.PERSISTENT)
		nsRoot.createChild("keySchema", keySchema.toString(), CreateMode.PERSISTENT)
		nsRoot.createChild("valueSchema", valueSchema.toString(), CreateMode.PERSISTENT)

		val partition = nsRoot.getOrCreate("partitions/1")
		val policy = new PartitionedPolicy
		policy.partitions = List(new KeyPartition)

		partition.createChild("policy", policy.toBytes, CreateMode.PERSISTENT)
		partition.createChild("servers", "", CreateMode.PERSISTENT)
	}
	def addPartition(ns:String, name:String, policy:PartitionedPolicy):Unit = {
		val partition = namespaces.getOrCreate(ns+"/partitions/"+name)

		partition.createChild("policy", policy.toBytes, CreateMode.PERSISTENT)
		partition.createChild("servers", "", CreateMode.PERSISTENT)
	}
	def addPartition(ns:String,name:String):Unit = {
		val policy = new PartitionedPolicy
		policy.partitions = List(new KeyPartition)
		addPartition(ns,name,policy)
	}
}
