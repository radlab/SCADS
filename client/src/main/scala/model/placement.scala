package edu.berkeley.cs.scads.model

import edu.berkeley.cs.scads.thrift.StorageNode
import edu.berkeley.cs.scads.storage.RangedPolicy
import edu.berkeley.cs.scads.storage.TestableScalaStorageEngine
import org.apache.zookeeper.{ZooKeeper, Watcher, WatchedEvent, CreateMode}
import org.apache.zookeeper.data.Stat
import scala.collection.mutable.HashMap
import scala.collection.jcl.Conversions
import org.apache.log4j.Logger


abstract class ClusterPlacement {
  def locate(namespace: String, key: String): Seq[StorageNode]
}

class TestCluster extends ClusterPlacement{
	val n = List(new TestableScalaStorageEngine())

  def locate(namespace: String, key: String): Seq[StorageNode] = n
}

class ZooKeptCluster(servers: String) extends ClusterPlacement with Watcher {
  val logger = Logger.getLogger("scads.zookeeper.clusterplacement")
  val zoo = new ZooKeeper(servers, 3000, this)
  val namespaceCache = new HashMap[String, Seq[(StorageNode, RangedPolicy)]]

  def locate(ns: String, key: String): Seq[StorageNode] = {
    val policies = if(namespaceCache.contains(ns))
      namespaceCache(ns)
    else
      loadNamespace(ns)

    policies.flatMap(n => {
      if(n._2.contains(key))
        List(n._1)
      else
        Nil
    })
  }

  protected def loadNamespace(ns: String): Seq[(StorageNode, RangedPolicy)] = {
    synchronized {
      if(namespaceCache.contains(ns))
        return namespaceCache(ns)
      val policies = Conversions.convertList(zoo.getChildren("/scads/namespaces/" + ns, true)).map(node => {
        getPolicy(ns, node)
      })
      namespaceCache.put(ns, policies)
      policies
    }
  }

  protected def getPolicy(ns: String, node: String): (StorageNode, RangedPolicy) = {
    val stat = new Stat
    val data = zoo.getData("/scads/namespaces/" + ns + "/" + node, true, stat)
    val parts = node.split(":")

    (StorageNode(parts(0), parts(1).toInt), RangedPolicy(data))
  }

  def process(event: WatchedEvent): Unit = {
		logger.info(event)
	}
}
