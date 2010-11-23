package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm.ZooKeeperHelper

import org.apache.zookeeper.CreateMode

/**
 * Usage of TestScalaEngine has been simplified. Now TestScalaEngine 
 * only exposes one method, which you use to get a fresh cluster. 
 * See ManagedScadsCluster for usage.
 */
object TestScalaEngine {
  lazy val zooKeeper = ZooKeeperHelper.getTestZooKeeper

  /** Main API of TestScalaEngine. Returns a new ManagedScadsCluster
   * which is quasi-guaranteed to be backed by a unique zookeeper root node.
   * The number of storage nodes which start in the ManagedScadsCluster is given
   * by the numNodes parameter. The default is 1 */
  def newScadsCluster(numNodes: Int = 1): ManagedScadsCluster = {
    require(numNodes >= 0, "numNodes must be non-negative")
    /** Cannot use EPHEMERAL* here since the zooRoot must be able
     * to have children (ie namespaces, keySchema, etc), and 
     * EPHEMERAL* nodes CANNOT have children */
    val zooRoot = zooKeeper.root.createChild("scadsClient-%s".format(java.util.UUID.randomUUID.toString), Array.empty, CreateMode.PERSISTENT)
    val cluster = new ManagedScadsCluster(zooRoot)
    cluster ensureExactly numNodes
    cluster
  }

}
