package edu.berkeley.cs
package scads
package perf
package scadr

import comm._
import piql._
import storage._
import avro.runtime._
import avro.marker._

import deploylib._
import deploylib.mesos._

case class ScadrLoaderClient(var numServers: Int, var numLoaders: Int, var followingCardinality: Int, var replicationFactor: Int = 1) extends AvroClient with AvroRecord {

  def experimentName = "Scadr"

  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode) = {
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)
    val scadrClient = new ScadrClient(cluster, new SimpleExecutor)
    val loader = new ScadrLoader(scadrClient,
      replicationFactor = replicationFactor,
      numClients = numLoaders,
      numUsers = numServers * 10000 / replicationFactor,
      numThoughtsPerUser = 100,
      numSubscriptionsPerUser = followingCardinality,
      numTagsPerThought = 5)

    val clientId = coordination.registerAndAwait("clientStart", numLoaders)
    if(clientId == 0) {
      logger.info("Awaiting scads cluster startup")
      cluster.blockUntilReady(numServers)
      loader.createNamespaces
      scadrClient.users.setReadWriteQuorum(0.33, 0.67)
      scadrClient.thoughts.setReadWriteQuorum(0.33, 0.67)
      scadrClient.subscriptions.setReadWriteQuorum(0.33, 0.67)
      scadrClient.tags.setReadWriteQuorum(0.33, 0.67)
      scadrClient.idxUsersTarget.setReadWriteQuorum(0.33, 0.67)
    }

    coordination.registerAndAwait("startBulkLoad", numLoaders)
    logger.info("Begining bulk loading of data")
    val data = loader.getData(clientId)
    logger.info("Loading users")
    scadrClient.users ++= data.userData
    logger.info("Loading thoughts")
    scadrClient.thoughts ++= data.thoughtData
    logger.info("Loading subscriptions")
    scadrClient.subscriptions ++= data.subscriptionData
    logger.info("Bulk loading complete")
    coordination.registerAndAwait("loadingComplete", numLoaders)

    if(clientId == 0)
      clusterRoot.createChild("clusterReady", data=this.toBytes)

    System.exit(0)
  }
}
