package edu.berkeley.cs
package scads
package perf
package scadr

import comm._
import piql._
import piql.scadr._
import storage._
import avro.runtime._
import avro.marker._

import deploylib._
import deploylib.mesos._
import deploylib.ec2._

case class ScadrLoaderTask(var numServers: Int,
                           var numLoaders: Int,
                           var followingCardinality: Int = 10,
                           var replicationFactor: Int = 1,
                           var usersPerServer: Int = 10000,
                           var thoughtsPerUser: Int = 100) extends DataLoadingTask with AvroRecord {
  var clusterAddress: String = _

  def run(): Unit = {
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)
    cluster.blockUntilReady(numServers)

    val loader = new ScadrLoader(
      replicationFactor = replicationFactor,
      numClients = numLoaders,
      numUsers = numServers * usersPerServer / replicationFactor,
      numThoughtsPerUser = thoughtsPerUser,
      numSubscriptionsPerUser = followingCardinality,
      numTagsPerThought = 5)

    val clientId = coordination.registerAndAwait("clientStart", numLoaders)
    if (clientId == 0) {
      logger.info("Awaiting scads cluster startup")
      cluster.blockUntilReady(numServers)
      retry() {
        loader.createNamespaces(cluster)
        val scadrClient = new ScadrClient(cluster, new SimpleExecutor)
        scadrClient.users.setReadWriteQuorum(0.33, 0.67)
        scadrClient.thoughts.setReadWriteQuorum(0.33, 0.67)
        scadrClient.subscriptions.setReadWriteQuorum(0.33, 0.67)
        scadrClient.tags.setReadWriteQuorum(0.33, 0.67)
      }
    }

    coordination.registerAndAwait("namespacesReady", numLoaders)
    val scadrClient = new ScadrClient(cluster, new SimpleExecutor)

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

    if (clientId == 0) {
      clusterRoot.createChild("clusterReady", data = this.toBytes)
      ExperimentNotification.completions.publish("ScadrLoad Complete", this.toJson)
    }

  }
}
