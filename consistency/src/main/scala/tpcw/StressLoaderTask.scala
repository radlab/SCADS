package edu.berkeley.cs
package scads
package consistency
package tpcw

import comm._
import piql._
import perf._
import storage._
import avro.runtime._
import avro.marker._

import deploylib._
import deploylib.mesos._

import net.lag.logging.Logger

import edu.berkeley.cs.scads.piql.tpcw._
import edu.berkeley.cs.scads.piql.exec._
import edu.berkeley.cs.scads.storage.transactions._

// MDCC version of TpcwLoaderTask.
case class StressLoaderTask(var clusterAddress: String,
                            var numServers: Int,
                            var numEBs: Double,
                            var numItems: Int,
                            var numClusters: Int,
                            var txProtocol: NSTxProtocol) extends AvroRecord {
  var numLoaders = 1

  def run() = {
    val logger = Logger()
    logger.setLevel(java.util.logging.Level.FINEST)

    val clusterRoot = ZooKeeperNode(clusterAddress)
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)
    cluster.blockUntilReady(numServers)

    val loader = new MDCCMicroTpcwLoader(numEBs = numEBs, numItems = numItems)

    println("get microitems")
    val microItems = cluster.getNamespace[MicroItem]("microItems", txProtocol)
    println("done get microitems")

    val clientId = coordination.registerAndAwait("clientStart", numLoaders, timeout=60*60*1000)
    if (clientId == 0) {
      logger.info("Awaiting scads cluster startup")
      cluster.blockUntilReady(numServers)
      loader.getMicroData(microItems).repartitionForClusters(numClusters)
      microItems.setReadWriteQuorum(0.1, 1.0)
    }
    coordination.registerAndAwait("namespacesReady", numLoaders)

    coordination.registerAndAwait("startBulkLoad", numLoaders)
    logger.info("Begining bulk loading of data")
    loader.getMicroData(microItems).load(clientId, numLoaders)
    logger.info("Bulk loading complete")
    coordination.registerAndAwait("loadingComplete", numLoaders)

    if(clientId == 0)
      clusterRoot.createChild("clusterReady", data=this.toBytes)
  }
}
