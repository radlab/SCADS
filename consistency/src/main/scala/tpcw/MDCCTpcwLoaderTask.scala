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

import edu.berkeley.cs.scads.piql.tpcw._
import edu.berkeley.cs.scads.piql.exec._
import edu.berkeley.cs.scads.storage.transactions._

// MDCC version of TpcwLoaderTask.
case class MDCCTpcwLoaderTask(var numServers: Int,
                              var numLoaders: Int,
                              var numEBs: Double,
                              var numItems: Int,
                              var numClusters: Int,
                              var txProtocol: NSTxProtocol) extends DataLoadingTask with AvroRecord {
  var clusterAddress: String = _
  
  def run() = {
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)
    cluster.blockUntilReady(numServers)

    val loader = new TpcwLoader(numEBs = numEBs, numItems = numItems)

    val clientId = coordination.registerAndAwait("clientStart", numLoaders, timeout=60*60*1000)
    if (clientId == 0) retry(5) {
      logger.info("Awaiting scads cluster startup")
      cluster.blockUntilReady(numServers)
      val client = new MDCCTpcwClient(cluster, new SimpleExecutor, txProtocol)
      loader.createNamespacesForClusters(client, numClusters)
      import client._
      List(addresses,
           authors,
           xacts,
           countries,
           items,
           orderLines,
           orders,
           shoppingCartItems) foreach { ns => ns.setReadWriteQuorum(0.1, 1.0) }
    }
    coordination.registerAndAwait("namespacesReady", numLoaders)

    val tpcwClient = new MDCCTpcwClient(cluster, new SimpleExecutor, txProtocol)
    coordination.registerAndAwait("startBulkLoad", numLoaders)
    logger.info("Begining bulk loading of data")
    loader.namespaces(tpcwClient).foreach(_.load(clientId, numLoaders))
    logger.info("Bulk loading complete")
    coordination.registerAndAwait("loadingComplete", numLoaders)

    if(clientId == 0)
      clusterRoot.createChild("clusterReady", data=this.toBytes)
  }
}
