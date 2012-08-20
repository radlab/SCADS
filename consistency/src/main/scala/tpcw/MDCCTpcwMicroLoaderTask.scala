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

import ch.ethz.systems.tpcw.populate.data.Generator
import ch.ethz.systems.tpcw.populate.data.objects._

// MDCC version of TpcwLoaderTask.
// Didn't want to change the original MDCCTpcwLoaderTask, so that the schema
// doesn't change for the old data.
case class MDCCTpcwMicroLoaderTask(var numServers: Int,
                                   var numLoaders: Int,
                                   var numEBs: Double,
                                   var numItems: Int,
                                   var numClusters: Int,
                                   var txProtocol: NSTxProtocol,
                                   var namespace: String = "") extends DataLoadingTask with AvroRecord {
  var clusterAddress: String = _
  
  def run() = {
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)
    cluster.blockUntilReady(numServers)

    val loader = new MDCCMicroTpcwLoader(numEBs = numEBs, numItems = numItems)

    val microItems = cluster.getNamespace[MicroItem]("microItems", txProtocol)

    val clientId = coordination.registerAndAwait("clientStart", numLoaders, timeout=60*60*1000)
    if (clientId == 0) retry(5) {
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

class MDCCMicroTpcwLoader(numEBs : Double,
                          numItems : Int) extends TpcwLoader(numEBs, numItems) {
  def getMicroData(ns: PairNamespace[MicroItem]) = RandomData(ns, createMicroItem(_), numItems)

  def createMicroItem(itemId : Int) : MicroItem = {
    val to = Generator.generateItem(itemId, numItems).asInstanceOf[ItemTO]
    var item = MicroItem(toItem(itemId))
    item.I_TITLE = to.getI_title
    item.I_A_ID = toAuthor(to.getI_a_id)
    item.I_PUB_DATE = to.getI_pub_date
    item.I_PUBLISHER = to.getI_publisher
    item.I_SUBJECT = to.getI_subject
    item.I_DESC = to.getI_desc
    item.I_RELATED1 = to.getI_related1
    item.I_RELATED2 = to.getI_related2
    item.I_RELATED3 = to.getI_related3
    item.I_RELATED4 = to.getI_related4
    item.I_RELATED5 = to.getI_related5
    item.I_THUMBNAIL = to.getI_thumbnail
    item.I_IMAGE = to.getI_image
    item.I_SRP = to.getI_srp
    item.I_COST = to.getI_cost
    item.I_AVAIL = to.getI_avail
    item.I_STOCK = to.getI_stock + 100 //
//    item.I_STOCK = to.getI_stock + 30 // 30 works well for 30 clients, 1 minute
//    item.I_STOCK = to.getI_stock
    item.ISBN = to.getI_isbn
    item.I_PAGE = to.getI_page
    item.I_BACKING = to.getI_backing
    item.I_DIMENSION = to.getI_dimensions
    item
  }
}
