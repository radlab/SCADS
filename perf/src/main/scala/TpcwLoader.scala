package edu.berkeley.cs
package scads
package perf
package tpcw

import comm._
import piql._
import storage._
import avro.runtime._
import avro.marker._

import deploylib._
import deploylib.mesos._

case class TpcwLoaderClient(var numServers: Int,
                            var numLoaders: Int,
                            var numEBs: Double,
                            var numItems: Int,
                            var replicationFactor: Int = 1) extends AvroClient with AvroRecord {

  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode) = {
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)
    val tpcwClient = new TpcwClient(cluster, new SimpleExecutor)
    val loader = new TpcwLoader(tpcwClient,
      numClients = numLoaders,
      numEBs = numEBs,
      numItems = numItems)
    val clientId = coordination.registerAndAwait("clientStart", numLoaders)
    if (clientId == 0) {
      logger.info("Awaiting scads cluster startup")
      cluster.blockUntilReady(numServers)
      loader.createNamespaces
      import tpcwClient._
      List(address,
           author,
           authorNameItemIndex,
           xacts,
           country,
           item,
           itemSubjectDateTitleIndex,
           orderline,
           order,
           customerOrderIndex,
           itemTitleIndex,
           shoppingCartItem) foreach { ns => ns.setReadWriteQuorum(0.33, 0.67) }
    }

    coordination.registerAndAwait("startBulkLoad", numLoaders)
    logger.info("Begining bulk loading of data")
    val data = loader.getData(clientId)

    logger.info("Loading addresses")
    tpcwClient.address ++= data.addresses
    logger.info("Loading authors")
    tpcwClient.author ++= data.authors
    logger.info("Loading xacts")
    tpcwClient.xacts ++= data.xacts
    logger.info("Loading countries")
    tpcwClient.country ++= data.countries
    logger.info("Loading customers")
    tpcwClient.customer ++= data.customers
    logger.info("Loading items")
    tpcwClient.item ++= data.items
    logger.info("Loading orders")
    tpcwClient.order ++= data.orders
    logger.info("Loading orderlines")
    tpcwClient.orderline ++= data.orderlines

    logger.info("Loading author name indexes")
    tpcwClient.authorNameItemIndex ++= data.authorNameItemIndexes
    logger.info("Loading item subject date indexes")
    tpcwClient.itemSubjectDateTitleIndex ++= data.itemSubjectDateTitleIndexes
    logger.info("Loading customer order indexes")
    tpcwClient.customerOrderIndex ++= data.customerOrderIndexes
    logger.info("Loading item title indexes")
    tpcwClient.itemTitleIndex ++= data.itemTitleIndexes

    logger.info("Bulk loading complete")
    coordination.registerAndAwait("loadingComplete", numLoaders)

    if(clientId == 0)
      clusterRoot.createChild("clusterReady", data=this.toBytes)

    System.exit(0)
  }

}
