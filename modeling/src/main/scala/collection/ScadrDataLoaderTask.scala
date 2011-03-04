package edu.berkeley.cs
package scads
package piql
package modeling

import deploylib.mesos._
import deploylib.ec2._
import comm._
import storage._
import piql._
import perf._
import piql.scadr._
import avro.marker._
import avro.runtime._

import net.lag.logging.Logger
import java.io.File
import java.net._

import scala.collection.JavaConversions._
import scala.collection.mutable._

import com.amazonaws.services.sns._
import com.amazonaws.auth._
import com.amazonaws.services.sns.model._

case class ScadrDataLoaderTask(
  var params: ScadrClusterParams
) extends AvroTask with AvroRecord {
  def run(): Unit = {
    /* setting up cluster */
    println("setting up cluster...")
    val clusterRoot = ZooKeeperNode(params.clusterAddress)
    val cluster = new ExperimentalScadsCluster(clusterRoot)
    cluster.blockUntilReady(params.numStorageNodes)

    println("setting up scadr client and scadr loader...")
    val scadrClient = new ScadrClient(cluster, new SimpleExecutor)
    //val scadrLoader = new ScadrLoader(scadrClient, 1, 1)
    val scadrLoader = new ScadrLoader(
      scadrClient,                      //val client: ScadrClient,
      1,                                //val replicationFactor: Int,
      params.numLoadClients,            //val numClients: Int, // number of clients to split the loading by
      params.numUsers,                  //val numUsers: Int = 100,
      params.numThoughtsPerUser,        //val numThoughtsPerUser: Int = 10,
      params.numSubscriptionsPerUser,   //val numSubscriptionsPerUser: Int = 10,
      5                                 //val numTagsPerThought: Int = 5) // I don't think this is implemented
    )

    // wait for each client to start up
    // get clientId
    // client 0 creates the namespaces
    val coordination = clusterRoot.getOrCreate("coordination")
    val clientId = coordination.registerAndAwait("clientStart", params.numLoadClients)
    if (clientId == 0)
      scadrLoader.createNamespaces
    
    // wait until the namespaces have been created
    coordination.registerAndAwait("startBulkLoad", params.numLoadClients)
    val scadrData = scadrLoader.getData(clientId)

    logger.info("Begining bulk load of data")
    scadrData.load
    logger.info("Bulkloading complete.  Waiting for other loaders to finish")

    coordination.registerAndAwait("bulkLoadComplete", params.numLoadClients)
    if(clientId == 0)
      ExperimentNotification.completions.publish("SCADr Data Load Complete", this.toJson)
  }
}
