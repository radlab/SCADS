package edu.berkeley.cs
package scads
package piql
package modeling

import deploylib.mesos._
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

    /* load data */
    println("loading data...")
    val scadrClient = new ScadrClient(cluster, new SimpleExecutor)
    //val scadrLoader = new ScadrLoader(scadrClient, 1, 1)
    val scadrLoader = new ScadrLoader(
      scadrClient, //val client: ScadrClient,
      1, //val replicationFactor: Int,
      1, //val numClients: Int, // number of clients to split the loading by
      params.numUsers, //val numUsers: Int = 100,
      params.numThoughtsPerUser, //val numThoughtsPerUser: Int = 10,
      params.numSubscriptionsPerUser, //val numSubscriptionsPerUser: Int = 10,
      5 //val numTagsPerThought: Int = 5) // I don't think this is implemented
    )
    scadrLoader.createNamespaces
    val scadrData = scadrLoader.getData(0)  // check sizes
    println("user data size: " + scadrData.userData.size)
    println("thought data size: " + scadrData.thoughtData.size)
    println("subscription data size: " + scadrData.subscriptionData.size)
    scadrData.load

    println("users:  " + scadrClient.users.getRange(None, None).size)
    println("thoughts:  " + scadrClient.thoughts.getRange(None, None).size)
    println("subscriptions:  " + scadrClient.subscriptions.getRange(None, None).size)
    println("done!")
  }
}