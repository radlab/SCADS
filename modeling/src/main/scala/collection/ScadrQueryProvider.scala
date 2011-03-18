package edu.berkeley.cs
package scads
package piql
package modeling

import storage._
import piql.scadr._
import perf.scadr._

case class ScadrUserGenerator(numUsers: Int) extends ParameterGenerator {
  private final def toUser(idx: Int) = "User%010d".format(idx)
  
  // must be + 1 since users are indexed startin g from 1
  final def getValue = toUser(scala.util.Random.nextInt(numUsers) + 1) 
}

class ScadrQueryProvider extends QueryProvider {
  def getQueryList(cluster: ScadsCluster, executor: QueryExecutor): IndexedSeq[QuerySpec] = {
    val scadrClient = new ScadrClient(cluster, executor)
    val clusterConfig = cluster.root.awaitChild("clusterReady")
    val loaderConfig = classOf[ScadrLoaderTask].newInstance.parse(clusterConfig.data)
    val randomUser = ScadrUserGenerator(loaderConfig.numServers *
					loaderConfig.usersPerServer)
    val perPage = CardinalityList(10 to 50 by 10 toIndexedSeq)
    val numSubscriptions = CardinalityList(100 to 500 by 100 toIndexedSeq)

    QuerySpec(scadrClient.findUser, randomUser :: Nil) ::
    QuerySpec(scadrClient.myThoughts, randomUser :: perPage :: Nil) ::
    QuerySpec(scadrClient.usersFollowedBy, randomUser :: perPage :: Nil) ::
    QuerySpec(scadrClient.thoughtstream, randomUser :: numSubscriptions :: perPage :: Nil)::
    QuerySpec(scadrClient.usersFollowing, randomUser :: perPage :: Nil) ::
    QuerySpec(scadrClient.findSubscription, randomUser :: randomUser :: Nil) :: Nil toIndexedSeq
  }
}



