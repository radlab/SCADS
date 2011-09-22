package edu.berkeley.cs
package scads
package piql
package modeling

import storage._
import piql.opt._
import piql.plans._
import piql.scadr._
import perf.scadr._

import net.lag.logging.Logger
import scala.util.Random
import scala.collection.mutable.ArrayBuffer

//move to scadr data
abstract trait UserGenerator {
  val numUsers: Int

  protected final def toUserName(idx: Int) = "User%010d".format(idx)
  protected final def randomUserName(rand: Random) = toUserName(rand.nextInt(numUsers) + 1)
}

class ScadrQueryProvider extends QueryProvider {
  val logger = Logger()

  def getQueryList(cluster: ScadsCluster, executor: QueryExecutor): IndexedSeq[QuerySpec] = {
    implicit val exec = executor
    val clusterConfig = cluster.root.awaitChild("clusterReady")
    val scadrClient = new ScadrClient(cluster, executor)
    val loaderConfig = classOf[ScadrLoaderTask].newInstance.parse(clusterConfig.data)
    val maxUser = (loaderConfig.numServers / loaderConfig.replicationFactor) * loaderConfig.usersPerServer
    logger.info("Initalizing ScadrQueryProvider with %d users", maxUser)

    val randomUser = RandomUser(maxUser)
    val perPage = CardinalityList(10 to 50 by 10 toIndexedSeq)
    val numSubscriptions = CardinalityList(10 to 50 by 10 toIndexedSeq)

    val localSubscriptionList = RandomSubscriptionList(maxUser, numSubscriptions.values)

    // index lookup
    /*
    val findSubscription = (
      subscriptions.where("subscriptions.owner".a === (0.?))
  		 .where("subscriptions.target".a === (1.?))
      ).toPiql("findSubscription")
    */
    val indexLookupSubscriptions = new OptimizedQuery(
      "indexLookupSubscriptions",
      IndexLookup(
        scadrClient.subscriptions,
        (0.?) :: Nil
      ),
      executor
    )
    
    val indexLookupUsers = new OptimizedQuery(
      "indexLookupUsers",
      IndexLookup(
        scadrClient.users,
        (0.?) :: Nil
      ),
      executor
    )
    
    // index scan
    val indexScanSubscriptions = scadrClient.subscriptions.where("owner".a === (0.?))
						  .limit(1.?, 10000)
						  .toPiql("indexScanSubscriptions")

    val indexScanThoughts = scadrClient.thoughts.where("owner".a === (0.?))
						  .limit(1.?, 10000)
						  .toPiql("indexScanThoughts")


    // index lookup join
    val indexLookupJoinUsers = new OptimizedQuery("indexLookupJoinUsers",
					      IndexLookupJoin(
						      scadrClient.users,
						      AttributeValue(0,1) :: Nil,
						      LocalIterator(0)),
					      executor)

    // index merge join
    val indexMergeJoinThoughts = new OptimizedQuery("indexMergeJoinThoughts",
					     LocalStopAfter(
					       ParameterLimit(1, 10000),
					       IndexMergeJoin(
						       scadrClient.thoughts,
						       AttributeValue(0,1) :: Nil,
						       AttributeValue(1,1) :: Nil,
						       ParameterLimit(1, 10000),
						       false,
						       LocalIterator(0))),
					     executor)

    /*
    QuerySpec(indexLookupJoinQuery, localSubscriptionList :: Nil) ::
    QuerySpec(indexMergeQuery, localSubscriptionList :: perPage :: Nil) ::
    QuerySpec(indexScanQuery, randomUser :: numSubscriptions :: Nil) ::
    QuerySpec(indexScanQuery, randomUser :: perPage :: Nil) ::
    */
    // iterators
    //QuerySpec(indexLookupSubscriptions, randomUser :: randomUser :: Nil) :: // findSubscription
    QuerySpec(indexLookupUsers, randomUser :: Nil) :: // findUser
    QuerySpec(indexScanSubscriptions, randomUser :: perPage :: Nil) :: // usersFollowedBy, thoughtstream
    QuerySpec(indexScanThoughts, randomUser :: perPage :: Nil) :: // myThoughts
    QuerySpec(indexLookupJoinUsers, localSubscriptionList :: Nil) :: // usersFollowedBy
    QuerySpec(indexMergeJoinThoughts, localSubscriptionList :: perPage :: Nil) :: // thoughtstream
    // queries
    //QuerySpec(scadrClient.findSubscription, randomUser :: randomUser :: Nil) :: // MA says:  don't include
    QuerySpec(scadrClient.findUser, randomUser :: Nil) ::
    QuerySpec(scadrClient.myThoughts, randomUser :: perPage :: Nil) ::
    QuerySpec(scadrClient.thoughtstream,
	      RandomUserWithSubscriptionCardinality(maxUser,
						    loaderConfig.followingCardinality,
						    numSubscriptions.values) :: perPage :: Nil) ::
    QuerySpec(scadrClient.usersFollowedBy, randomUser :: perPage :: Nil) :: Nil toIndexedSeq
    //QuerySpec(scadrClient.usersFollowing, randomUser :: perPage :: Nil) :: // commented out by MA b/c doesn't work
  }

  //todo objects?
  case class RandomUser(numUsers: Int) extends ParameterGenerator with UserGenerator {
    // must be + 1 since users are indexed startin g from 1
    final def getValue(rand: Random) = (randomUserName(rand), None)
  }

  case class RandomUserWithSubscriptionCardinality(numUsers: Int, maxCardinality: Int, cardinalityList: IndexedSeq[Int]) extends ParameterGenerator with UserGenerator {
    final def getValue(rand: Random) = {
      val cardinality = cardinalityList(rand.nextInt(cardinalityList.size))
      val userId = scala.util.Random.nextInt(numUsers) / maxCardinality * maxCardinality + cardinality
      (toUserName(userId), Some(cardinality))
    }
  }

  case class RandomSubscriptionList(numUsers: Int, cardinalityList: IndexedSeq[Int]) extends ParameterGenerator with UserGenerator {
    protected final def randSubscription(rand: Random) = {
      val s = new Subscription(randomUserName(rand), randomUserName(rand))
      s.approved = true
      s
    }

    final def getValue(rand: Random) = {
      val numTuples = cardinalityList(rand.nextInt(cardinalityList.size))
      (ArrayBuffer.fill(numTuples)(ArrayBuffer(randSubscription(rand))), Some(numTuples))
    }
  }
}



