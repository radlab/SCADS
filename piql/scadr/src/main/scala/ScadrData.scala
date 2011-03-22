package edu.berkeley.cs
package scads
package piql
package scadr

import comm._
import storage._
import avro.marker._

import org.apache.avro.util._

import edu.berkeley.cs.scads.piql.DataGenerator._

import scala.collection.mutable.HashSet

import net.lag.logging.Logger
import org.apache.avro.generic.GenericRecord

case class ScadrKeySplits(
    usersKeySplits: Seq[Option[GenericRecord]],
    thoughtsKeySplits: Seq[Option[GenericRecord]],
    subscriptionsKeySplits: Seq[Option[GenericRecord]]
)

/**
 * Currently the loader assumes all nodes are equal and tries to distribute
 * evenly among the nodes with no preferences for any particular ones.
 */
class ScadrLoader(val replicationFactor: Int,
                  val numClients: Int, // number of clients to split the loading by
                  val numUsers: Int = 100,
                  val numThoughtsPerUser: Int = 10,
                  val numSubscriptionsPerUser: Int = 10,
                  val numTagsPerThought: Int = 5) {

  require(replicationFactor >= 1)
  require(numUsers >= 0)
  require(numThoughtsPerUser >= 0)
  require(numSubscriptionsPerUser >= 0)
  require(numTagsPerThought >= 0)

  val logger = Logger()


  protected def createNamespace(namespace: PairNamespace[AvroPair], keySplits: Seq[Option[GenericRecord]]): Unit = {
    val services = namespace.cluster.getAvailableServers.grouped(replicationFactor).toSeq
    val partitionScheme = keySplits.map(_.map(namespace.keyToBytes)).zip(services)
    namespace.setPartitionScheme(partitionScheme)
  }


  def createNamespaces(cluster: ScadsCluster) {
    val splits = keySplits(cluster.getAvailableServers.size)
    logger.info("Creating namespaces with keysplits: %s", splits)
    createNamespace(cluster.getNamespace[User]("users").asInstanceOf[PairNamespace[AvroPair]], splits.usersKeySplits)
    createNamespace(cluster.getNamespace[Thought]("thoughts").asInstanceOf[PairNamespace[AvroPair]], splits.thoughtsKeySplits)
    createNamespace(cluster.getNamespace[Subscription]("subscriptions").asInstanceOf[PairNamespace[AvroPair]], splits.subscriptionsKeySplits)
  }

  private def toUser(idx: Int) = "User%010d".format(idx)
  def randomUser = toUser(scala.util.Random.nextInt(numUsers) + 1) // must be + 1 since users are indexed starting from 1

  /**
   * Get the key splits based on the num* parameters and the scads cluster.
   * The number of nodes on the cluster is determined by calling
   * getAvailableServers on the scads cluster.
   *
   * We assume uniform distribution over subscriptions
   */
  def keySplits(clusterSize: Int): ScadrKeySplits = {
    // TODO: not sure what to do here - should we just have some nodes
    // duplicate user key ranges?
    if (clusterSize > numUsers)
      throw new RuntimeException("More clusters than users- don't know how to make key split")

    require(clusterSize % replicationFactor == 0, "numServers must be divisible by by replicationFactor")
    val usersPerNode = numUsers / (clusterSize / replicationFactor)
    val usersIdxs = None +: (1 until clusterSize).map(i => Some(i * usersPerNode + 1))

    val usersKeySplits = usersIdxs.map(_.map(idx => User(toUser(idx)))).map(_.map(_.key))
    val thoughtsKeySplits = usersIdxs.map(_.map(idx => Thought(toUser(idx), 0))).map(_.map(_.key))
    val subscriptionsKeySplits = usersIdxs.map(_.map(idx => Subscription(toUser(idx), ""))).map(_.map(_.key))

    // assume uniform distribution of tags over 8 bit ascii - not really
    // ideal, but we can generate the data such that this is true

    var size = 256
    while (size < clusterSize)
      size = size * 256

    val numPerNode = size / clusterSize
    assert(numPerNode >= 1)

    // encodes i as a base 256 string. not super efficient
    def toKeyString(i: Int): String = i match {
      case 0 => ""
      case _ => toKeyString(i / 256) + (i % 256).toChar
    }

    ScadrKeySplits(usersKeySplits,
                   thoughtsKeySplits,
                   subscriptionsKeySplits)
  }

  case class ScadrData(userData: Seq[User],
                       thoughtData: Seq[Thought],
                       subscriptionData: Seq[Subscription]) {
    def load(client: ScadrClient) {
      logger.info("Loading users")
      client.users ++= userData
      logger.info("Loading thoughts")
      client.thoughts ++= thoughtData
      logger.info("Loading subscriptions")
      client.subscriptions ++= subscriptionData
    }
  }

  /**
   * Makes a slice of data from [startUser, endUser). Checks to make sure
   * first that start/end are valid for the given loader. Does not allow for
   * empty slices  (so startUser &lt; endUser is required). Note that users
   * are indexed by 1 (so a valid start user goes from 1 to numUsers, and a
   * valid end user goes from 2 to numUsers + 1).
   */
  private def makeScadrData(startUser: Int, endUser: Int): ScadrData = {
    require(1 <= startUser && startUser <= numUsers)
    require(1 <= endUser && endUser <= numUsers + 1)
    require(startUser < endUser)

    // create lazy views on the data

    def newUserIdView =
      (startUser until endUser).view

    val userData: Seq[User] =
      newUserIdView.map(i => {
  val u = User(toUser(i))
  u.homeTown = "hometown" + (i % 10)
  u.password = "secret"
  u
      })

    val thoughtData: Seq[Thought] =
      newUserIdView.flatMap(userId =>
  (1 to numThoughtsPerUser).view.map(i => {
    val t = Thought(toUser(userId), i)
    t.text = toUser(userId) + " thinks " + i
    t
  })
      )

    val subscriptionData: Seq[Subscription] = userData.flatMap(user =>
      randomInts(user.username.hashCode, numUsers, numSubscriptionsPerUser).view.map(u => {
  //val s = Subscription(user.username, userData(u).username)
  val s = Subscription(user.username, "User%010d".format(u))
  s.approved = true
  s
      })
    )

    ScadrData(userData, thoughtData, subscriptionData)
  }

  /**
   * Clients are 0-indexed
   */
  def getData(clientId: Int): ScadrData = {
    require(clientId >= 0 && clientId < numClients)

    // TODO: fix this
    if (numClients > numUsers)
      throw new RuntimeException("More clients than user keys - don't know how to partition load")

    val usersPerClient = numUsers / numClients
    val startIdx = clientId * usersPerClient + 1
    makeScadrData(startIdx, startIdx + usersPerClient)
  }

}
