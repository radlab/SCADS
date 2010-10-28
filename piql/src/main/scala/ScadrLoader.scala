package edu.berkeley.cs.scads.piql

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker._

import org.apache.avro.util._

import edu.berkeley.cs.scads.piql.DataGenerator._

import scala.collection.mutable.HashSet

import net.lag.logging.Logger

case class ScadrKeySplits(
    usersKeySplits: Seq[(Option[UserKey], Seq[StorageService])],
    thoughtsKeySplits: Seq[(Option[ThoughtKey], Seq[StorageService])],
    subscriptionsKeySplits: Seq[(Option[SubscriptionKey], Seq[StorageService])],
    tagsKeySplits: Seq[(Option[HashTagKey], Seq[StorageService])],
    idxUsersTargetKeySplits: Seq[(Option[UserTarget], Seq[StorageService])])

/**
 * Currently the loader assumes all nodes are equal and tries to distribute
 * evenly among the nodes with no preferences for any particular ones.
 */
class ScadrLoader(val client: ScadrClient,
                  val replicationFactor: Int,
                  val numClients: Int, // number of clients to split the loading by
                  val numUsers: Int = 100,
                  val numThoughtsPerUser: Int = 10,
                  val numSubscriptionsPerUser: Int = 10,
                  val numTagsPerThought: Int = 5) {

  require(client != null)
  require(replicationFactor >= 1)
  require(numUsers >= 0)
  require(numThoughtsPerUser >= 0)
  require(numSubscriptionsPerUser >= 0)
  require(numTagsPerThought >= 0)

  val logger = Logger()


  def createNamespaces() {
    val splits = keySplits
    logger.info("Creating namespaces with keysplits: %s", splits)
    client.cluster.createNamespace[UserKey, UserValue]("users", splits.usersKeySplits)
    client.cluster.createNamespace[ThoughtKey, ThoughtValue]("thoughts", splits.thoughtsKeySplits)
    client.cluster.createNamespace[SubscriptionKey, SubscriptionValue]("subscriptions", splits.subscriptionsKeySplits)
    client.cluster.createNamespace[HashTagKey, HashTagValue]("tags", splits.tagsKeySplits)
    client.cluster.createNamespace[UserTarget, NullRecord]("idxUsersTarget", splits.idxUsersTargetKeySplits)
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
  def keySplits: ScadrKeySplits = {
    val servers = client.cluster.getAvailableServers
    val clusterSize = servers.size

    // TODO: not sure what to do here - should we just have some nodes
    // duplicate user key ranges?
    if (clusterSize > numUsers)
      throw new RuntimeException("More clusters than users- don't know how to make key split")

    val usersPerNode = numUsers / clusterSize
    val usersIdxs = None +: (1 until clusterSize).map(i => Some(i * usersPerNode + 1))

    val usersKeySplits = usersIdxs.map(_.map(idx => UserKey(toUser(idx))))
    val thoughtsKeySplits = usersIdxs.map(_.map(idx => ThoughtKey(toUser(idx), 0)))
    val subscriptionsKeySplits = usersIdxs.map(_.map(idx => SubscriptionKey(toUser(idx), "")))
    val idxUsersTargetKeySplits = usersIdxs.map(_.map(idx => UserTarget(toUser(idx), "")))

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

    val tagsKeySplits = None +: (1 until clusterSize).map(i => Some(HashTagKey(toKeyString(i * numPerNode), 0, "")))

    val services = (0 until clusterSize).map(i => (0 until replicationFactor).map(j => servers((i + j) % clusterSize)))

    ScadrKeySplits(usersKeySplits zip services,
                   thoughtsKeySplits zip services,
                   subscriptionsKeySplits zip services,
                   tagsKeySplits zip services,
                   idxUsersTargetKeySplits zip services)
  }

  case class ScadrData(userData: Seq[(UserKey, UserValue)],
                       thoughtData: Seq[(ThoughtKey, ThoughtValue)],
                       subscriptionData: Seq[(SubscriptionKey, SubscriptionValue)],
                       tagData: Seq[(HashTagKey, HashTagValue)],
                       idxUsersTargetData: Seq[(UserTarget, NullRecord)]) {
    def load() {
      logger.info("Loading users")
      client.users ++= userData
      logger.info("Loading thoughts")
      client.thoughts ++= thoughtData
      logger.info("Loading subscriptions")
      client.subscriptions ++= subscriptionData
      logger.info("Loading tags")
      client.tags ++= tagData
      logger.info("Loading usersidx")
      client.idxUsersTarget ++= idxUsersTargetData
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

    val userData: Seq[(UserKey, UserValue)] = 
      newUserIdView.map(i => (UserKey(toUser(i)), UserValue("hometown" + (i % 10))))

    val thoughtData: Seq[(ThoughtKey, ThoughtValue)] = 
      newUserIdView.flatMap(userId => (1 to numThoughtsPerUser).view.map(i => (ThoughtKey(toUser(userId), i), ThoughtValue(toUser(userId) + " thinks " + i))))

    val subscriptionData: Seq[(SubscriptionKey, SubscriptionValue)] = 
      newUserIdView.flatMap(userId => randomInts(toUser(userId).hashCode, numUsers, numSubscriptionsPerUser).map(u => (SubscriptionKey(toUser(userId), toUser(u + 1)), SubscriptionValue(true))))

    val idxUsersTargetData: Seq[(UserTarget, NullRecord)] = 
      subscriptionData.map(s => (UserTarget(s._1.target, s._1.owner), NullRecord(true)))

    val tagData: Seq[(HashTagKey, HashTagValue)] =
      thoughtData.flatMap(thought => randomStrings(thought._1.owner.hashCode, numTagsPerThought).map(tag => (HashTagKey(tag, thought._1.timestamp, thought._1.owner), HashTagValue(false))))

    //val thoughtData: Seq[(ThoughtKey, ThoughtValue)] = userData.flatMap(user => (1 to numThoughtsPerUser).view.map(i => (ThoughtKey(user._1.username, i), ThoughtValue(user._1.username + " thinks " + i))))
    //val subscriptionData: Seq[(SubscriptionKey, SubscriptionValue)] = userData.flatMap(user => randomInts(user._1.username.hashCode, numUsers, numSubscriptionsPerUser).view.map(u => (SubscriptionKey(user._1.username, userData(u)._1.username), SubscriptionValue(true))))

    ScadrData(userData, thoughtData, subscriptionData, tagData, idxUsersTargetData)
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
