package edu.berkeley.cs.scads.piql

import edu.berkeley.cs.scads.storage._
import com.googlecode.avro.marker._

import org.apache.avro.util._

case class UserKey(var username: String) extends AvroRecord
case class UserValue(var homeTown: String) extends AvroRecord

case class ThoughtKey(var owner: String, var timestamp: Int) extends AvroRecord
case class ThoughtValue(var text: String) extends AvroRecord

case class SubscriptionKey(var owner: String, var target: String) extends AvroRecord
case class SubscriptionValue(var approved: Boolean) extends AvroRecord

case class HashTagKey(var tag: String, var timestamp: Int, var owner: String) extends AvroRecord
case class HashTagValue() extends AvroRecord

case class UserTarget(var target: String, var owner: String) extends AvroRecord
case class NullRecord(var b: Boolean) extends AvroRecord

class ScadrClient(cluster: ScadsCluster, executor: QueryExecutor, numUsers: Int = 100, thoughtsPerUser: Int = 10, subscriptionsPerUser: Int = 10, tagsPerThought: Int = 5) {
  val maxResultsPerPage = 10
  val maxSubscriptions = 5000

  implicit def toGeneric(ns: SpecificNamespace[_, _]) = ns.genericNamespace

  def randomInts(seed: Int, maxInt: Int, count: Int): Array[Int] = {
    val rand = new scala.util.Random(seed)
    val nums = new Array[Int](count)
    var pos = 0

    while(pos < count) {
      val randNum = rand.nextInt(maxInt)
      if(!(nums contains randNum)) {
        nums(pos) = randNum
        pos += 1
      }
    }
    nums
  }

  //TODO: File bug on scala compiler.  Can't instantiate this class if types are given for the following explicitly
  val userData: Seq[(UserKey, UserValue)] = (1 to numUsers).view.map(i => (UserKey("User" + i), UserValue("hometown" + (i % 10))))
  val thoughtData: Seq[(ThoughtKey, ThoughtValue)] = userData.flatMap(user => (1 to thoughtsPerUser).view.map(i => (ThoughtKey(user._1.username, i), ThoughtValue(user._1.username + " thinks " + i))))
  val subscriptionData: Seq[(SubscriptionKey, SubscriptionValue)] = userData.flatMap(user => randomInts(user._1.username.hashCode, numUsers, thoughtsPerUser).view.map(u => (SubscriptionKey(user._1.username, userData(u)._1.username), SubscriptionValue(true))))
  val idxUsersTargetData: Seq[(UserTarget, NullRecord)] = subscriptionData.map(s => (UserTarget(s._1.target, s._1.owner), NullRecord(true)))

  val users = cluster.getNamespace[UserKey, UserValue]("users")
  val thoughts = cluster.getNamespace[ThoughtKey, ThoughtValue]("thoughts")
  val subscriptions = cluster.getNamespace[SubscriptionKey, SubscriptionValue]("subscriptions")
  val tags = cluster.getNamespace[HashTagKey, HashTagValue]("tags")

  val idxUsersTarget = cluster.getNamespace[UserTarget, NullRecord]("idxUsersTarget")

  def bulkLoadTestData: Unit = {
    users ++= userData
    thoughts ++= thoughtData
    subscriptions ++= subscriptionData
    idxUsersTarget ++= idxUsersTargetData
  }

  def exec(plan: QueryPlan, args: Any*) = {
    val iterator = executor(plan, args:_*)
    iterator.open
    iterator.toList
  }

  val findUserPlan = IndexLookup(users, Array(ParameterValue(0)))
  def findUser(username: String): QueryResult =
    exec(findUserPlan, new Utf8(username))

  val myThoughtsPlan =
    StopAfter(ParameterLimit(1, maxResultsPerPage),
      IndexScan(thoughts, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), false)
    )
  def myThoughts(username: String, count: Int): QueryResult =
    exec(myThoughtsPlan, new Utf8(username), count)

  val usersFollowedByPlan =
    StopAfter(ParameterLimit(1, maxResultsPerPage),
      IndexLookupJoin(users, Array(AttributeValue(0, 1)),
        IndexScan(subscriptions, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), true)
      )
    )
  def usersFollowedBy(username: String, count: Int): QueryResult =
    exec(usersFollowedByPlan, new Utf8(username), count)

  val usersFollowingPlan =
    StopAfter(ParameterLimit(1, maxResultsPerPage),
      IndexLookupJoin(users, Array(AttributeValue(0, 1)),
        IndexScan(idxUsersTarget, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), true)
      )
    )
  def usersFollowing(username: String, count: Int): QueryResult =
    exec(usersFollowingPlan, new Utf8(username), count)

  val thoughtStreamPlan =
    StopAfter(ParameterLimit(1, maxResultsPerPage),
      IndexMergeJoin(thoughts, Array(AttributeValue(0, 1)), Array(AttributeValue(2, 1)), ParameterLimit(1, maxResultsPerPage), false,
        Selection(Equality(FixedValue(true), AttributeValue(1, 0)),
          IndexScan(subscriptions, Array(ParameterValue(0)), FixedLimit(maxSubscriptions), true)
        )
      )
    )
  def thoughtstream(username: String, count: Int): QueryResult =
    exec(thoughtStreamPlan, new Utf8(username), count)

  /* SELECT thoughts.*
     FROM thoughts
       JOIN tags ON thoughts.owner = tags.owner AND thoughts.timestamp = tags.timestamp
     WHERE tags.tag = [1: tag]
     ORDER BY timestamp DESC */
  val thoughsByHashTagPlan =
    StopAfter(ParameterLimit(1, maxResultsPerPage),
      IndexLookupJoin(thoughts, Array(AttributeValue(0, 1), AttributeValue(0,2)),
        IndexScan(tags, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), false)
      )
    )
  def thoughtsByHashTag(tag: String, count: Int): QueryResult =
    exec(thoughsByHashTagPlan, new Utf8(tag), count)
}
