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

case class UserTarget(var target: String) extends AvroRecord

class ScadrClient(cluster: ScadsCluster, executor: QueryExecutor) {
  val maxResultsPerPage = 10
  val maxSubscriptions = 5000

  implicit def toGeneric(ns: SpecificNamespace[_, _]) = ns.genericNamespace

  val users = cluster.getNamespace[UserKey, UserValue]("users")
  val thoughts = cluster.getNamespace[ThoughtKey, ThoughtValue]("thoughts")
  val subscriptions = cluster.getNamespace[SubscriptionKey, SubscriptionValue]("subscriptions")
  val tags = cluster.getNamespace[HashTagKey, HashTagValue]("tags")

  val idxUsersTarget = cluster.getNamespace[UserTarget, UserKey]("idxUsersTarget")

  val findUserPlan = IndexLookup(users, Array(ParameterValue(0)))
  def findUser(username: String): QueryResult =
    executor(findUserPlan, new Utf8(username)).toList

  val myThoughtsPlan = IndexScan(thoughts, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), true)
  def myThoughts(username: String, count: Int): QueryResult =
    executor(myThoughtsPlan, username, count).toList

  val usersFollowedByPlan =
    IndexLookupJoin(users, Array(AttributeValue(0, 1)),
      IndexScan(subscriptions, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), true)
    )
  def usersFollowedBy(username: String, count: Int): QueryResult =
    executor(usersFollowedByPlan, username, count).toList

  val usersFollowingPlan =
    IndexLookupJoin(users, Array(AttributeValue(1, 0)),
      SequentialDereferenceIndex(subscriptions,
        IndexScan(idxUsersTarget, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), true)
      )
    )
  def usersFollowing(username: String, count: Int): QueryResult =
    executor(usersFollowingPlan, username, count).toList

  val thoughtStreamPlan =
    IndexMergeJoin(thoughts, Array(AttributeValue(0, 1)), Array("timestamp"), ParameterLimit(1, maxResultsPerPage), false,
      Selection(Equality(FixedValue(true), AttributeValue(1, 0)),
        IndexScan(subscriptions, Array(ParameterValue(0)), FixedLimit(maxSubscriptions), true)
      )
    )
  def thoughtstream(username: String, count: Int): QueryResult =
    executor(thoughtStreamPlan, username, count).toList

  /* SELECT thoughts.*
     FROM thoughts
       JOIN tags ON thoughts.owner = tags.owner AND thoughts.timestamp = tags.timestamp
     WHERE tags.tag = [1: tag]
     ORDER BY timestamp DESC */
  val thoughsByHashTagPlan =
    IndexLookupJoin(thoughts, Array(AttributeValue(0, 1), AttributeValue(0,2)),
      IndexScan(tags, Array(ParameterValue(0)), ParameterLimit(1, 10), false)
    )
  def thoughtsByHashTag(tag: String, count: Int): QueryResult =
    executor(thoughsByHashTagPlan, tag, count).toList
}
