package edu.berkeley.cs.scads.piql

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker._

import org.apache.avro.util._

case class UserKey(var username: String) extends AvroRecord
case class UserValue(var homeTown: String) extends AvroRecord

case class ThoughtKey(var owner: String, var timestamp: Int) extends AvroRecord
case class ThoughtValue(var text: String) extends AvroRecord

case class SubscriptionKey(var owner: String, var target: String) extends AvroRecord
case class SubscriptionValue(var approved: Boolean) extends AvroRecord

case class HashTagKey(var tag: String, var timestamp: Int, var owner: String) extends AvroRecord
case class HashTagValue(var dummy: Boolean) extends AvroRecord

case class UserTarget(var target: String, var owner: String) extends AvroRecord
case class NullRecord(var b: Boolean) extends AvroRecord

class ScadrClient(val cluster: ScadsCluster, executor: QueryExecutor, maxSubscriptions: Int = 5000) {
  val maxResultsPerPage = 10

  implicit def toGeneric(ns: SpecificNamespace[_, _]) = ns.genericNamespace

  // namespaces are declared to be lazy so as to allow for manual
  // createNamespace calls to happen first (and after instantiating this
  // class)

  lazy val users = cluster.getNamespace[UserKey, UserValue]("users")
  lazy val thoughts = cluster.getNamespace[ThoughtKey, ThoughtValue]("thoughts")
  lazy val subscriptions = cluster.getNamespace[SubscriptionKey, SubscriptionValue]("subscriptions")
  lazy val tags = cluster.getNamespace[HashTagKey, HashTagValue]("tags")

  lazy val idxUsersTarget = cluster.getNamespace[UserTarget, NullRecord]("idxUsersTarget")

  private def exec(plan: QueryPlan, args: Any*) = {
    val iterator = executor(plan, args:_*)
    iterator.open
    iterator.toSeq
  }

  private lazy val findUserPlan = IndexLookup(users, Array(ParameterValue(0)))
  def findUser(username: String): QueryResult =
    exec(findUserPlan, new Utf8(username))

  private lazy val myThoughtsPlan =
    StopAfter(ParameterLimit(1, maxResultsPerPage),
      IndexScan(thoughts, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), false)
    )
  def myThoughts(username: String, count: Int): QueryResult =
    exec(myThoughtsPlan, new Utf8(username), count)

  private lazy val usersFollowedByPlan =
    IndexLookupJoin(users, Array(AttributeValue(0, 1)),
      IndexScan(subscriptions, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), true)
    )
  
  private lazy val usersFollowedByStopAfterPlan =
    StopAfter(ParameterLimit(1, maxResultsPerPage), usersFollowedByPlan)

  /**
   * Who am I following?
   */
  def usersFollowedBy(username: String, count: Int): QueryResult =
    exec(usersFollowedByStopAfterPlan, new Utf8(username), count)

  def usersFollowedByPaginate(username: String, count: Int): PageResult = {
    val iterator = executor(usersFollowedByPlan, new Utf8(username), count)
    val res = new PageResult(iterator, count) 
    res.open
    res
  }

  private lazy val usersFollowingPlan =
    StopAfter(ParameterLimit(1, maxResultsPerPage),
      IndexLookupJoin(users, Array(AttributeValue(0, 1)),
        IndexScan(idxUsersTarget, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), true)
      )
    )

  /**
   * Who is following ME?
   */
  def usersFollowing(username: String, count: Int): QueryResult =
    exec(usersFollowingPlan, new Utf8(username), count)

  private lazy val thoughtStreamPlan =   // (sub_owner, sub_target), (sub_approved), (thought_owner, thought_timestamp), (thought_text)
    IndexMergeJoin(thoughts, Array(AttributeValue(0, 1)), Array(AttributeValue(2, 1)), ParameterLimit(1, maxResultsPerPage), false,
      Selection(Equality(FixedValue(true), AttributeValue(1, 0)), // (owner, target), (approved)
        IndexScan(subscriptions, Array(ParameterValue(0)), FixedLimit(maxSubscriptions), true) // (owner, target), (approved)
      )
    )

  private lazy val thoughtStreamStopAfterPlan = 
    StopAfter(ParameterLimit(1, maxResultsPerPage), thoughtStreamPlan)

  def thoughtstream(username: String, count: Int): QueryResult =
    exec(thoughtStreamStopAfterPlan, new Utf8(username), count)

  def thoughtstreamPaginate(username: String, count: Int): PageResult = {
    val iterator = executor(thoughtStreamPlan, new Utf8(username), count)
    val res = new PageResult(iterator, count) 
    res.open
    res
  }

  /* SELECT thoughts.*
     FROM thoughts
       JOIN tags ON thoughts.owner = tags.owner AND thoughts.timestamp = tags.timestamp
     WHERE tags.tag = [1: tag]
     ORDER BY timestamp DESC */
  private lazy val thoughsByHashTagPlan =
    StopAfter(ParameterLimit(1, maxResultsPerPage),
      IndexLookupJoin(thoughts, Array(AttributeValue(0, 2), AttributeValue(0, 1)),
        IndexScan(tags, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), false)
      )
    )
  def thoughtsByHashTag(tag: String, count: Int): QueryResult =
    exec(thoughsByHashTagPlan, new Utf8(tag), count)


  def saveThought(thoughtKey: ThoughtKey, thoughtValue: ThoughtValue) {
    thoughts.put(thoughtKey, thoughtValue)
  }

}
