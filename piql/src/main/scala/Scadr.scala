package edu.berkeley.cs.scads.piql

import edu.berkeley.cs.scads.storage._
import com.googlecode.avro.marker._

class ScadrClient(cluster: ScadsCluster) extends PhysicalOperators {
  val maxResultsPerPage = 10
  val maxSubscriptions = 5000

  case class UserKey(var username: String) extends AvroRecord
  case class UserValue(var homeTown: String) extends AvroRecord

  case class ThoughtKey(var owner: String, var timestamp: Int) extends AvroRecord
  case class ThoughtValue(var text: String) extends AvroRecord

  case class SubscriptionKey(var owner: String, var target: String) extends AvroRecord
  case class SubscriptionValue(var approved: Boolean) extends AvroRecord

  /* TODO: Fix variance of namespace to remove cast */
  val users = cluster.getNamespace[UserKey, UserValue]("users").asInstanceOf[Namespace]
  val thoughts = cluster.getNamespace[ThoughtKey, ThoughtValue]("thoughts").asInstanceOf[Namespace]
  val subscriptions = cluster.getNamespace[SubscriptionKey, SubscriptionValue]("subscriptions").asInstanceOf[Namespace]

  case class UserTarget(var target: String) extends AvroRecord
  val idxUsersTarget = cluster.getNamespace[UserTarget, UserKey]("idxUsersTarget").asInstanceOf[Namespace]

  val findUserPlan = IndexLookup(users, Array(ParameterValue(0)))
  def findUser(username: String): QueryResult =
    findUserPlan.execute(Array[Any](username))

  val myThoughtsPlan = IndexScan(thoughts, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), true)
  def myThoughts(username: String, count: Int): QueryResult = {
    myThoughtsPlan.execute(Array[Any](username, count))
  }

  val myFollowersPlan =
    IndexLookupJoin(users, Array(AttributeValue("target")),
      IndexScan(subscriptions, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), true)
    )
  def myFollowers(username: String, count: Int): QueryResult = {
    myThoughtsPlan.execute(Array[Any](username, count))
  }

  val myFollowingPlan =
    IndexLookupJoin(users, Array(AttributeValue("target")),
      IndexScan(idxUsersTarget, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), true)
    )
  def myFollowing(username: String, count: Int): QueryResult = {
    myFollowingPlan.execute(Array[Any](username, count))
  }

  val thoughtStreamPlan =
    IndexMergeJoin(thoughts, Array(AttributeValue("target")), Array("timestamp"), ParameterLimit(1, maxResultsPerPage), false,
      Selection(Equality(FixedValue(true), AttributeValue("approved")),
        IndexScan(subscriptions, Array(ParameterValue(0)), FixedLimit(maxSubscriptions), true)
      )
    )
  def thoughtstream(username: String, count: Int): QueryResult = {
    thoughtStreamPlan.execute(Array[Any](username, count))
  }
}
