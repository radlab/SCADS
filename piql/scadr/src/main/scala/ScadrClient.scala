package edu.berkeley.cs
package scads
package piql
package scadr

import storage.ScadsCluster
import avro.marker._

import org.apache.avro.util._

case class User(var username: String) extends AvroPair {
  var homeTown: String = _
}

case class Thought(var owner: String, var timestamp: Int) extends AvroPair {
  var text: String = _
}

case class Subscription(var owner: String, var target: String) extends AvroPair {
  var approved: Boolean = _
}

case class HashTag(var tag: String, var timestamp: Int, var owner: String) extends AvroPair

class ScadrClient(val cluster: ScadsCluster, executor: QueryExecutor, maxSubscriptions: Int = 5000) {
  val maxResultsPerPage = 10
  implicit val exec = executor

  // namespaces are declared to be lazy so as to allow for manual
  // createNamespace calls to happen first (and after instantiating this
  // class)

  //HACK: typecast to namespace
  lazy val users = cluster.getNamespace[User]("users").asInstanceOf[Namespace]
  lazy val thoughts = cluster.getNamespace[Thought]("thoughts").asInstanceOf[Namespace]
  lazy val subscriptions = cluster.getNamespace[Subscription]("subscriptions").asInstanceOf[Namespace]
  lazy val tags = cluster.getNamespace[HashTag]("tags").asInstanceOf[Namespace]

  /* Optimized queries */
  val findUser = users.where("username".a === (0.?)).toPiql

  /* Old hand coded plans */
  type QueryArgs = Seq[Any]

  private lazy val myThoughtsPlan =
    LocalStopAfter(ParameterLimit(1, maxResultsPerPage),
      IndexScan(thoughts, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), false))
  val myThoughts = (args: QueryArgs) => exec(myThoughtsPlan, args)

  /**
   * Which users are followed by a given user
   */
  private lazy val usersFollowedByPlan =
    IndexLookupJoin(users, Array(AttributeValue(0, 1)),
      IndexScan(subscriptions, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), true))

  private lazy val usersFollowedByStopAfterPlan =
    LocalStopAfter(ParameterLimit(1, maxResultsPerPage), usersFollowedByPlan)

  /**
   * Who am I following?
   */
  def usersFollowedBy = (args: QueryArgs) => exec(usersFollowedByStopAfterPlan, args)


  private lazy val usersFollowingPlan =
    LocalStopAfter(ParameterLimit(1, maxResultsPerPage),
      IndexLookupJoin(users, Array(AttributeValue(0, 1)),
        IndexScan(users, Array(ParameterValue(0)), ParameterLimit(1, maxResultsPerPage), true)))

  /**
   * Who is following ME?
   */
  def usersFollowing = (args: QueryArgs) => exec(usersFollowingPlan, args)

  private lazy val thoughtStreamPlan = // (sub_owner, sub_target, sub_approved), (thought_owner, thought_timestamp, thought_text)
    IndexMergeJoin(thoughts, Array(AttributeValue(0, 1)), Array(AttributeValue(1, 1)), ParameterLimit(1, maxResultsPerPage), false,
      LocalSelection(EqualityPredicate(ConstantValue(true), AttributeValue(0, 2)), // (owner, target), (approved)
        IndexScan(subscriptions, Array(ParameterValue(0)), FixedLimit(maxSubscriptions), true) // (owner, target), (approved)
    ))

  private lazy val thoughtStreamStopAfterPlan =
    LocalStopAfter(ParameterLimit(1, maxResultsPerPage), thoughtStreamPlan)

  def thoughtstream = (args: QueryArgs) => exec(thoughtStreamStopAfterPlan, args)
}
