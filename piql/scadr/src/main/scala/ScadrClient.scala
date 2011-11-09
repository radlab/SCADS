package edu.berkeley.cs
package scads
package piql
package scadr

import storage.ScadsCluster
import avro.marker._

import org.apache.avro.util._

case class User(var username: String) extends AvroPair {
  var homeTown: String = _
  var password: String = _
}

case class Thought(var owner: String, var timestamp: Int) extends AvroPair {
  var text: String = _
}

case class Subscription(var owner: String, var target: String) extends AvroPair {
  var approved: Boolean = _
}

class ScadrClient(val cluster: ScadsCluster, executor: QueryExecutor = new ParallelExecutor) {
  val maxSubscriptions = 10000
  val maxResultsPerPage = 10000
  implicit val exec = executor

  // namespaces are declared to be lazy so as to allow for manual
  // createNamespace calls to happen first (and after instantiating this
  // class)

  //HACK: typecast to namespace
  val users = cluster.getNamespace[User]("users")
  val thoughts = cluster.getNamespace[Thought]("thoughts")
  val subscriptions = cluster.getNamespace[Subscription]("subscriptions")

  val namespaces = List(users, thoughts, subscriptions)
  //val allNamespaces = namespaces.flatMap(ns => ns :: ns.listIndexes.map(_._2).toList)

  /* Optimized queries */
  val findUser = users.where("username".a === (0.?)).toPiql("findUser")

  val myThoughts = (
    thoughts.where("thoughts.owner".a === (0.?))
      .sort("thoughts.timestamp".a :: Nil, false)
      .limit(1.?, maxResultsPerPage)
    ).toPiql("myThoughts")

  val usersFollowedBy = (
    subscriptions.where("subscriptions.owner".a === (0.?))
      .limit(1.?, maxResultsPerPage)
      .join(users)
      .where("subscriptions.target".a === "users.username".a)
    ).toPiql("usersFollowedBy")


  val thoughtstream = (
    subscriptions.where("subscriptions.owner".a === (0.?))
      .dataLimit(maxSubscriptions)
      .where("subscriptions.approved".a === true)
      .join(thoughts)
      .where("thoughts.owner".a === "subscriptions.target".a)
      .sort("thoughts.timestamp".a :: Nil, false)
      .limit(1.?, maxResultsPerPage)
    ).toPiql("thoughtstream")

  val tsAddThoughtDelta =
    subscriptions.where("subscriptions.target".a === "@t.owner".a)
      .where("subscriptions.approved".a === true)
      .select("subscription.owner".a, "@t.timestamp".a, "@t.owner".a)

  val tsAddSubscriptionDelta =
    thoughts.where("thoughts.owner".a === "@s.target".a)
      .where("@s.approved".a === true)
      .select("@s.owner".a, "thoughts.timestamp".a, "thoughts.owner".a)
      .sort("thoughts.timestamp".a :: Nil, false)
      .limit(1.?, maxResultsPerPage)

  /**
   * Who is following ME?
   */
  val usersFollowing = (
    subscriptions.where("subscriptions.target".a === (0.?))
      .limit(1.?, maxResultsPerPage)
      .join(users)
      .where("users.username".a === "subscriptions.owner".a)
    ).toPiql("usersFollowing")

  val findSubscription = (
    subscriptions.where("subscriptions.owner".a === (0.?))
      .where("subscriptions.target".a === (1.?))
    ).toPiql("findSubscription")
}
