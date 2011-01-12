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

  val myThoughts = (
    thoughts.where("thoughts.owner".a === (0.?))
	    .sort("thoughts.timestamp".a :: Nil, false)
	    .limit(maxResultsPerPage)
  ).toPiql

  lazy val usersFollowedBy = (
    subscriptions.where("subscriptions.owner".a === (0.?))
	 .limit(maxResultsPerPage)
	 .join(users)
	 .where("subscriptions.target".a === "users.username".a)
  ).toPiql


  val thoughtstream = (
    subscriptions.where("subscriptions.owner".a === (0.?))
		 .dataLimit(5000)
		 .join(thoughts)
		 .where("thoughts.owner".a === "subscriptions.target".a)
		 .sort("thoughts.timestamp".a :: Nil, false)
		 .limit(10)
  ).toPiql

  /**
   * Who is following ME?
   */
  lazy val usersFollowing = (
    subscriptions.where("subscriptions.target".a === (0.?))
		 .limit(1000)
		 .join(users)
		 .where("users.username".a === "subscriptions.owner".a)
    ).toPiql
}
