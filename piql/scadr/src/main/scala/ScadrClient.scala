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

case class HashTag(var tag: String, var timestamp: Int, var owner: String) extends AvroPair

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
  val tags = cluster.getNamespace[HashTag]("tags")
  // Additional way to access tags, to match AvroRecord's conventions:
  val hashtags = cluster.getNamespace[HashTag]("hashtags")

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
		 .limit(1.?, maxSubscriptions)
		 .join(thoughts)
		 .where("thoughts.owner".a === "subscriptions.target".a)
		 .sort("thoughts.timestamp".a :: Nil, false)
		 .limit(2.?, maxResultsPerPage)
  ).toPiql("thoughtstream")

  //HACK
  lazy val usersInTown =
    users.where("homeTown".a === (0.?))
	 .limit(10)
	 .toPiql("usersInTown")

  /**
   * Who is following ME?
   */
  lazy val usersFollowing = (
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
