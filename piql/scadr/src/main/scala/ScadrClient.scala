package edu.berkeley.cs
package scads
package piql
package scadr

import storage._
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

  lazy val users = cluster.getNamespace[User]("users")
  lazy val thoughts = cluster.getNamespace[Thought]("thoughts")
  lazy val subscriptions = cluster.getNamespace[Subscription]("subscriptions")
  lazy val tags = cluster.getNamespace[HashTag]("tags")

  def findUser = users.where("username".a === (0.?)).toPiql

  def thoughtstream = (
    subscriptions
      .where("owner".a === (0.?))
      .where("approved".a === true)
      .join(thoughts)
      .where("thoughts.owner".a === "subscriptions.target".a)
      .sort("timestamp" :: Nil, false)
      .limit(10)
  ).toPiql

}
