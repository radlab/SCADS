package edu.berkeley.cs.scads.piql

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker._

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
case class UserTarget(var target: String, var owner: String) extends AvroPair

class ScadrClient(val cluster: ScadsCluster, executor: QueryExecutor, maxSubscriptions: Int = 5000) {
  val maxResultsPerPage = 10

  // namespaces are declared to be lazy so as to allow for manual
  // createNamespace calls to happen first (and after instantiating this
  // class)

  lazy val users = cluster.getNamespace[User]("users")
  lazy val thoughts = cluster.getNamespace[Thought]("thoughts")
  lazy val subscriptions = cluster.getNamespace[Subscription]("subscriptions")
  lazy val tags = cluster.getNamespace[HashTag]("tags")

  lazy val idxUsersTarget = cluster.getNamespace[UserTarget]("idxUsersTarget")

  private def exec(plan: QueryPlan, args: Any*) = {
    val iterator = executor(plan, args:_*)
    iterator.open
    val ret = iterator.toList
    iterator.close
    ret

  }

  def findUser = users.where("username".a === (0.?))

  def thoughtstream = (
    subscriptions
      .where("owner".a === (0.?))
      .where("approved".a === true)
      .join(thoughts)
      .where("thoughts.owner".a === "subscriptions.target".a)
      .sort("timestamp" :: Nil, false)
      .limit(10)
  )

}
