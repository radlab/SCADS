package edu.berkeley.cs
package scads
package piql
package comrades

import storage.ScadsCluster
import avro.marker._

case class Candidate(var name: String) extends AvroPair {
  var v = 1
}

class ComradesClient(val cluster: ScadsCluster, executor: QueryExecutor) {
  implicit val exec = executor

  lazy val users = cluster.getNamespace[Candidate]("candidates")

  
}
