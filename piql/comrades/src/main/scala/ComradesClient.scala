package edu.berkeley.cs
package scads
package piql
package comrades

import storage.ScadsCluster
import avro.marker._

import org.apache.avro.util._

case class Candidate(var email: String) extends AvroPair {
  var name: String = _
  var school: String = _
  var gpa: Float = _
  var researchArea: String = _
}

case class Interview(var candidate: String, var time: Int) extends AvroPair {
  var status: String = _
  var score: Int = _
  var comments: Int = _
  var interviewer: String = _
}

class ComradesClient(val cluster: ScadsCluster, executor: QueryExecutor) {
  implicit val exec = executor
  val maxResultsPerPage = 10

  lazy val candidates = cluster.getNamespace[Candidate]("candidates")
  lazy val interviews = cluster.getNamespace[Interview]("interviews")

  val findByStatus = (
    interviews.where("interviews.status".a === (0.?))
      .sort("interviews.time".a :: Nil, false)
      .limit(maxResultsPerPage)
      .join(candidates)
      .where("interviews.candidate".a === "candidates.email".a)
    ).toPiql("findByStatus")

  val findRecent = (
    interviews.sort("interviews.time".a :: Nil, false)
      .limit(maxResultsPerPage)
      .join(candidates)
      .where("interviews.candidate".a === "candidates.email".a)
    ).toPiql("findRecent")

  val findCandidate = candidates.where("candidates.email".a === (0.?)).toPiql("findCandidate")

  val findCandidatesByName = (
    candidates.where("candidates.name".a === (0.?))
      .limit(maxResultsPerPage)
    ).toPiql("findCandidatesByName")
}
