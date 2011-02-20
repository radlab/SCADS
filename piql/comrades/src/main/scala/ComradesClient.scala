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

case class Interview(var candidate: String, var createdAt: Int) extends AvroPair {
  var interviewedAt: Int = _
  var status: String = _
  var score: Int = _
  var comments: String = _
  var interviewer: String = _
  var researchArea: String = _
}

class ComradesClient(val cluster: ScadsCluster, executor: QueryExecutor) {
  implicit val exec = executor
  val maxResultsPerPage = 10

  lazy val candidates = cluster.getNamespace[Candidate]("candidates")
  lazy val interviews = cluster.getNamespace[Interview]("interviews")

  val findWaiting = (
    interviews
      .where("interviews.researchArea".a === (0.?))
      .where("interviews.interviewedAt".a === 0)
      .sort("interviews.createdAt".a :: Nil, true)
      .limit(maxResultsPerPage)
      .join(candidates)
      .where("interviews.candidate".a === "candidates.email".a)
    ).toPiql("findWaiting")

  val findTopRated = (
    interviews
      .where("interviews.researchArea".a === (0.?))
      .where("interviews.score".a === 5)
      .where("interviews.status".a === "INTERVIEWED")
      .sort("interviews.interviewedAt".a :: Nil, false)
      .limit(maxResultsPerPage)
      .join(candidates)
      .where("interviews.candidate".a === "candidates.email".a)
    ).toPiql("findWaiting")

  val findCandidateDetails = (
    candidates
      .where("candidates.email".a === (0.?))
      .join(interviews)
      .where("interviews.candidate".a === "candidates.email".a)
    ).toPiql("findCandidate")

  val findCandidatesByName = (
    candidates
      .where("candidates.name".a === (0.?))
      .limit(maxResultsPerPage)
    ).toPiql("findCandidatesByName")
}
