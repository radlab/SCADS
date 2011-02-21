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

case class Interview(var candidate: String, var createdAt: Long) extends AvroPair {
  var interviewedAt: Long = _
  var status: String = _
  var score: Int = _
  var comments: String = _
  var interviewer: String = _
  var researchArea: String = _
}

class ComradesClient(val cluster: ScadsCluster, executor: QueryExecutor) {
  implicit val exec = executor
  val maxResultsPerPage = 10

  val candidates = cluster.getNamespace[Candidate]("candidates")
  val interviews = cluster.getNamespace[Interview]("interviews")

  val findWaiting = /* MICHAEL OPTIMIZER (
    interviews
      .where("interviews.researchArea".a === (0.?))
      .where("interviews.interviewedAt".a === 0)
      .sort("interviews.createdAt".a :: Nil, true)
      .limit(maxResultsPerPage)
      .join(candidates)
      .where("interviews.candidate".a === "candidates.email".a)
    ).toPiql("findWaiting") */

    new OptimizedQuery("findWaiting",
      IndexLookupJoin(candidates,
		      AttributeValue(0, 3) :: Nil,
	LocalStopAfter(FixedLimit(maxResultsPerPage),
	  IndexScan(interviews.getOrCreateIndex("researchArea" :: "interviewedAt" :: "createdAt" :: Nil),
		    ParameterValue(0) :: ConstantValue(new java.lang.Long(0)) :: Nil,
		    FixedLimit(maxResultsPerPage),
		    true))),
      executor)

  val findTopRated = /* MICHAEL OPTIMIZER (
    interviews
      .where("interviews.researchArea".a === (0.?))
      .where("interviews.score".a === 5)
      .where("interviews.status".a === "INTERVIEWED")
      .sort("interviews.interviewedAt".a :: Nil, false)
      .limit(maxResultsPerPage)
      .join(candidates)
      .where("interviews.candidate".a === "candidates.email".a)
    ).toPiql("findTopRated") */

    new OptimizedQuery("findTopRated",
      IndexLookupJoin(candidates,
		      AttributeValue(0, 4) :: Nil,
	LocalStopAfter(FixedLimit(maxResultsPerPage),
          IndexScan(interviews.getOrCreateIndex("researchArea" :: "score" :: "status" :: "interviewedAt" :: Nil),
		    ParameterValue(0) :: ConstantValue(5) :: ConstantValue(new org.apache.avro.util.Utf8("INTERVIEWED")) :: Nil,
		    FixedLimit(maxResultsPerPage),
		    false))),
      executor)

  val findCandidate = (
    candidates
      .where("candidates.email".a === (0.?))
    ).toPiql("findCandidateDetails")

  val findInterviewsForCandidate = (
    interviews.where("interviews.candidate".a === (0.?))
        .limit(maxResultsPerPage)
        .toPiql("findInterviewsForCandidate")
  )

  val findInterview = (
    interviews.where("interviews.candidate".a === (0.?))
      .where("interviews.createdAt".a === (1.?))
    ).toPiql("findInterview")

  val findCandidatesByName = (
    candidates
      .where("candidates.name".a === (0.?))
      .limit(maxResultsPerPage)
    ).toPiql("findCandidatesByName")
}
