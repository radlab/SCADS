package edu.berkeley.cs.scads.test

import scala.collection.JavaConversions._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

import org.apache.avro.generic.{GenericData, IndexedRecord}

import edu.berkeley.cs.scads.storage.TestScalaEngine
import edu.berkeley.cs.scads.piql._

@RunWith(classOf[JUnitRunner])
class ParallelScadrSpec extends Spec with ShouldMatchers with QueryResultMatchers {
  val client = new ScadrClient(TestScalaEngine.getTestCluster, new ParallelExecutor with DebugExecutor)
  client.bulkLoadTestData

  describe("The SCADr client") {
    it("findUser") {
      client.userData.foreach(u => client.findUser(u._1.username) should returnTuples(Array(u._1, u._2)))
    }

    it("myThoughts") {
      client.userData.foreach(u => {
        val answer = client.thoughtData.filter(_._1.owner equals u._1.username).map(t => Array(t._1, t._2)).reverse
        client.myThoughts(u._1.username, 10) should returnTuples(answer)
      })
    }

    it("usersFollowedBy") {
      client.userData.flatMap(u => client.usersFollowedBy(u._1.username, 10)).size should equal(client.subscriptionData.size)
    }

    it("usersFollowedByPaginate") {
      client.userData.foreach(u => {
        //println("User: %s".format(u._1.username))
        val flattened = client.usersFollowedByPaginate(u._1.username, 2).toList.flatMap(x => x)
        val subsPerUser = client.usersFollowedBy(u._1.username, 10)
        flattened.size should equal(subsPerUser.size)
      })
    }

  }
}

