package edu.berkeley.cs.scads.test

import scala.collection.JavaConversions._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

import org.apache.avro.generic.{GenericData, IndexedRecord}

import edu.berkeley.cs.scads.storage.TestScalaEngine
import edu.berkeley.cs.scads.piql._

import scala.collection.mutable.{ ArrayBuffer, HashMap }

abstract class AbstractScadrSpec extends Spec with ShouldMatchers with QueryResultMatchers {
    
  val client: ScadrClient
  val loader = new ScadrLoader(client, 1, 1)
  val data = loader.getData(0)
  data.load()

  private def usersFollowedBy(username: String) = {
    val subs = data.subscriptionData.filter { case (k, v) => k.owner == username } map { case (k, v) => k.target } toSet
    val users = data.userData.filter({ case (k, v) => subs.contains(k.username) }).sortBy(_._1.username)
    users
  }

  private def thoughtstream(username: String) = {
    // compute actual answer in memory...
    val subs = data.subscriptionData.filter { case(k, v) => k.owner == username }
    val allThoughts = subs.flatMap(x => data.thoughtData.filter { case (k, v) => k.owner == x._1.target })
    val answer = allThoughts.sortWith({ case (a, b) => a._1.timestamp > b._1.timestamp || (a._1.timestamp == b._1.timestamp && a._1.owner < b._1.owner) })
    answer
  }

  describe("The SCADr client") {
    it("findUser") {
      data.userData.foreach(u => client.findUser(u._1.username) should returnTuples(Array(u._1, u._2)))
    }

    it("myThoughts") {
      data.userData.foreach(u => {
        val answer = data.thoughtData.filter(_._1.owner equals u._1.username).map(t => Array(t._1, t._2)).reverse.take(10)
        client.myThoughts(u._1.username, 10) should returnTuples(answer)
      })
    }

    it("usersFollowedBy") {
      data.userData.foreach(u => {
        val followers = client.usersFollowedBy(u._1.username, 10)
        val users = usersFollowedBy(u._1.username).take(10)
        followers.map(x => Array(x(2), x(3))) should returnTuples (users.map(x => Array(x._1, x._2)))
      })
    }

    it("usersFollowedByPaginate") {
      data.userData.foreach(u => {
        val flattened = client.usersFollowedByPaginate(u._1.username, 2).toList.flatten
        flattened.size should equal(loader.numSubscriptionsPerUser)
        val users = usersFollowedBy(u._1.username)
        flattened.map(x => Array(x(2), x(3))) should returnTuples (users.map(x => Array(x._1, x._2)))
      })
    }

    it("usersFollowing") {
      data.userData.foreach(u => {
        val following = client.usersFollowing(u._1.username, 10)

        // compute using the index
        val subs = data.idxUsersTargetData.filter { case (k, v) => k.target == u._1.username } map { case (k, v) => k.owner } toSet 

        val users = data.userData.filter({ case (k, v) => subs.contains(k.username) }).sortBy(_._1.username).take(10)
        following.map(x => Array(x(2), x(3))) should returnTuples (users.map(x => Array(x._1, x._2)))

        // compute with brute force
        val subs0 = data.subscriptionData.filter { case (k, v) => k.target == u._1.username } map { case (k, v) => k.owner } toSet
        val users0 = data.userData.filter({ case (k, v) => subs0.contains(k.username) }).sortBy(_._1.username).take(10)
        following.map(x => Array(x(2), x(3))) should returnTuples (users0.map(x => Array(x._1, x._2)))
      })
    }

    it("thoughtstream") {
      data.userData.foreach(u => {
        val thoughts = client.thoughtstream(u._1.username, 10)
        val answer = thoughtstream(u._1.username).take(10)
        (thoughts.map(x => Array(x(2), x(3)))) should returnTuples(answer.map(t => Array(t._1, t._2)))
      })
    }

    it("thoughtstreamPaginate") {
      data.userData.foreach(u => {
        val thoughts = client.thoughtstreamPaginate(u._1.username, 4).toList.flatten
        val answer = thoughtstream(u._1.username)
        (thoughts.map(x => Array(x(2), x(3)))) should returnTuples(answer.map(t => Array(t._1, t._2)))
      })
    }

    it("thoughtsByHashTag") {

      val tagData = data.tagData.foldLeft(new HashMap[String, ArrayBuffer[HashTagKey]]) { case (m, (k, v)) => 
        m.getOrElseUpdate(k.tag, new ArrayBuffer[HashTagKey]) += k 
        m
      }
      val thoughtData = data.thoughtData.toMap

      tagData.foreach { case (tag, tagKeys) => 
        val thoughtsByTag = client.thoughtsByHashTag(tag, 10)

        val thoughts = tagKeys
          .map(tagKey => (ThoughtKey(tagKey.owner, tagKey.timestamp), thoughtData(ThoughtKey(tagKey.owner, tagKey.timestamp))))
          .sortWith({ case (a, b) => a._1.timestamp > b._1.timestamp || (a._1.timestamp == b._1.timestamp && a._1.owner > b._1.owner) })
          .take(10)

        thoughtsByTag.map(x => Array(x(2), x(3))) should returnTuples(thoughts.map(x => Array(x._1, x._2)))
      }

    }

  }
}
