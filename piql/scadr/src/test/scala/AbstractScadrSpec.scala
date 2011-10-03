package edu.berkeley.cs
package scads
package piql
package scadr
package test

import storage.TestScalaEngine
import piql.exec._
import piql.debug._
import piql.test._

import scala.collection.JavaConversions._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

import org.apache.avro.generic.{GenericData, IndexedRecord}
import scala.collection.mutable.{ ArrayBuffer, HashMap }

@RunWith(classOf[JUnitRunner])
class ScadrSpec extends AbstractScadrSpec {
  lazy val client = new ScadrClient(TestScalaEngine.newScadsCluster(), new SimpleExecutor with DebugExecutor)
}

@RunWith(classOf[JUnitRunner])
class ParallelScadrSpec extends AbstractScadrSpec {
  lazy val client = new ScadrClient(TestScalaEngine.newScadsCluster(), new ParallelExecutor with DebugExecutor)
}


@RunWith(classOf[JUnitRunner])
class LazyScadrSpec extends AbstractScadrSpec {
  lazy val client = new ScadrClient(TestScalaEngine.newScadsCluster(), new LazyExecutor with DebugExecutor)
}

abstract class AbstractScadrSpec extends Spec with ShouldMatchers with QueryResultMatchers {

  val client: ScadrClient
  val loader = new ScadrLoader(1, 1)
  val data = loader.getData(0)
  data.load(client)

  private def usersFollowedBy(username: String) = {
    val subs = data.subscriptionData.filter(_.owner == username).map( _.target).toSet
    data.userData.filter(s => subs.contains(s.username)).sortBy(_.username)
  }

  private def thoughtstream(username: String) = {
    // compute actual answer in memory...
    val subs = data.subscriptionData.filter(_.owner == username)
    val allThoughts = subs.flatMap(x => data.thoughtData.filter(_.owner == x.target))
    val answer = allThoughts.sortWith({ case (a, b) => a.timestamp > b.timestamp || (a.timestamp == b.timestamp && a.owner < b.owner) })
    answer
  }

  describe("The SCADr client") {
    it("findUser") {
data.userData.foreach(u => client.findUser(u.username).head should equal(Vector(u)))
    }

    it("myThoughts") {
      data.userData.foreach(u => {
        val answer = data.thoughtData.filter(_.owner equals u.username).map(t => Vector(t)).reverse.take(10).toList
        client.myThoughts(u.username, 10) should equal (answer)
      })
    }

    it("usersFollowedBy") {
      data.userData.foreach(u => {
        val followers = client.usersFollowedBy(u.username, 10)
        val users = usersFollowedBy(u.username).take(10)
        followers.map(x => Vector(x(1))) should equal (users.map(x => Vector(x)))
      })
    }

    it("thoughtstream") {
      data.userData.foreach(u => {
        val thoughts = client.thoughtstream(u.username, 10)
        val answer = thoughtstream(u.username).take(10).toList
        (thoughts.map(x => Vector(x(1)))) should equal(answer.map(t => Vector(t)))
      })
    }

    it("usersFollowedByPaginate") {
      pending
      /*
      data.userData.foreach(u => {
        val flattened = client.usersFollowedByPaginate(u.username, 2).toList.flatten
        flattened.size should equal(loader.numSubscriptionsPerUser)
        val users = usersFollowedBy(u.username)
        flattened.map(x => Array(x(2), x(3))) should equal (users.map(x => Array(x, x)))
      })*/
    }

    it("usersFollowing") {
      pending
      /*
      data.userData.foreach(u => {

        val following = client.usersFollowing(u.username, 10)

        // compute using the index
        val subs = data.idxUsersTargetData.filter { case (k, v) => k.target == u.username } map { case (k, v) => k.owner } toSet

        val users = data.userData.filter({ case (k, v) => subs.contains(k.username) }).sortBy(_.username).take(10)
        following.map(x => Array(x(2), x(3))) should equal (users.map(x => Array(x, x)))

        // compute with brute force
        val subs0 = data.subscriptionData.filter { case (k, v) => k.target == u.username } map { case (k, v) => k.owner } toSet
        val users0 = data.userData.filter({ case (k, v) => subs0.contains(k.username) }).sortBy(_.username).take(10)
        following.map(x => Array(x(2), x(3))) should equals (users0.map(x => Array(x, x)))
      })
      */
    }


    it("thoughtstreamPaginate") {
      pending
      /*
      data.userData.foreach(u => {
        val thoughts = client.thoughtstreamPaginate(u.username, 4).toList.flatten
        val answer = thoughtstream(u.username)
        (thoughts.map(x => Array(x(2), x(3)))) should equal(answer.map(t => Array(t, t)))
      })
      */
    }

    it("thoughtsByHashTag") {
      pending
      /*
      val tagData = data.tagData.foldLeft(new HashMap[String, ArrayBuffer[HashTagKey]]) { case (m, (k, v)) =>
        m.getOrElseUpdate(k.tag, new ArrayBuffer[HashTagKey]) += k
        m
      }
      val thoughtData = data.thoughtData.toMap

      tagData.foreach { case (tag, tagKeys) =>
        val thoughtsByTag = client.thoughtsByHashTag(tag, 10)

        val thoughts = tagKeys
          .map(tagKey => (ThoughtKey(tagKey.owner, tagKey.timestamp), thoughtData(ThoughtKey(tagKey.owner, tagKey.timestamp))))
          .sortWith({ case (a, b) => a.timestamp > b.timestamp || (a.timestamp == b.timestamp && a.owner > b.owner) })
          .take(10)

        thoughtsByTag.map(x => Array(x(2), x(3))) should equal(thoughts.map(x => Array(x, x)))
      }
      */
    }

  }
}
