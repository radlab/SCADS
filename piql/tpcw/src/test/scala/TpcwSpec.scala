package edu.berkeley.cs.scads
package piql
package tpcw
package test

import piql.exec._
import piql.debug._
import piql.test._

import scala.collection.JavaConversions._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

import org.apache.avro.generic.{GenericData, IndexedRecord}

import edu.berkeley.cs.scads.storage.TestScalaEngine

import scala.collection.mutable.{ ArrayBuffer, HashMap }

@RunWith(classOf[JUnitRunner])
class TpcwSpec extends Spec with ShouldMatchers with QueryResultMatchers {
  lazy val client = new TpcwClient(TestScalaEngine.newScadsCluster(), new SimpleExecutor with DebugExecutor)

  val dataSize = 10

  client.authors ++= (1 to dataSize).map {i =>
    val a = Author("author" + i)
    a.A_FNAME = "FirstName" + i
    a.A_LNAME = "LastName" + i
    a
  }

  client.items ++= (1 to dataSize).map {i =>
    val o = Item("book" + i)
    o.I_A_ID = "author" + i
    o.I_SUBJECT = "subject" + (i % 3)
    o.I_TITLE = "This is Book #" + i
    o
  }

  describe("TPCW") {
    it("Should maintain mat view for best sellers WI") {

    }
  }
}
