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
    a.A_MNAME = "MiddleName" + i
    a.A_DOB = 0
    a.A_BIO = "I'm an author!"
    a
  }

  client.items ++= (1 to dataSize).map {i =>
    val o = Item("book" + i)
    o.I_TITLE = "This is Book #" + i
    o.I_A_ID = "author" + i
    o.I_PUB_DATE = 0
    o.I_PUBLISHER = "publisher"
    o.I_SUBJECT = "subject" + (i % 3)
    o.I_DESC = "IM A BOOK"
    o.I_RELATED1 = 0
    o.I_RELATED2 = 0
    o.I_RELATED3 = 0
    o.I_RELATED4 = 0
    o.I_RELATED5 = 0
    o.I_THUMBNAIL = "http://test.com/book.jpg"
    o.I_IMAGE = "http://test.com/book.jpg"
    o.I_SRP = 0.0
    o.I_COST = 0.0
    o.I_AVAIL = 0
    o.I_STOCK = 1
    o.ISBN = "30941823-0491823-40"
    o.I_PAGE = 100
    o.I_BACKING = "HARDCOVER"
    o.I_DIMENSION = "10x10x10"
    o
  }

  client.orders ++= (1 to dataSize).map {i =>
    val o = Order("order" + i)
    o.O_C_UNAME = "I'm a customer!"
    o.O_DATE_Time = System.currentTimeMillis()
    o.O_SUB_TOTAL = 0.0
    o.O_TAX = 0.0
    o.O_TOTAL = 0.0
    o.O_SHIP_TYPE = "Ground"
    o.O_SHIP_DATE = System.currentTimeMillis() + 60 * 60 * 1000 //Wow thats fast!
    o.O_BILL_ADDR_ID = "" //Don't do any joins!
    o.O_SHIP_ADDR_ID = ""
    o.O_STATUS = "shipped"
    o
  }

  client.orderLines ++= (1 to dataSize).flatMap {i =>
    (1 to i).map {j =>
      val ol = OrderLine("order" + i, j)
      ol.OL_I_ID = "book" + j
      ol.OL_QTY = 1
      ol.OL_DISCOUNT = 0.0
      ol.OL_COMMENT = "Order it!"
      ol
    }
  }

  describe("TPCW") {
    it("Should maintain mat view for best sellers WI") {
      client.orderCountStaging.iterateOverRange(None,None).foreach(oc => println(oc.key.toString + oc.value.toString))
      client.updateOrderCount()
      client.orderCount.iterateOverRange(None,None).foreach(oc => println(oc.key.toString + oc.value.toString))
    }
  }
}
