package edu.berkeley.cs.scads
package piql
package tpcw
package test

import piql.plans._
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

import scala.collection.mutable.{ArrayBuffer, HashMap}

@RunWith(classOf[JUnitRunner])
class TpcwAdminSpec extends Spec with ShouldMatchers with QueryResultMatchers {
  lazy val client = new TpcwClient(TestScalaEngine.newScadsCluster(), new SimpleExecutor with DebugExecutor)
  val timeB = 800000000000L

  client.items ++= List("One", "Two").flatMap(round =>
    (1 to 4).map {
      i =>
        val o = Item("book" + i)
        o.I_TITLE = "This is Book #" + i
        o.I_A_ID = "author" + i
        o.I_PUB_DATE = 0
        o.I_PUBLISHER = "publisher"
        o.I_SUBJECT = "subjectX"
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
    })

  client.orders ++= (1 to 4).map {
    i =>
      val o = Order("order" + i)
      o.O_C_UNAME = if (i == 4) "user2" else "user1"
      o.O_DATE_Time = timeB
      o.O_SUB_TOTAL = 0.0
      o.O_TAX = 0.0
      o.O_TOTAL = 0.0
      o.O_SHIP_TYPE = "Ground"
      o.O_SHIP_DATE = 0
      o.O_BILL_ADDR_ID = ""
      o.O_SHIP_ADDR_ID = ""
      o.O_STATUS = "pending"
      o
  }

  client.orderLines ++= (1 to 3).flatMap {
    i =>
      (1 to i).map {
        j =>
          val ol = OrderLine("order" + i, j)
          ol.OL_I_ID = "book" + j
          ol.OL_QTY = 3
          ol.OL_DISCOUNT = 0.0
          ol.OL_COMMENT = "Order it!"
          ol
      }
  }

  it("Should maintain mat view for admin confirm WI") {
    def fetch(bookA: Int, bookB: Int) = {
      val key = RelatedItemCountStaging(800002800000L,"book" + bookA, "book" + bookB)
      val option = client.relatedItemCountStaging.get(key)
      if (option.isDefined) {
        option.get.get(0)
      } else {
        0
      }
    }

    // Order Stream of Books:
    // 1, 1, 2, 1, 2, 3
    fetch(2,1) should equal(9)
    fetch(2,2) should equal(0)
    fetch(2,3) should equal(3)
    fetch(2,4) should equal(0)

    fetch(3,1) should equal(9)
    fetch(3,2) should equal(6)
    fetch(3,3) should equal(0)
    fetch(3,4) should equal(0)

    client.orderLines.put({
      val ol = OrderLine("order1", 1001)
      ol.OL_I_ID = "book4"
      ol.OL_QTY = 200
      ol.OL_DISCOUNT = 0.0
      ol.OL_COMMENT = "This inserts book 4 using the same customer as before."
      ol
    })

    // After purchase of Book #4
    fetch(2,1) should equal(9)
    fetch(2,4) should equal(200)
    fetch(4,2) should equal(6)
    fetch(4,3) should equal(3)

    client.orderLines.put({
      val ol = OrderLine("order4", 1004)
      ol.OL_I_ID = "book4"
      ol.OL_QTY = 100
      ol.OL_DISCOUNT = 0.0
      ol.OL_COMMENT = "This inserts book 4 using a DIFFERENT customer (user2)."
      ol
    })

    // Single purchase by different customer does not affect counts immediately.
    fetch(2,1) should equal(9)
    fetch(2,4) should equal(200)
    fetch(3,4) should equal(200)
    fetch(4,2) should equal(6)
    fetch(4,3) should equal(3)

    client.orderLines.put({
      val ol = OrderLine("order4", 1008)
      ol.OL_I_ID = "book3"
      ol.OL_QTY = 1
      ol.OL_DISCOUNT = 0.0
      ol.OL_COMMENT = "This inserts book 3 using a DIFFERENT customer (user2)."
      ol
    })
    fetch(3,4) should equal(300)  // 200 from user1, 100 from user2
    fetch(4,3) should equal(4)    // 3 from user1, 1 from user2
    fetch(3,2) should equal(6)    // just from user1

    client.orderLines.put({
      val ol = OrderLine("order4", 1009)
      ol.OL_I_ID = "book3"
      ol.OL_QTY = 1
      ol.OL_DISCOUNT = 0.0
      ol.OL_COMMENT = "This buys book 3 (again) as user2."
      ol
    })
    fetch(3,4) should equal(300)  // unaffected
    fetch(4,3) should equal(5)    // 3 from user1, 2 from user2
    fetch(3,2) should equal(6)    // unaffected

//    var key = RelatedItemCountStaging(800002800000L,null,null)
//    for (s <- client.relatedItemCountStaging.iterateOverRange(key, key)) {
//      println("Entry: " + s + ", " + s.RELATED_COUNT)
//    }
  }
}

@RunWith(classOf[JUnitRunner])
class TpcwSpec extends Spec with ShouldMatchers with QueryResultMatchers {
  lazy val client = new TpcwClient(TestScalaEngine.newScadsCluster(), new SimpleExecutor with DebugExecutor)

  val dataSize = 10
  val timeA = 12345000000000L

  client.authors ++= (1 to dataSize).map {
    i =>
      val a = Author("author" + i)
      a.A_FNAME = "FirstName" + i
      a.A_LNAME = "LastName" + i
      a.A_MNAME = "MiddleName" + i
      a.A_DOB = 0
      a.A_BIO = "I'm an author!"
      a
  }

  client.items ++= List("One", "Two").flatMap(round =>
    (1 to dataSize).map {
      i =>
        val o = Item("round" + round + "Book" + i)
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
    })

  client.orders ++= (1 to dataSize).map {
    i =>
      val o = Order("roundOneOrder" + i)
      o.O_C_UNAME = "I'm a customer!"
      o.O_DATE_Time = timeA
      o.O_SUB_TOTAL = 0.0
      o.O_TAX = 0.0
      o.O_TOTAL = 0.0
      o.O_SHIP_TYPE = "Ground"
      o.O_SHIP_DATE = timeA + 60 * 60 * 1000 //Wow thats fast!
      o.O_BILL_ADDR_ID = "" //Don't do any joins!
      o.O_SHIP_ADDR_ID = ""
      o.O_STATUS = "shipped"
      o
  }

  client.orderLines ++= (1 to dataSize).flatMap {
    i =>
      (1 to i).map {
        j =>
          val ol = OrderLine("roundOneOrder" + i, j)
          ol.OL_I_ID = "roundOneBook" + j
          ol.OL_QTY = 3
          ol.OL_DISCOUNT = 0.0
          ol.OL_COMMENT = "Order it!"
          ol
      }
  }

  client.orders ++= (1 to dataSize).map {
    i =>
      val o = Order("roundTwoOrder" + i)
      o.O_C_UNAME = "I'm a customer again!"
      o.O_DATE_Time = timeA + 30 * 60 * 1000
      o.O_SUB_TOTAL = 0.0
      o.O_TAX = 0.0
      o.O_TOTAL = 0.0
      o.O_SHIP_TYPE = "Ground"
      o.O_SHIP_DATE = timeA + 115 * 60 * 1000
      o.O_BILL_ADDR_ID = ""
      o.O_SHIP_ADDR_ID = ""
      o.O_STATUS = "shipped"
      o
  }

  client.orderLines ++= (1 to dataSize).flatMap {
    i =>
      (1 to i).map {
        j =>
          val ol = OrderLine("roundTwoOrder" + i, j)
          ol.OL_I_ID = "roundTwoBook" + j
          ol.OL_QTY = 3
          ol.OL_DISCOUNT = 0.0
          ol.OL_COMMENT = "Order it!"
          ol
      }
  }

  describe("TPCW") {
    it("Should maintain mat view for best sellers WI") {

      // (1) Materializes order counts for round 1 of orders at +0 min.
      client.updateOrderCount(client.getEpoch(timeA),
        subjects = "subject0" :: Nil,
        k = 4)

      // (2) Materializes order counts for round 1&2 of orders at +45 min.
      client.updateOrderCount(client.getEpoch(timeA + 45 * 60 * 1000),
        subjects = "subject0" :: Nil,
        k = 4)

      // TopK was never materialized for these epochs.
      client.topOrdersInPreviousHour(timeA + 7 * 60 * 1000, "subject0") should equal(Nil)
      client.topOrdersInPreviousHour(timeA + 44 * 60 * 1000, "subject0") should equal(Nil)

      // Hits the view computed in (1).
      val top = client.topOrdersInPreviousHour(timeA, "subject0")
      val topb = client.topOrdersInPreviousHour(timeA + 3 * 60 * 1000, "subject0")
      top should equal(topb)
      top should equal(List(("roundOneBook9", 6),
        ("roundOneBook6", 15),
        ("roundOneBook3", 24)))

      // Hits the view computed in (2).
      client.topOrdersInPreviousHour(timeA + 45 * 60 * 1000, "subject0") should equal(
        List(("roundOneBook6", 15),
          ("roundTwoBook6", 15),
          ("roundOneBook3", 24),
          ("roundTwoBook3", 24)))
    }
  }
}

class TpcwTestDataSpec extends Spec with ShouldMatchers with QueryResultMatchers {
  implicit val executor = new ParallelExecutor with DebugExecutor
  val client =
    new piql.tpcw.TpcwClient(
      new piql.tpcw.TpcwLoaderTask(2, 1, 2, 10, 2).newTestCluster,
      executor)

  import client._

  val lookupOrder = LocalTuples(0, "ol", OrderLine.keySchema, OrderLine.schema)
    .join(orders)
    .where("ol.OL_O_ID".a === "orders.O_ID".a)
    .toPiql("lookupOrders")


  it("correctly total all orders in an epoch") {
    //Sanity checks
    val epoch = getEpoch(orders.getRange(None, None, limit = 1).head.O_DATE_Time)
    val ols = (
      lookupOrder(orderLines.iterateOverRange(None, None).map(Vector(_)).toSeq)
        .filter(o => getEpoch(o(1).asInstanceOf[Order].O_DATE_Time) == epoch)
        .map(_(0).asInstanceOf[OrderLine])
      )

    val totalOrders =
      orderCountStaging.iterateOverRange(
        OrderCountStaging(epoch, null, null).key,
        OrderCountStaging(epoch, null, null).key
      ).map(_.OC_COUNT) sum

    ols.map(_.OL_QTY).sum should equal(totalOrders)

  }
}
