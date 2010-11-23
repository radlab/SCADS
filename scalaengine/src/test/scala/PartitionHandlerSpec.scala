package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ BeforeAndAfterAll, Spec }
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

@RunWith(classOf[JUnitRunner])
class PartitionHandlerSpec extends Spec with ShouldMatchers with BeforeAndAfterAll {
  implicit def toOption[A](a: A): Option[A] = Option(a)

  val cluster = TestScalaEngine.newScadsCluster(0)

  override def afterAll(): Unit = {
    cluster.shutdownCluster()
  }

  def withPartitionService(startKey: Option[IntRec] = None, endKey: Option[IntRec] = None)(f: PartitionService => Unit): Unit = {
    val handler = cluster.addNode()
    val root = handler.root

    root.getOrCreate("namespaces/partitiontestns/keySchema").data = IntRec.schema.toString.getBytes
    root.getOrCreate("namespaces/partitiontestns/valueSchema").data = IntRec.schema.toString.getBytes
    root.getOrCreate("namespaces/partitiontestns/partitions")

    val service = (handler.remoteHandle !? CreatePartitionRequest("partitiontestns", startKey.map(_.toBytes), endKey.map(_.toBytes))) match {
      case CreatePartitionResponse(newPartService) => newPartService
      case _ => fail("Couldn't get partition handler")
    }

    f(service)
  }

  describe("PatitionHandler") {
    it("store values") {
      withPartitionService(None, None) { partition =>
        (1 to 10).foreach(i => {
          (partition !? PutRequest(IntRec(i).toBytes, IntRec(i*2).toBytes)) should equal(PutResponse())
        })

        (1 to 10).foreach(i => {
          (partition !? GetRequest(IntRec(i).toBytes)) match {
            case GetResponse(Some(bytes)) => new IntRec().parse(bytes) should equal(IntRec(i*2))
            case m => fail("Expected GetResponse but got: " + m)
          }
        })
      }
    }

    it("should bulk load values") {
      withPartitionService(None, None) { p =>
        val req = BulkPutRequest((1 to 1000).map(i => PutRequest(IntRec(i).toBytes, IntRec(i * 2).toBytes)).toSeq)
        p !? req match {
          case BulkPutResponse() => // success
          case m => fail("Expected BulkPutResponse but got: " + m)
        }

        (1 to 1000).foreach(i => {
          (p !? GetRequest(IntRec(i).toBytes)) match {
            case GetResponse(Some(bytes)) => new IntRec().parse(bytes) should equal(IntRec(i * 2))
            case m => fail("Expected GetResponse but got: " + m)
          }
        })
      }
    }

    it("should get ranges of data") {
      withPartitionService(None, None) { p =>
        val req = BulkPutRequest((1 to 100).map(i => PutRequest(IntRec(i).toBytes, IntRec(i * 2).toBytes)).toSeq)
        p !? req match {
          case BulkPutResponse() => // success
          case m => fail("Expected BulkPutResponse but got: " + m)
        }

        p !? GetRangeRequest(None, None) match {
          case GetRangeResponse(resps) =>
            resps.map(rec => new IntRec().parse(rec.key).f1) should equal(1 to 100)
          case m => fail("Expected GetRangeResponse but got: " + m)
        }

        p !? GetRangeRequest(Some(IntRec(25).toBytes), Some(IntRec(75).toBytes)) match {
          case GetRangeResponse(resps) =>
            resps.map(rec => new IntRec().parse(rec.key).f1) should equal(25 until 75)
          case m => fail("Expected GetRangeResponse but got: " + m)
        }
      }
    }

    it("should copy data overwriting existing data") {
      withPartitionService(None, None) { p1 =>
        withPartitionService(None, None) { p2 =>
          p1 !? BulkPutRequest((1 to 10000).map(i => PutRequest(IntRec(i).toBytes, IntRec(i).toBytes)).toSeq)
          (1 to 5).foreach(i => p2 !? PutRequest(IntRec(i).toBytes, IntRec(i*2).toBytes))

          p2 !? CopyDataRequest(p1, true)

          (1 to 10000).foreach(i => p2 !? GetRequest(IntRec(i).toBytes) match {
            case GetResponse(Some(bytes)) => new IntRec().parse(bytes) should equal(IntRec(i))
            case u => fail("P2 doesn't contain " + i + " instead got: " + u)
          })
        }
      }
    }

    it("should copy data without overwriting existing data") {
      withPartitionService(None, None) { p1 =>
        withPartitionService(None, None) { p2 =>
          (1 to 10).foreach(i => p1 !? PutRequest(IntRec(i).toBytes, IntRec(i).toBytes))
          (1 to 5).foreach(i => p2 !? PutRequest(IntRec(i).toBytes, IntRec(i*2).toBytes))

          p2 !? CopyDataRequest(p1, false)

          (1 to 5).foreach(i => p2 !? GetRequest(IntRec(i).toBytes) match {
            case GetResponse(Some(bytes)) => new IntRec().parse(bytes) should equal(IntRec(i*2))
            case u => fail("P2 doesn't contain " + i + " instead got: " + u)
          })

          (6 to 10).foreach(i => p2 !? GetRequest(IntRec(i).toBytes) match {
            case GetResponse(Some(bytes)) => new IntRec().parse(bytes) should equal(IntRec(i))
            case u => fail("P2 doesn't contain " + i + " instead got: " + u)
          })
        }
      }
    }

    it("should reject requests which fall outside of its partition range") {
      withPartitionService(IntRec(0), IntRec(10)) { p1 => // [0, 10)
        val VALUE = IntRec(0).toBytes

        try {
          p1 !? PutRequest(IntRec(-1).toBytes, VALUE) match {
            case u => fail("-1 falls outside of the range of p1's key range: " + u)
          }
        } catch {
          case e: RuntimeException =>
            // supposed to fail
        }

        p1 !? PutRequest(IntRec(0).toBytes, VALUE) match {
          case PutResponse() =>
          case u => fail("0 falls inside of range of p1's key range: " + u)
        }

        p1 !? PutRequest(IntRec(9).toBytes, VALUE) match {
          case PutResponse() =>
          case u => fail("9 falls inside of range of p1's key range: " + u)
        }

        try {
          p1 !? GetRequest(IntRec(10).toBytes) match {
            case u @ GetResponse(_) => fail("10 is outside of p1's range - should return error: " + u)
            case u => fail("Unexpected message: " + u)
          }
        } catch {
          case e: RuntimeException =>
            // supposed to fail
        }

        p1 !? CountRangeRequest(Some(IntRec(0).toBytes), Some(IntRec(10).toBytes)) match {
          case CountRangeResponse(cnt) => cnt should equal(2)
          case u => fail("[0, 10) is bounded by p1's key range: " + u)
        }

        try {
          p1 !? CountRangeRequest(Some(IntRec(1).toBytes), Some(IntRec(11).toBytes)) match {
            case u => fail("11 falls outside of the range of p1's key range: " + u)
          }
        } catch {
          case e: RuntimeException =>
            // supposed to fail
        }
      }
    }

    it("should respect limits and offsets for ranges") {
      withPartitionService(None, None) { p1 =>
        p1 !? BulkPutRequest((1 to 100).map(i => PutRequest(IntRec(i).toBytes, IntRec(i).toBytes)).toSeq)
        p1 !? GetRangeRequest(Some(IntRec(91).toBytes), None, offset=Some(10)) match {
          case GetRangeResponse(Nil) => // success
          case m => fail("Expected empty GetRangeResponse but got: " + m)
        }
      }
    }

  }
}
