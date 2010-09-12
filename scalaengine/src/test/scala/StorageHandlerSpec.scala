package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

@RunWith(classOf[JUnitRunner])
class StorageHandlerSpec extends Spec with ShouldMatchers {
  implicit def toOption[A](a: A): Option[A] = Option(a)

  private def initHandler(handler: StorageHandler) {
    val root = handler.root
    root.getOrCreate("namespaces/testns/keySchema").data = IntRec.schema.toString.getBytes
    root.getOrCreate("namespaces/testns/valueSchema").data = IntRec.schema.toString.getBytes
    root.getOrCreate("namespaces/testns/partitions")

    root.getOrCreate("namespaces/testgc/keySchema").data = IntRec.schema.toString.getBytes
    root.getOrCreate("namespaces/testgc/valueSchema").data = IntRec.schema.toString.getBytes
    root.getOrCreate("namespaces/testgc/partitions")
  }

  def getHandler(id: String) = {
    val handler = TestScalaEngine.getTestHandler("testScads", id)
    initHandler(handler)
    handler
  }

  def getHandler() = {
    val handler = TestScalaEngine.getTestHandler()
    initHandler(handler)
    handler
  }

  describe("StorageHandler") {
    it("should create partitions") {
      val handler = getHandler()
      handler.remoteHandle !? CreatePartitionRequest("testns", None, None) match {
        case CreatePartitionResponse(newPart) => newPart
        case u => fail("Unexpected message: " + u)
      }
    }

    it("should delete partitions") {
      val handler = getHandler().remoteHandle
      val partId = handler !? CreatePartitionRequest("testns", None, None) match {
        case CreatePartitionResponse(service) => service.partitionId
        case u => fail("Unexpected msg:" + u)
      }

      handler !? DeletePartitionRequest(partId) match {
        case DeletePartitionResponse() => true
        case u => fail("Unexpected response to delete partition: " + u)
      }
    }

    it("should garbage collect deleted partitions") {
      val handler = getHandler().remoteHandle

      def createPartition(lower: Option[Int], upper: Option[Int]): PartitionService = {
        handler !? CreatePartitionRequest("testgc", lower.map(l => IntRec(l).toBytes), upper.map(u => IntRec(u).toBytes)) match {
          case CreatePartitionResponse(service) => service
          case u => fail("Unexpected msg:" + u)
        }
      }

      def deletePartition(partId: String) {
        handler !? DeletePartitionRequest(partId) match {
          case DeletePartitionResponse() => // success 
          case u => fail("Unexpected response to delete partition: " + u)
        }
      }

      val p0_100 = createPartition(Some(0), Some(100)) // [0, 100)
      (1 until 100).foreach(i => p0_100 !? PutRequest(IntRec(i).toBytes, IntRec(i).toBytes)) 

      val p25_50 = createPartition(Some(25), Some(50)) // [25, 50)
      val p75_100 = createPartition(Some(75), Some(100)) // [75, 100)

      deletePartition(p0_100.partitionId) // should only delete keys in [0, 25) and [50, 75)

      def expectEmptyRange(view: PartitionService, lower: Int, upper: Int) {
        (lower until upper).foreach(i => {
          view !? GetRequest(IntRec(i).toBytes) match {
            case GetResponse(Some(value)) => fail("Should have deleted key " + i)
            case GetResponse(None) => // success
            case u => fail("Unexpected msg: " + u)
          }
        })
      }

      def expectFullRange(view: PartitionService, lower: Int, upper: Int) {
        (lower until upper).foreach(i => {
          view !? GetRequest(IntRec(i).toBytes) match {
            case GetResponse(Some(value)) => new IntRec().parse(value) should equal(IntRec(i)) 
            case GetResponse(None) => fail("Should NOT have deleted key " + i) 
            case u => fail("Unexpected msg: " + u)
          }
        })
      }

      val pAll = createPartition(None, None)

      expectEmptyRange(pAll, 0, 25)
      expectFullRange(pAll, 25, 50)
      expectEmptyRange(pAll, 50, 75)
      expectFullRange(pAll, 75, 100)

      deletePartition(pAll.partitionId)
      deletePartition(p25_50.partitionId)
      deletePartition(p75_100.partitionId)

    }

    it("should recreate partitions") {
      var handler = getHandler("testrecreate")

      val startkey = IntRec(10).toBytes
      val endkey = IntRec(15).toBytes

      val createRequest = CreatePartitionRequest("testns", Some(startkey), Some(endkey))

      val (service, partId) = handler.remoteHandle !? createRequest match {
        case CreatePartitionResponse(service) => 
          (service, service.partitionId)
        case u => fail("Unexpected msg:" + u)
      }

      service !? PutRequest(IntRec(11).toBytes, IntRec(22).toBytes) should equal (PutResponse())

      handler.stop

      handler = getHandler("testrecreate")
      handler.remoteHandle !? DeletePartitionRequest(partId) match {
        case DeletePartitionResponse() => true
        case u => fail("Partition was not recreated, and therefore could not be deleted: " + u)
      }

      val service0 = handler.remoteHandle !? createRequest match {
        case CreatePartitionResponse(service) => 
          service
        case u => fail("Unexpected msg:" + u)
      }

      service0 !? GetRequest(IntRec(11).toBytes) match {
        case GetResponse(Some(_)) =>
          fail("Record was not deleted, even though partition was deleted")
        case GetResponse(None) =>
          // success
        case u => fail("Unexpected msg:" + u)
      }

    }
  }
}
