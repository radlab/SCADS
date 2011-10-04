package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import net.lag.logging.Logger

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

class PartitionMock extends ServiceHandler[StorageMessage] {
  val logger = Logger()
  def startup(): Unit = null
  def shutdown(): Unit = null

  val registry = StorageRegistry

  def process(src: Option[RemoteServiceProxy[StorageMessage]], msg: StorageMessage): Unit = {
    msg match {
      case GetRangeRequest(startKey, None, limit, offset, true) => {
        val start = startKey.map(new IntRec().parse(_).f1).getOrElse(1) + offset.getOrElse(0)
        val end = if((start + limit.getOrElse(10000) - 1) >= 10000) 10000 else start + limit.get - 1
        src.foreach(_ ! GetRangeResponse((start to end).map(i => Record(IntRec(i).toBytes, Some(IntRec(i).toBytes))).toList))
      }
      case m => logger.fatal("Invalid StorageMessage to mock partition: %s", m)
    }
  }
}

@RunWith(classOf[JUnitRunner])
class PartitionIteratorSpec extends Spec with ShouldMatchers {
  describe("The PartitionIterator") {
    it("should iterate over values") {
      val pm = new PartitionMock
      val rH = pm.remoteHandle

      new ActorlessPartitionIterator(PartitionService(rH, "1", StorageService(rH)), None, None).map(r => new IntRec().parse(r.key).f1).toList should equal((1 to 10000).toList)

      pm.stop
    }
  }
}
