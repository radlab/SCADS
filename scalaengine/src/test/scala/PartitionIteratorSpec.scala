package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import net.lag.logging.Logger

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

class PartitionMock extends ServiceHandler[GetRangeRequest] {
  val logger = Logger()
  def startup(): Unit = null
  def shutdown(): Unit = null

  def process(src: Option[RemoteActorProxy], msg: GetRangeRequest): Unit = {
    msg match {
      case GetRangeRequest(startKey, None, limit, None, true) => {
        val start = startKey.map(new IntRec().parse(_).f1).getOrElse(1)
        val end = if((start + limit.getOrElse(10000) - 1) >= 10000) 10000 else start + limit.get - 1
        src.foreach(_ ! GetRangeResponse((start to end).map(i => Record(IntRec(i).toBytes, Some(IntRec(i).toBytes))).toList))
      }
      case _ => logger.fatal("Invalid message to mock partition")
    }
  }
}

@RunWith(classOf[JUnitRunner])
class PartitionIteratorSpec extends Spec with ShouldMatchers {
  describe("The PartitionIterator") {
    it("should iterate over values") {
      val pm = new PartitionMock
      val rH = pm.remoteHandle

      new PartitionIterator(rH.toPartitionService("1", rH.toStorageService), None, None).map(r => new IntRec().parse(r.key).f1).toList should equal((1 to 10000).toList)

      pm.stop
    }
  }
}
