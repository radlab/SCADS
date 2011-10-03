package edu.berkeley.cs.scads.storage

import java.util.concurrent.ArrayBlockingQueue

import scala.actors._
import scala.actors.Actor._

import net.lag.logging.Logger

import edu.berkeley.cs.scads.comm._

abstract class FutureBasedIterator(partitionService: PartitionService, minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]], recsPerMessage: Int = 1024) extends Iterator[Record] {
  require(recsPerMessage > 0)

  private val recsPerMessageDiv2 = recsPerMessage / 2

  private var isDone = false

  private var currentBuffer: IndexedSeq[Record] = Vector.empty
  private var currentIdx = 0

  private var activeFuture: MessageFuture[StorageMessage] = _

  def next: Record = {
    if (!hasNext)
      throw new UnsupportedOperationException("next on empty iterator")
    val res = currentBuffer(currentIdx)
    currentIdx += 1
    if (currentIdx >= recsPerMessageDiv2 && (activeFuture eq null) && canRequestMore)
      activeFuture = issueNextRequest()
    res
  }

  protected def issueNextRequest(): MessageFuture[StorageMessage]
  protected def canRequestMore: Boolean
  protected def getRecordsFromFuture(ftch: MessageFuture[StorageMessage]): IndexedSeq[Record]

  def hasNext: Boolean =
    if (isDone) false 
    else if (currentIdx < currentBuffer.size) true
    else if (activeFuture ne null) {
      currentBuffer = getRecordsFromFuture(activeFuture)
      currentIdx = 0
      activeFuture = null
      hasNext
    } else if (canRequestMore) {
      assert(activeFuture eq null)
      activeFuture = issueNextRequest()
      hasNext
    } else {
      isDone = true
      false
    }
}

class ActorlessPartitionIterator(partitionService: PartitionService, minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]], recsPerMessage: Int = 1024) 
  extends FutureBasedIterator(partitionService, minKey, maxKey, recsPerMessage) {

  private var lastRecvKey: Option[Array[Byte]] = None
  private var lastSizeReturned = recsPerMessage

  protected def issueNextRequest(): MessageFuture[StorageMessage] = {
    // dispatch new request
    val req = 
      lastRecvKey.map(sk => GetRangeRequest(Some(sk), maxKey, limit=Some(recsPerMessage), offset=Some(1))).getOrElse(GetRangeRequest(minKey, maxKey, limit=Some(recsPerMessage)))
    partitionService !! req
  }

  protected def canRequestMore = lastSizeReturned == recsPerMessage

  protected def getRecordsFromFuture(ftch: MessageFuture[StorageMessage]) =
    ftch.get(10 * 60 * 1000).getOrElse(throw new RuntimeException("GetRangeRequest timedout")) match {
      case GetRangeResponse(recs) => 
        lastSizeReturned = recs.size
        lastRecvKey = recs.lastOption.map(_.key) 
        recs.toIndexedSeq
      case e => throw new RuntimeException("Invalid response to a GetRangeRequest: " + e)
    }
}

class CursorBasedPartitionIterator(partitionService: PartitionService, minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]], recsPerMessage: Int = 8192) 
  extends FutureBasedIterator(partitionService, minKey, maxKey, recsPerMessage) {

  private var cursorId: Option[Int] = None
  private var isServerDone = false

  protected def issueNextRequest(): MessageFuture[StorageMessage] = {
    val rec = CursorScanRequest(cursorId, recsPerMessage)
    partitionService !! rec
  }

  protected def canRequestMore = !isServerDone 
  protected def getRecordsFromFuture(ftch: MessageFuture[StorageMessage]) =
    ftch.get(3 * 60 * 1000).getOrElse(throw new RuntimeException("CursorScanRequest timedout")) match {
      case CursorScanResponse(id, recs) =>
        cursorId = id
        isServerDone = id.isEmpty
        assert(isServerDone || recs.size == recsPerMessage)
        recs
      case e => throw new RuntimeException("Invalid response to a CursorScanRequest: " + e)
    }
}