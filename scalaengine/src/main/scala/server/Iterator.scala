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

  private var activeFuture: MessageFuture = _

  def next: Record = {
    if (!hasNext)
      throw new UnsupportedOperationException("next on empty iterator")
    val res = currentBuffer(currentIdx)
    currentIdx += 1
    if (currentIdx >= recsPerMessageDiv2 && (activeFuture eq null) && canRequestMore)
      activeFuture = issueNextRequest()
    res
  }

  protected def issueNextRequest(): MessageFuture
  protected def canRequestMore: Boolean
  protected def getRecordsFromFuture(ftch: MessageFuture): IndexedSeq[Record]

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

  protected def issueNextRequest(): MessageFuture = {
    // dispatch new request
    val req = 
      lastRecvKey.map(sk => GetRangeRequest(Some(sk), maxKey, limit=Some(recsPerMessage), offset=Some(1))).getOrElse(GetRangeRequest(minKey, maxKey, limit=Some(recsPerMessage)))
    partitionService !! req
  }

  protected def canRequestMore = lastSizeReturned == recsPerMessage

  protected def getRecordsFromFuture(ftch: MessageFuture) =
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

  protected def issueNextRequest(): MessageFuture = {
    val rec = CursorScanRequest(cursorId, recsPerMessage)
    partitionService !! rec
  }

  protected def canRequestMore = !isServerDone 
  protected def getRecordsFromFuture(ftch: MessageFuture) =
    ftch.get(3 * 60 * 1000).getOrElse(throw new RuntimeException("CursorScanRequest timedout")) match {
      case CursorScanResponse(id, recs) =>
        cursorId = id
        isServerDone = id.isEmpty
        assert(isServerDone || recs.size == recsPerMessage)
        recs
      case e => throw new RuntimeException("Invalid response to a CursorScanRequest: " + e)
    }
}

/**
 * Iterator that makes succesive GetRange requests to the specified partitionService to retrieve all values [minKey, maxKey).
 * Records are retrieved in batches of size recsPerMessage by an async Actor and are buffered upto bufferSize.
 * Note, this iterator is not threadsafe.
 */
class PartitionIterator(partitionService: PartitionService, minKey: Option[Array[Byte]], maxKey: Option[Array[Byte]], recsPerMessage: Int = 1000, bufferSize: Int = 5) extends Iterator[Record] {
  val logger = Logger()

  /**
   * Queue used to exchange record sets between the actor and the consumer of the iterator
   * Each time a recordset is retrieved a RecordSetTaken message should be sent to the iterActor
   */
  protected val availableRecordSets = new ArrayBlockingQueue[Seq[Record]](bufferSize)
  /**
   * The current record set being iterated over.
   * Note it is the responsibility of the taker of the last record to set
   * this back to None so that subsuquent calls to next/hasNext will retrieve a new RecordSet
   * from the queue.
   * A recordset with 0 or 1 items denotes the end of the iterator.
   */
  var currentRecordSet: Option[Seq[Record]] = None
  protected var positionInRecordSet = 0
  protected var done = false

  protected object RecordSetTaken
  protected val iterActor = actor {
    implicit val remoteActor = MessageHandler.registerActor(self)
    var currentKey = minKey
    var outstandingRecordSets = 0
    var done = false


    loop {
      if(!done && outstandingRecordSets < bufferSize) {
        partitionService.!(GetRangeRequest(currentKey, maxKey, limit=Some(recsPerMessage)))

        react {
          case GetRangeResponse(recs) => {
            outstandingRecordSets += 1
            if(recs.size <= 1) {
              MessageHandler.unregisterActor(remoteActor)
              availableRecordSets.offer(recs)
              done = true
            }
            else {
              availableRecordSets.offer(recs.dropRight(1))
              currentKey = Some(recs.last.key)
            }
          }
        }
      }
      else {
        react {
          case RecordSetTaken => outstandingRecordSets -= 1
        }
      }
    }
  }

  protected def getCurrentRecordSet: Seq[Record] = {
    currentRecordSet.getOrElse {
      currentRecordSet = Some(availableRecordSets.take)
      positionInRecordSet = 0
      iterActor ! RecordSetTaken
      currentRecordSet.get
    }
  }

  def next: Record = {
    val set = getCurrentRecordSet
    val result = set(positionInRecordSet)
    positionInRecordSet += 1
    if(positionInRecordSet >= set.size && set.size > 1)
      currentRecordSet = None
    result
  }

  def hasNext: Boolean = getCurrentRecordSet.size > positionInRecordSet
}
