package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.avro.marker._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

import collection.mutable.ArrayBuffer

import org.apache.avro._
import generic._
import io._
import specific._

import java.util.Comparator

import java.util.concurrent.TimeUnit
import java.nio._


// This works with SpecificNamespace
trait Transactions[K <: SpecificRecord, V <: SpecificRecord]
extends RangeKeyValueStore[K, V]
with KeyRoutable
with TransactionRecordMetadata {

  def putLogical(key: K, value: V): Unit = {
    val schema = value.getSchema.toString
    putBytesLogical(keyToBytes(key), schema, valueToBytes(value))
  }

  override def put(key: K, value: Option[V]): Unit = {
    putBytes(keyToBytes(key), value.map(v => valueToBytes(v)))
  }

  override def get(key: K): Option[V] = {
    getBytes(keyToBytes(key)).map(b =>
      // Don't return a record which was deleted
      if (b.length == 0)
        return None
      else
        return Some(bytesToValue(b))
    )
  }

  override def getRange(start: Option[K], 
                        end: Option[K], 
                        limit: Option[Int] = None, 
                        offset: Option[Int] = None, 
                        ascending: Boolean = true) = {
    // Filter out records which were deleted
    getKeys(start.map(prefix => fillOutKey(prefix, newKeyInstance _)(minVal)).map(keyToBytes), end.map(prefix => fillOutKey(prefix, newKeyInstance _)(maxVal)).map(keyToBytes), limit, offset, ascending).filter {
      case(k,v) => v.length > 0
    } map { 
      case (k,v) => bytesToBulk(k, v) 
    }
  }

  override def putBytes(key: Array[Byte], value: Option[Array[Byte]]): Unit = {
    val servers = serversForKey(key)
    ThreadLocalStorage.updateList.value match {
      case None => {
        // TODO: what does it mean to do puts outside of a tx?
        //       for now, just writes to all servers
        val putRequest = PutRequest(key, Some(recordToBytes(value, None)))
        val responses = servers.map(_ !! putRequest)
        responses.blockFor(servers.length, 500, TimeUnit.MILLISECONDS)
      }
      case Some(updateList) => {
        updateList.appendVersionUpdate(servers, key, recordToBytes(value, None))
      }
    }
  }

  def putBytesLogical(key: Array[Byte],
                      schema: String,
                      value: Array[Byte]): Unit = {
    val servers = serversForKey(key)
    ThreadLocalStorage.updateList.value match {
      case None => {
        // TODO: what does it mean to do puts outside of a tx?
        //       for now, do nothing...
        throw new RuntimeException("")
      }
      case Some(updateList) => {
        updateList.appendLogicalUpdate(servers, key,
                                       schema,
                                       recordToBytes(Some(value), None))
      }
    }
  }

  // TODO: will need the getBytes() to get metadata, similar to putBytes()
  //       re-implement a quorum protocol?
  override def getBytes(key: Array[Byte]): Option[Array[Byte]] = {
    val servers = serversForKey(key)
    val getRequest = GetRequest(key)
    val responses = servers.map(_ !! getRequest)
    val handler = new GetHandlerTmp(key, responses)
    val record = handler.vote(1)
    if (handler.failed) {
      None
    } else {
      record.map(extractRecordFromValue)
    }
  }

  // TODO: does get range need to collect ALL metatdata as well?
  //       could potentially be a very large list, and complicates code.


  // This is just modified from the quorum protocol...
  class GetHandlerTmp(val key: Array[Byte], val futures: Seq[MessageFuture], val timeout: Long = 5000) {
    private val responses = new java.util.concurrent.LinkedBlockingQueue[MessageFuture]
    futures.foreach(_.forward(responses))
    var winnerValue: Option[Array[Byte]] = None
    private var ctr = 0
    val timeoutCounter = new TimeoutCounter(timeout)
    var failed = false

    def vote(quorum: Int): Option[Array[Byte]] = {
      (1 to quorum).foreach(_ => compareNext())
      return winnerValue
    }

    private def compareNext(): Unit = {
      ctr += 1
      val future = responses.poll(timeoutCounter.remaining, TimeUnit.MILLISECONDS)
      if (future == null) {
        failed = true
        return
      }
      future() match {
        case GetResponse(v) => {
          val cmp = optCompareMetadataTmp(winnerValue, v)
          if (cmp > 0) {
          } else if (cmp < 0) {
            winnerValue = v
          }else {
          }
        }
        case m => throw new RuntimeException("Unknown message " + m)
      }
    }
  }

  // This is just modified from the quorum protocol...
  protected def optCompareMetadataTmp(optLhs: Option[Array[Byte]], optRhs: Option[Array[Byte]]): Int = (optLhs, optRhs) match {
    case (None, None) => 0
    case (None, Some(_)) => -1
    case (Some(_), None) => 1
    case (Some(lhs), Some(rhs)) => compareMetadata(lhs, rhs)
  }

}

class MDCCRecordReaderWriter {
  private val recordReaderWriter = new AvroSpecificReaderWriter[MDCCRecord](None)

  def toBytes(txRec: MDCCRecord): Array[Byte] = {
    recordReaderWriter.serialize(txRec)
  }

  def fromBytes(bytes: Array[Byte]): MDCCRecord = {
    recordReaderWriter.deserialize(bytes)
  }
}

trait TransactionRecordMetadata extends SimpleRecordMetadata {

  private val recordReaderWriter = new MDCCRecordReaderWriter

  override def createMetadata(rec: Array[Byte]): Array[Byte] = {
    recordToBytes(Some(rec), None)
  }

  def recordToBytes(rec: Option[Array[Byte]], metadata: Option[MDCCMetadata]): Array[Byte] = {
    val newMetadata = metadata.getOrElse(MDCCMetadata(0, List()))
    recordReaderWriter.toBytes(MDCCRecord(rec, newMetadata))
  }

  override def compareMetadata(lhs: Array[Byte], rhs: Array[Byte]): Int = {
    val txRecL = recordReaderWriter.fromBytes(lhs)
    val txRecR = recordReaderWriter.fromBytes(rhs)
    if (txRecL.metadata.currentRound < txRecR.metadata.currentRound)
      return -1
    else if (txRecL.metadata.currentRound > txRecR.metadata.currentRound)
      return 1
    else
      return 0
  }

  override def extractMetadataAndRecordFromValue(value: Array[Byte]): (Array[Byte], Array[Byte]) = {
    // TODO: This doesn't really make sense and nothing uses this so far...
    (new Array[Byte](0), new Array[Byte](0))
  }

  override def extractRecordFromValue(value: Array[Byte]): Array[Byte] = {
    val txRec = recordReaderWriter.fromBytes(value)
    txRec.value match {
      // Deleted records have zero length byte arrays
      case None => new Array[Byte](0)
      case Some(v) => v
    }
  }
}
