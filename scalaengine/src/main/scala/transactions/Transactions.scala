package edu.berkeley.cs.scads.storage.transactions

import conflict.ConflictResolver
import edu.berkeley.cs.scads.storage._


import edu.berkeley.cs.avro.marker._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

import collection.mutable.ArrayBuffer

import mdcc._
import org.apache.avro._
import generic._
import io._
import specific._

import java.util.Comparator

import org.apache.zookeeper._

import java.util.concurrent.TimeUnit
import java.nio._

// This works with SpecificNamespace
trait Transactions[K <: SpecificRecord, V <: SpecificRecord]
extends RangeKeyValueStore[K, V]
with KeyRoutable
with ZooKeeperGlobalMetadata
with TransactionRecordMetadata
with TransactionI {

  implicit protected def valueManifest: Manifest[V]
  val icUtil = new FieldICUtil[V]

  lazy val defaultMeta = MDCCMetaDefault.getDefault(nsRoot)
  lazy val conflictResolver = new ConflictResolver(valueSchema, icUtil.getFieldICList)
  lazy val recordCache = new MDCCRecordCache()

  def getConflictResolver = conflictResolver

  def getDefaultMeta = defaultMeta.defaultMetaData

  // TODO: Don't know why implicit manifest did not work.
  val protocolType: TxProtocol = TxProtocol2pc()

  override def initRootAdditional(node: ZooKeeperProxy#ZooKeeperNode): Unit = {
    // Write the integrity constraints to zookeeper.
    val writer = new AvroSpecificReaderWriter[FieldICList](None)
    val icBytes = writer.serialize(icUtil.getFieldICList)
    root.getOrCreate(name).createChild("valueICs", icBytes,
                                       CreateMode.PERSISTENT)
  }

  def putLogical(key: K, value: V): Unit = {
    putBytesLogical(keyToBytes(key), valueToBytes(value))
    ThreadLocalStorage.protocolMap.value match {
      case Some(protocolMap) => {
        protocolMap.addNamespaceProtocol(name, protocolType)
      }
      case _ =>
    }
  }

  override def put(key: K, value: Option[V]): Unit = {
    putBytes(keyToBytes(key), value.map(v => valueToBytes(v)))
    ThreadLocalStorage.protocolMap.value match {
      case Some(protocolMap) => {
        protocolMap.addNamespaceProtocol(name, protocolType)
      }
      case _ =>
    }
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
        val putRequest = PutRequest(key,
                                    Some(MDCCRecordUtil.toBytes(value, getDefaultMeta)))
        val responses = servers.map(_ !! putRequest)
        responses.blockFor(servers.length, 500, TimeUnit.MILLISECONDS)
      }
      case Some(updateList) => {
        updateList.appendValueUpdateInfo(this, servers, key, value)
      }
    }
  }

  def putBytesLogical(key: Array[Byte],
                      value: Array[Byte]): Unit = {
    val servers = serversForKey(key)
    ThreadLocalStorage.updateList.value match {
      case None => {
        // TODO: what does it mean to do puts outside of a tx?
        //       for now, do nothing...
        throw new RuntimeException("")
      }
      case Some(updateList) => {
        updateList.appendLogicalUpdate(this, servers, key, Some(value))
      }
    }
  }

  private def getReadQuorumSize(numServers: Int): Int = {
    ThreadLocalStorage.readConsistency.value match {
      case ReadLocal() => 1
      case ReadCustom(n) => scala.math.min(n, numServers)
      case ReadConsistent() => scala.math.ceil(numServers * 0.501).toInt
      case ReadAll() => numServers
    }
  }

  // TODO: will need the getBytes() to get metadata, similar to putBytes()
  //       re-implement a quorum protocol?
  override def getBytes(key: Array[Byte]): Option[Array[Byte]] = {
    val servers = serversForKey(key)
    val getRequest = GetRequest(key)
    val responses = servers.map(_ !! getRequest)
    val handler = new GetHandlerTmp(key, responses)
    val record = handler.vote(getReadQuorumSize(servers.length))
    if (handler.failed) {
      None
    } else {
      record match {
        case None => None
        case Some(bytes) => {
          val mdccRec = MDCCRecordUtil.fromBytes(bytes)
          ThreadLocalStorage.txReadList.value.map(readList =>
            readList.addRecord(key, mdccRec))
          mdccRec.value
        }
      }
    }
  }

  // TODO: does get range need to collect ALL metatdata as well?
  //       could potentially be a very large list, and complicates code.


  // This is just modified from the quorum protocol...
  class GetHandlerTmp(val key: Array[Byte], val futures: Seq[MessageFuture[StorageMessage]], val timeout: Long = 5000) {
    private val responses = new java.util.concurrent.LinkedBlockingQueue[MessageFuture[StorageMessage]]
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

trait TransactionI {
  def getDefaultMeta() : MDCCMetadata
  def keySchema: Schema
  def valueSchema: Schema
  def getConflictResolver : ConflictResolver
  def recordCache : MDCCRecordCache

}

trait TransactionRecordMetadata extends SimpleRecordMetadata {
  override def createMetadata(rec: Array[Byte]): Array[Byte] = {
    // TODO: This will need a default metadata for it to work.
    throw new RuntimeException("createMetadata is not implemented for transactions.")
    MDCCRecordUtil.toBytes(Some(rec), null)
  }

  override def compareMetadata(lhs: Array[Byte], rhs: Array[Byte]): Int = {
    MDCCMetaHelper.compareMetadata(MDCCRecordUtil.fromBytes(lhs).metadata,  MDCCRecordUtil.fromBytes(rhs).metadata)
  }

  override def extractMetadataAndRecordFromValue(value: Array[Byte]): (Array[Byte], Array[Byte]) = {
    // TODO: This doesn't really make sense and nothing uses this so far...
    (new Array[Byte](0), new Array[Byte](0))
  }

  override def extractRecordFromValue(value: Array[Byte]): Array[Byte] = {
    val txRec = MDCCRecordUtil.fromBytes(value)
    txRec.value match {
      // Deleted records have zero length byte arrays
      case None => new Array[Byte](0)
      case Some(v) => v
    }
  }
}
