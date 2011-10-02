package edu.berkeley.cs.scads.storage.transactions
import edu.berkeley.cs.scads.storage._

import edu.berkeley.cs.avro.marker._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

import collection.mutable.ArrayBuffer

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
with TransactionRecordMetadata {

  implicit protected def valueManifest: Manifest[V]

  // TODO: Don't know why implicit manifest did not work.
  val protocolType: TxProtocol = TxProtocol2pc()

  // r is the new restriction, and l is the (optionally) existing restriction.
  // r is a lower restriction (GT or GE).
  private def getLowerRestriction(r: FieldRestriction,
                                  l: Option[FieldRestriction]) = {
    val newRestriction = (r, l.getOrElse(r)) match {
      case (FieldRestrictionGT(x), FieldRestrictionGT(y)) =>
        FieldRestrictionGT(math.max(x, y))
      case (FieldRestrictionGT(x), FieldRestrictionGE(y)) =>
        if (x >= y) {
          FieldRestrictionGT(x)
        } else {
          FieldRestrictionGE(y)
        }
      case (FieldRestrictionGE(x), FieldRestrictionGT(y)) =>
        if (y >= x) {
          FieldRestrictionGT(y)
        } else {
          FieldRestrictionGE(x)
        }
      case (FieldRestrictionGE(x), FieldRestrictionGE(y)) =>
        FieldRestrictionGE(math.max(x, y))
      case (x, _) => x
    }
    Some(newRestriction)
  }

  // r is the new restriction, and u is the (optionally) existing restriction.
  // r is an upper restriction (LT or LE).
  private def getUpperRestriction(r: FieldRestriction,
                                  u: Option[FieldRestriction]) = {
    val newRestriction = (r, u.getOrElse(r)) match {
      case (FieldRestrictionLT(x), FieldRestrictionLT(y)) =>
        FieldRestrictionLT(math.min(x, y))
      case (FieldRestrictionLT(x), FieldRestrictionLE(y)) =>
        if (x <= y) {
          FieldRestrictionLT(x)
        } else {
          FieldRestrictionLE(y)
        }
      case (FieldRestrictionLE(x), FieldRestrictionLT(y)) =>
        if (y <= x) {
          FieldRestrictionLT(y)
        } else {
          FieldRestrictionLE(x)
        }
      case (FieldRestrictionLE(x), FieldRestrictionLE(y)) =>
        FieldRestrictionLE(math.min(x, y))
      case (x, _) => x
    }
    Some(newRestriction)
  }

  private def foldRestrictions(t: (Option[FieldRestriction],
                                   Option[FieldRestriction]),
                               r: FieldRestriction) = {
    (t, r) match {
      case ((l, u), x:FieldRestrictionGT) => (getLowerRestriction(x, l), u)
      case ((l, u), x:FieldRestrictionGE) => (getLowerRestriction(x, l), u)
      case ((l, u), x:FieldRestrictionLT) => (l, getUpperRestriction(x, u))
      case ((l, u), x:FieldRestrictionLE) => (l, getUpperRestriction(x, u))
    }
  }

  override def initRootAdditional(node: ZooKeeperProxy#ZooKeeperNode): Unit = {
    val fields = valueManifest.erasure.asInstanceOf[Class[V]].getDeclaredFields
    val rlist = fields.map(f => {
      val annotations = f.getDeclaredAnnotations

      if (annotations.length > 0) {
        val fieldPos = valueSchema.getField(f.getName).pos()

        val restrictions = annotations.map(a => {
          a match {
            case x: JavaFieldAnnotations.JavaFieldGT => FieldRestrictionGT(x.value)
            case x: JavaFieldAnnotations.JavaFieldGE => FieldRestrictionGE(x.value)
            case x: JavaFieldAnnotations.JavaFieldLT => FieldRestrictionLT(x.value)
            case x: JavaFieldAnnotations.JavaFieldLE => FieldRestrictionLE(x.value)
          }
        }).foldLeft[(Option[FieldRestriction], Option[FieldRestriction])]((None, None))(foldRestrictions _)

        FieldIC(fieldPos, restrictions._1, restrictions._2)
      } else {
        // No annotations on the field.
        null
      }
    }).filter(_ != null)

    // Write the integrity constraints to zookeeper.
    val writer = new AvroSpecificReaderWriter[FieldICList](None)
    val icBytes = writer.serialize(FieldICList(rlist))
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
                                    Some(MDCCRecordUtil.toBytes(value, None)))
        val responses = servers.map(_ !! putRequest)
        responses.blockFor(servers.length, 500, TimeUnit.MILLISECONDS)
      }
      case Some(updateList) => {
        updateList.appendValueUpdateInfo(servers, key, value)
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
        updateList.appendLogicalUpdate(servers, key, Some(value))
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
    val record = handler.vote(servers.length)
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

object MDCCRecordUtil {
  private val recordReaderWriter = new AvroSpecificReaderWriter[MDCCRecord](None)

  def toBytes(rec: MDCCRecord): Array[Byte] = {
    recordReaderWriter.serialize(rec)
  }

  def toBytes(rec: Option[Array[Byte]], metadata: Option[MDCCMetadata]): Array[Byte] = {
    recordReaderWriter.serialize(MDCCRecord(rec, metadata.getOrElse(MDCCMetadata(0, List()))))
  }

  def fromBytes(bytes: Array[Byte]): MDCCRecord = {
    recordReaderWriter.deserialize(bytes)
  }  
}

trait TransactionRecordMetadata extends SimpleRecordMetadata {
  override def createMetadata(rec: Array[Byte]): Array[Byte] = {
    MDCCRecordUtil.toBytes(Some(rec), None)
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
