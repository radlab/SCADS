package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.avro.marker._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

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
        updateList.append(servers, key, Some(recordToBytes(value, None)))
      }
    }
  }

  // TODO: will need the getBytes() to get metadata, similar to putBytes()
  //       re-implement a quorum protocol?
  // TODO: does get range need to collect ALL metatdata as well?
  //       could potentially be a very large list...
}

class TxRecordReaderWriter {
  private val recordReaderWriter = new AvroSpecificReaderWriter[TxRecord](None)

  def toBytes(txRec: TxRecord): Array[Byte] = {
    recordReaderWriter.serialize(txRec)
  }

  def fromBytes(bytes: Array[Byte]): TxRecord = {
    recordReaderWriter.deserialize(bytes)
  }
}

trait TransactionRecordMetadata extends SimpleRecordMetadata {

  private val recordReaderWriter = new TxRecordReaderWriter

  override def compareKey(x: Array[Byte], y: Array[Byte]): Int = 
    BinaryData.compare(x, 0, y, 0, keySchema)
 
  override def hashKey(x: Array[Byte]): Int = {
    // TODO: use some more awesome hash function

    // same hash function as java String for now
    var hash = 0
    var idx = 0
    val len = x.length
    while (idx < len) {
      hash = 31 * hash + x(idx)
      idx += 1
    }
    hash
  }

  override def createMetadata(rec: Array[Byte]): Array[Byte] = {
    recordToBytes(Some(rec), None)
  }

  def recordToBytes(rec: Option[Array[Byte]], metadata: Option[TxRecordMetadata]): Array[Byte] = {
    val newMetadata = metadata match {
      case None => {
        // TODO: No metadata for this record, create a dummy one, for now
        TxRecordMetadata(0, List[VersionMaster]())
      }
      case Some(m) => m
    }
    recordReaderWriter.toBytes(TxRecord(newMetadata, rec))
  }

  override def compareMetadata(lhs: Array[Byte], rhs: Array[Byte]): Int = {
    val txRecL = recordReaderWriter.fromBytes(lhs)
    val txRecR = recordReaderWriter.fromBytes(rhs)
    if (txRecL.metadata.version < txRecR.metadata.version)
      return -1
    else if (txRecL.metadata.version > txRecR.metadata.version)
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
    txRec.rec match {
      // Deleted records have zero length byte arrays
      case None => new Array[Byte](0)
      case Some(r) => r
    }
  }
}
