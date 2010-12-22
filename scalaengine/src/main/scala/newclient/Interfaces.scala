package edu.berkeley.cs.scads.storage.newclient

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema
import org.apache.avro.generic._

trait PersistentStore[BulkPutType] {
  def ++=(that: TraversableOnce[BulkPutType]): Unit
}
trait KeyValueStoreLike[KeyType <: IndexedRecord, 
                        ValueType <: IndexedRecord,
                        BulkPutType] 
  extends PersistentStore[BulkPutType] {
  def get(key: KeyType): Option[ValueType]
  def put(key: KeyType, value: ValueType): Boolean
}
trait RangeKeyValueStoreLike[KeyType <: IndexedRecord,
                             ValueType <: IndexedRecord,
                             RangeType]
  extends KeyValueStoreLike[KeyType, ValueType, RangeType] {
  def getRange(start: Option[KeyType], 
               end: Option[KeyType], 
               limit: Option[Int] = None, 
               offset: Option[Int] = None, 
               ascending: Boolean = true): Seq[RangeType]
}

trait Serializer[KeyType <: IndexedRecord, ValueType <: IndexedRecord, BulkType] {
  def bytesToKey(bytes: Array[Byte]): KeyType
  def bytesToValue(bytes: Array[Byte]): ValueType
  def bytesToBulk(k: Array[Byte], v: Array[Byte]): BulkType
  def keyToBytes(key: KeyType): Array[Byte]
  def valueToBytes(value: ValueType): Array[Byte]
  def bulkToBytes(b: BulkType): (Array[Byte], Array[Byte])
}

trait KeyValueSerializer[KeyType <: IndexedRecord, ValueType <: IndexedRecord] 
  extends Serializer[KeyType, ValueType, (KeyType, ValueType)]

trait PairSerializer[PairType <: AvroPair] 
  extends Serializer[IndexedRecord, IndexedRecord, PairType]

trait Protocol {
  def getBytes(key: Array[Byte]): Option[Array[Byte]]
  def putBytes(key: Array[Byte], value: Array[Byte]): Boolean
  def putBulkBytes(that: TraversableOnce[(Array[Byte], Array[Byte])]): Unit
}
trait RangeProtocol extends Protocol {
  def getKeys(start: Option[Array[Byte]], 
              end: Option[Array[Byte]], 
              limit: Option[Int], 
              offset: Option[Int], 
              ascending: Boolean): Seq[(Array[Byte], Array[Byte])]
}

trait KeyRoutable {
  def serversForKey(key: Array[Byte]): Seq[PartitionService]
  def onRoutingTableChanged(newTable: Array[Byte]): Unit
}

case class RangeDesc(startKey: Option[Array[Byte]], endKey: Option[Array[Byte]], servers: Seq[PartitionService])
trait KeyRangeRoutable extends KeyRoutable {
  def serversForKeyRange(start: Option[Array[Byte]], end: Option[Array[Byte]]): Seq[RangeDesc]
}

trait KeyPartitionable {
  def splitPartition(splitKeys: Seq[Array[Byte]]): Unit
  def mergePartition(mergeKeys: Seq[Array[Byte]]): Unit
  def deletePartitions(partitionHandlers: Seq[PartitionService]): Unit
  /** For each target, replace the PartitionService on the StorageService */
  def replicatePartitions(targets: Seq[(PartitionService, StorageService)]): Seq[PartitionService]
}

trait GlobalMetadata {
  def keySchema: Schema
  def valueSchema: Schema

  def remoteKeySchema: Schema
  def remoteValueSchema: Schema
}

trait TypedGlobalMetadata[T <: IndexedRecord] extends GlobalMetadata {
  /** This is a necessary evil because traits cannot take implicit parameters */
  implicit def getManifest: Manifest[T] 
}

trait RecordMetadata {
  def compareKey(x: Array[Byte], y: Array[Byte]): Int
  def hashKey(x: Array[Byte]): Int
  def extractMetadataFromValue(value: Array[Byte]): Array[Byte]
}
