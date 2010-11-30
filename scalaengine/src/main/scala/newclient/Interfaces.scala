package edu.berkeley.cs.scads.storage.newclient

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema
import org.apache.avro.generic._

trait NamespaceLike {
  val name: String

  protected def onCreate(f: => Unit): Unit
  protected def onOpen(f: => Unit): Unit
  protected def onClose(f: => Unit): Unit
  protected def onDelete(f: => Unit): Unit

  def create(): Unit
  def open(): Unit
  def close(): Unit
  def delete(): Unit
}

trait Queryable[BulkPutType] {
  def ++=(that: TraversableOnce[BulkPutType]): Unit
}
trait KeyValueStoreLike[KeyType <: IndexedRecord, 
                        ValueType <: IndexedRecord,
                        BulkPutType] 
  extends Queryable[BulkPutType] {
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

trait Serializer[K <: IndexedRecord, V <: IndexedRecord, B] {
  def bytesToKey(bytes: Array[Byte]): K
  def bytesToValue(bytes: Array[Byte]): V
  def bytesToBulk(k: Array[Byte], v: Array[Byte]): B
  def keyToBytes(key: K): Array[Byte]
  def valueToBytes(value: V): Array[Byte]
  def bulkToBytes(b: B): (Array[Byte], Array[Byte])
}

trait KeyValueSerializer[KeyType <: IndexedRecord, ValueType <: IndexedRecord] 
  extends Serializer[KeyType, ValueType, (KeyType, ValueType)]

trait PairSerializer[PairType <: AvroPair] 
  extends Serializer[IndexedRecord, IndexedRecord, PairType]

trait Protocol {
  def getKey(key: Array[Byte]): Option[Array[Byte]]
  def putKey(key: Array[Byte], value: Array[Byte]): Boolean
  def putKeys(that: TraversableOnce[(Array[Byte], Array[Byte])]): Unit
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
trait KeyRangeRoutable extends KeyRoutable {
  case class RangeDesc(startKey: Option[Array[Byte]], endKey: Option[Array[Byte]], values: Seq[PartitionService])
  def serversForKeyRange(start: Option[Array[Byte]], end: Option[Array[Byte]]): Seq[RangeDesc]
}

trait GlobalMetadata {
  val keySchema: Schema
  val valueSchema: Schema

  def remoteKeySchema: Schema
  def remoteValueSchema: Schema
}

trait RecordMetadata {
  def compareKey(x: Array[Byte], y: Array[Byte]): Int
  def hashKey(x: Array[Byte]): Int
  def extractMetadataFromValue(value: Array[Byte]): Array[Byte]
}
