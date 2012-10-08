package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._

import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.avro.Schema.Type
import org.apache.avro.util.Utf8

import scala.collection.JavaConversions._

trait PersistentStore[BulkPutType] {
  def ++=(that: TraversableOnce[BulkPutType]): Unit
  def --=(that: TraversableOnce[BulkPutType]): Unit
}

trait KeyValueStoreLike[KeyType <: IndexedRecord, 
                        ValueType <: IndexedRecord,
                        BulkPutType]
  extends PersistentStore[BulkPutType] {
  def get(key: KeyType): Option[ValueType]
  def put(key: KeyType, value: Option[ValueType]): Unit
  def incrementField(key: KeyType, fieldName: String, amount: Int): Unit
  def bulkIncrementField(key: KeyType, fieldName: String, amount: Int): Unit
  def asyncPut(key: KeyType, value: Option[ValueType]): ScadsFuture[Unit]

  def asyncGet(key: KeyType): ScadsFuture[Option[ValueType]]
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

  def asyncGetRange(start: Option[KeyType], 
                    end: Option[KeyType], 
                    limit: Option[Int] = None, 
                    offset: Option[Int] = None, 
                    ascending: Boolean = true): ScadsFuture[Seq[RangeType]]

  def topK(start: Option[KeyType],
           end: Option[KeyType],
           orderingFields: Seq[String],
           k: Int, ascending: Boolean = false): Seq[RangeType]
}


private[storage] object Serializer {
  final val MinString = ""
  final val MaxString = new String(Array.fill[Byte](20)(127.asInstanceOf[Byte]))
}

trait Serializer[KeyType <: IndexedRecord, ValueType <: IndexedRecord, BulkType] {
  import Serializer._

  def bytesToKey(bytes: Array[Byte]): KeyType
  def bytesToValue(bytes: Array[Byte]): ValueType
  def bytesToBulk(k: Array[Byte], v: Array[Byte]): BulkType
  def keyToBytes(key: KeyType): Array[Byte]
  def valueToBytes(value: ValueType): Array[Byte]
  def bulkToBytes(b: BulkType): (Array[Byte], Array[Byte])

  protected def minVal(fieldType: Type, fieldSchema: Schema): Any = fieldType match {
    case Type.BOOLEAN => false
    case Type.DOUBLE => java.lang.Double.MIN_VALUE
    case Type.FLOAT => java.lang.Float.MIN_VALUE
    case Type.INT => java.lang.Integer.MIN_VALUE
    case Type.LONG => java.lang.Long.MIN_VALUE
    case Type.STRING => new Utf8(MinString)
    case Type.RECORD =>
      fillOutKey(newRecord(fieldSchema), () => newRecord(fieldSchema))(minVal _)
    case unsupportedType =>
      throw new RuntimeException("Invalid key type in partial key getRange. " + unsupportedType + " not supported for inquality queries.")
  }

  protected def maxVal(fieldType: Type, fieldSchema: Schema): Any = fieldType match {
    case Type.BOOLEAN => true
    case Type.DOUBLE => java.lang.Double.MAX_VALUE
    case Type.FLOAT => java.lang.Float.MAX_VALUE
    case Type.INT => java.lang.Integer.MAX_VALUE
    case Type.LONG => java.lang.Long.MAX_VALUE
    case Type.STRING => new Utf8(MaxString)
    case Type.RECORD =>
      fillOutKey(newRecord(fieldSchema), () => newRecord(fieldSchema))(maxVal _)
    case unsupportedType =>
      throw new RuntimeException("Invalid key type in partial key getRange. " + unsupportedType + " not supported for inquality queries.")
  }

  protected def fillOutKey[R <: IndexedRecord](keyPrefix: R, keyFactory: () => R)(fillFunc: (Type, Schema) => Any): R = {
    val filledRec = keyFactory()

    filledRec.getSchema.getFields.foreach(field => {
      if(keyPrefix.get(field.pos) == null)
       filledRec.put(field.pos, fillFunc(field.schema.getType, field.schema))
      else
       filledRec.put(field.pos, keyPrefix.get(field.pos))
    })
    filledRec
  }

  def newRecord(schema: Schema): IndexedRecord
  def newKeyInstance: KeyType
}

trait KeyValueSerializer[KeyType <: IndexedRecord, ValueType <: IndexedRecord] 
  extends Serializer[KeyType, ValueType, (KeyType, ValueType)]

trait PairSerializer[PairType <: AvroPair] 
  extends Serializer[IndexedRecord, IndexedRecord, PairType]

trait Protocol {
  def getBytes(key: Array[Byte]): Option[Array[Byte]]
  def putBytes(key: Array[Byte], value: Option[Array[Byte]]): Unit
  def asyncPutBytes(key: Array[Byte], value: Option[Array[Byte]]): ScadsFuture[Unit]

  def incrementFieldBytes(key: Array[Byte], fieldName: String, amount: Int): Unit
  def bulkIncrementFieldBytes(key: Array[Byte], fieldName: String, amount: Int): Unit

  def flushBulkBytes(): Unit
  def bulkPutBytes(key: Array[Byte], value: Option[Array[Byte]]): Unit

  def asyncGetBytes(key: Array[Byte]): ScadsFuture[Option[Array[Byte]]]
}

trait RangeProtocol extends Protocol {
  def getKeys(start: Option[Array[Byte]], 
              end: Option[Array[Byte]], 
              limit: Option[Int], 
              offset: Option[Int], 
              ascending: Boolean): Seq[(Array[Byte], Array[Byte])]

  def topKBytes(startKey: Option[Array[Byte]], endKey: Option[Array[Byte]], orderingFields: Seq[String], k: Int, ascending: Boolean = false): Seq[Record]

  def asyncGetKeys(start: Option[Array[Byte]], 
                   end: Option[Array[Byte]], 
                   limit: Option[Int], 
                   offset: Option[Int], 
                   ascending: Boolean): ScadsFuture[Seq[(Array[Byte], Array[Byte])]]

  def putBulkLocations(parser:RecParser,
                       locations:Array[String],
                       firstKey:Option[Array[Byte]],
                       lastKey:Option[Array[Byte]])
}

trait KeyRoutable {
  def serversForKey(key: Array[Byte]): Seq[PartitionService]

  // type of partition this routable talks to
  def partitionType:String

  protected def onRoutingTableChanged(newTable: Array[Byte]): Unit
  protected def convertToRoutingKey(key: Array[Byte]): Array[Byte]
}

case class RangeDesc(startKey: Option[Array[Byte]], endKey: Option[Array[Byte]], servers: Seq[PartitionService])
trait KeyRangeRoutable extends KeyRoutable {
  def serversForKeyRange(start: Option[Array[Byte]], end: Option[Array[Byte]]): Seq[RangeDesc]

  protected def convertFromRoutingKey(key: Array[Byte]): Array[Byte]
}

trait KeyPartitionable {
  def splitPartition(splitKeys: Seq[Array[Byte]]): Unit
  def mergePartition(mergeKeys: Seq[Array[Byte]]): Unit

  def deletePartitions(partitionHandlers: Seq[PartitionService]): Unit
  /** For each target, replace the PartitionService on the StorageService */
  def replicatePartitions(targets: Seq[(PartitionService, StorageService)]): Seq[PartitionService]

  /** sets the partition scheme to be exactly that which is represented by the input.
   * any previously existing partitions are thrown away. it is required that
   * length of (scheme) >= 1 and that the 1st split key is None */
  def setPartitionSchemeBytes(scheme: Seq[(Option[Array[Byte]], Seq[StorageService])]): Unit

  def routingTable: RangeTable[Array[Byte], PartitionService]
}

trait GlobalMetadata {
  def name: String

  def keySchema: Schema
  def valueSchema: Schema

  def valueClass: String

  def remoteKeySchema: Schema
  def remoteValueSchema: Schema

  /** The GlobalMetadata catalogue is implementation agnostic, but must
   * support a simple persistent key/value configuration map (string -> byte array) */
  def watchMetadata(key: String, func: () => Unit): Array[Byte]
  def getMetadata(key: String): Option[Array[Byte]] 
  def putMetadata(key: String, value: Array[Byte]): Unit 
  def deleteMetadata(key: String): Unit
  def waitUntilMetadataPropagated(): Unit
}

trait PairGlobalMetadata extends GlobalMetadata {
  def pairSchema: Schema
}

trait RecordMetadata {
  def compareKey(x: Array[Byte], y: Array[Byte]): Int
  def hashKey(x: Array[Byte]): Int

  /** Create a value w/ metadata prepended */
  def createMetadata(rec: Array[Byte]): Array[Byte]

  /** Compare 2 values based on metadata */
  def compareMetadata(lhs: Array[Byte], rhs: Array[Byte]): Int

  /** Given a byte string which contains both value and metadata, extracts
   * this information and returns a tuple of (metadata, value) */
  def extractMetadataAndRecordFromValue(value: Array[Byte]): (Array[Byte], Array[Byte])

  /** strips out the metadata from the value. exists for performance
   reasons (is NOT implemented as extractMetadataAndRecordFromValue(value)._2) */
  def extractRecordFromValue(value: Array[Byte]): Array[Byte] 
}
