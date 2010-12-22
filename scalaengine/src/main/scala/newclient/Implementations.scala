package edu.berkeley.cs.scads.storage.newclient

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema 
import org.apache.avro.generic._

trait BaseKeyValueStoreImpl[K <: IndexedRecord, V <: IndexedRecord, B]
  extends KeyValueStoreLike[K, V, B]
  with Serializer[K, V, B]
  with Protocol {

  override def ++=(that: TraversableOnce[B]) = 
    putBulkBytes(that.toIterable.map(bulkToBytes))

  override def get(key: K): Option[V] =
    getBytes(keyToBytes(key)) map bytesToValue     

  override def put(key: K, value: V): Boolean =
    putBytes(keyToBytes(key), valueToBytes(value))

}

trait BaseRangeKeyValueStoreImpl[K <: IndexedRecord, V <: IndexedRecord, B]
  extends RangeKeyValueStoreLike[K, V, B]
  with BaseKeyValueStoreImpl[K, V, B]
  with RangeProtocol {

  override def getRange(start: Option[K],
                        end: Option[K],
                        limit: Option[Int],
                        offset: Option[Int],
                        ascending: Boolean) =
    getKeys(start.map(keyToBytes), end.map(keyToBytes), limit, offset, ascending).map { 
      case (k,v) => bytesToBulk(k, v) 
    }
}


/** Hash partition */
trait KeyValueStore[K <: IndexedRecord, V <: IndexedRecord]
  extends BaseKeyValueStoreImpl[K, V, (K, V)]
  with KeyValueSerializer[K, V]

/** Range partition */
trait RangeKeyValueStore[K <: IndexedRecord, V <: IndexedRecord]
  extends BaseRangeKeyValueStoreImpl[K, V, (K, V)]
  with KeyValueSerializer[K, V]
  with Iterable[(K, V)] {

  override def iterator: Iterator[(K, V)] = error("iterator")

}

/** avro pair */
trait RecordStore[P <: AvroPair]
  extends BaseRangeKeyValueStoreImpl[IndexedRecord, IndexedRecord, P]
  with PairSerializer[P]

/** indexes */
trait IndexStore
  extends RangeKeyValueStore[IndexedRecord, IndexedRecord]

trait AvroKeyValueSerializer[K <: IndexedRecord, V <: IndexedRecord] 
  extends KeyValueSerializer[K, V] {
  override def bytesToKey(bytes: Array[Byte]): K = error("bytesToKey")
  override def bytesToValue(bytes: Array[Byte]): V = error("bytesToValue") 
  override def bytesToBulk(k: Array[Byte], v: Array[Byte]) = error("bytesToBulk")
  override def keyToBytes(key: K): Array[Byte] = error("keyToBytes")
  override def valueToBytes(value: V): Array[Byte] = error("valueToBytes")
  override def bulkToBytes(b: (K, V)) = error("bulkToBytes")
}

trait AvroPairSerializer[P <: AvroPair]
  extends PairSerializer[P]

trait QuorumProtocol 
  extends Protocol
  with KeyRoutable
  with RecordMetadata {

  override def getBytes(key: Array[Byte]): Option[Array[Byte]] = error("getKey")
  override def putBytes(key: Array[Byte], value: Array[Byte]): Boolean = error("putKey")
  override def putBulkBytes(that: TraversableOnce[(Array[Byte], Array[Byte])]): Unit = error("putKeys")

}

trait QuorumRangeProtocol 
  extends RangeProtocol
  with QuorumProtocol
  with KeyRangeRoutable {

  override def getKeys(start: Option[Array[Byte]], 
                       end: Option[Array[Byte]], 
                       limit: Option[Int], 
                       offset: Option[Int], 
                       ascending: Boolean): Seq[(Array[Byte], Array[Byte])] = error("getKeys")

}

trait DefaultKeyRoutable extends KeyRoutable with Namespace {
  override def serversForKey(key: Array[Byte]): Seq[PartitionService] = error("serversForKey")
  override def onRoutingTableChanged(newTable: Array[Byte]): Unit = error("onRoutingTableChanged")
}

trait DefaultKeyRangeRoutable extends DefaultKeyRoutable with KeyRangeRoutable {
  override def serversForKeyRange(start: Option[Array[Byte]], end: Option[Array[Byte]]): Seq[RangeDesc] = error("serversForKeyRange")
}

trait ZooKeeperGlobalMetadata extends GlobalMetadata with Namespace with KeyRoutable {
  val root: ZooKeeperProxy#ZooKeeperNode
  def name: String = root.name
  override def remoteKeySchema: Schema = error("remoteKeySchema")
  override def remoteValueSchema: Schema = error("remoteValueSchema")
}

trait SimpleRecordMetadata extends RecordMetadata {
  override def compareKey(x: Array[Byte], y: Array[Byte]): Int = error("compareKey")
  override def hashKey(x: Array[Byte]): Int = error("hashKey")
  override def extractMetadataFromValue(value: Array[Byte]): Array[Byte] = error("extractMetadataFromValue")
}
