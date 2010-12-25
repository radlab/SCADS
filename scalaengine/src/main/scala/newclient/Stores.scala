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

  override def put(key: K, value: Option[V]): Boolean =
    putBytes(keyToBytes(key), value.map(v => valueToBytes(v)))

  override def asyncGet(key: K): ScadsFuture[Option[V]] =
    asyncGetBytes(keyToBytes(key)).map(_.map(bytesToValue))
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

  override def asyncGetRange(start: Option[K],
                             end: Option[K],
                             limit: Option[Int],
                             offset: Option[Int],
                             ascending: Boolean) =
    asyncGetKeys(start.map(keyToBytes), end.map(keyToBytes), limit, offset, ascending).map(_.map { case (k, v) => bytesToBulk(k, v) })
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
