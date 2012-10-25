package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema 
import org.apache.avro.generic._
import org.apache.avro.util._

import collection.mutable.Queue
import collection.JavaConversions._

trait BaseKeyValueStoreImpl[K <: IndexedRecord, V <: IndexedRecord, B]
  extends KeyValueStoreLike[K, V, B]
  with Serializer[K, V, B]
  with Protocol
  with KeyPartitionable {

  override def ++=(that: TraversableOnce[B]) = {
    that.toIterable.map(bulkToBytes).foreach(b => bulkPutBytes(b._1, b._2))
    flushBulkBytes
  }

  override def --=(that: TraversableOnce[B]) = {
    that.toIterable.map(bulkToBytes).foreach(b => bulkPutBytes(b._1, None))
    flushBulkBytes
  }

  override def get(key: K): Option[V] =
    getBytes(keyToBytes(key)) map bytesToValue     

  override def put(key: K, value: Option[V]): Unit =
    putBytes(keyToBytes(key), value.map(v => valueToBytes(v)))

  override def incrementField(key: K, fieldName: String, amount: Int): Unit =
    incrementFieldBytes(keyToBytes(key), fieldName, amount)

  override def bulkIncrementField(key: K, fieldName: String, amount: Int): Unit =
    bulkIncrementFieldBytes(keyToBytes(key), fieldName, amount)

  override def asyncPut(key: K, value: Option[V]): ScadsFuture[Unit] =
    asyncPutBytes(keyToBytes(key), value.map(valueToBytes))

  override def asyncGet(key: K): ScadsFuture[Option[V]] =
    asyncGetBytes(keyToBytes(key)).map(_.map(bytesToValue))

  def setPartitionScheme(scheme: Seq[(Option[K], Seq[StorageService])]): Unit =
    setPartitionSchemeBytes(
      scheme.map {
        case (start, servers) => (start.map(prefix => fillOutKey(prefix, newKeyInstance _)(minVal)).map(keyToBytes), servers)
      }
    )
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
    getKeys(start.map(prefix => fillOutKey(prefix, newKeyInstance _)(minVal)).map(keyToBytes),
            end.map(prefix => fillOutKey(prefix, newKeyInstance _)(maxVal)).map(keyToBytes),
            limit,
            offset,
            ascending).map { case (k,v) => bytesToBulk(k, v) }

  override def asyncTopK(start: Option[K],
                    end: Option[K],
                    orderingFields: Seq[String],
                    k: Int, ascending: Boolean = false): ScadsFuture[Seq[B]] =
    asyncTopKBytes(start.map(prefix => fillOutKey(prefix, newKeyInstance _)(minVal)).map(keyToBytes),
              end.map(prefix => fillOutKey(prefix, newKeyInstance _)(maxVal)).map(keyToBytes),
              orderingFields,
              k, ascending).map(_.map { case Record(k, v) => bytesToBulk(k, v.get) })

  override def asyncGroupedTopK(startKey: Option[K],
                                endKey: Option[K],
                                nsAddress: String,
                                groupFields: Seq[String],
                                orderingFields: Seq[String],
                                k: Int, ascending: Boolean = false): FutureCollection[StorageMessage] =
    asyncGroupedTopKBytes(
      startKey.map(prefix => fillOutKey(prefix, newKeyInstance _)(minVal)).map(keyToBytes),
      endKey.map(prefix => fillOutKey(prefix, newKeyInstance _)(maxVal)).map(keyToBytes),
      nsAddress,
      groupFields,
      orderingFields,
      k,
      ascending)

  override def asyncGetRange(start: Option[K],
                             end: Option[K],
                             limit: Option[Int],
                             offset: Option[Int],
                             ascending: Boolean) =
    asyncGetKeys(start.map(prefix => fillOutKey(prefix, newKeyInstance _)(minVal)).map(keyToBytes), end.map(prefix => fillOutKey(prefix, newKeyInstance _)(maxVal)).map(keyToBytes), limit, offset, ascending).map(_.map { case (k, v) => bytesToBulk(k, v) })
}

/** Hash partition */
trait KeyValueStore[K <: IndexedRecord, V <: IndexedRecord]
  extends BaseKeyValueStoreImpl[K, V, (K, V)]
  with KeyValueSerializer[K, V] {

  def iterator: Iterator[(K, V)] = sys.error("iterator")

}

private[storage] object RangeKeyValueStore {
  val windowSize = 16
}

/** Range partition */
trait RangeKeyValueStore[K <: IndexedRecord, V <: IndexedRecord]
  extends BaseRangeKeyValueStoreImpl[K, V, (K, V)]
  with KeyValueSerializer[K, V] {

  import RangeKeyValueStore._

  def iterator: Iterator[(K, V)] = {
    new Iterator[(K, V)] {
      private var lastFetchKey: Option[Array[Byte]] = None
      private var isDone = false
      private val buf = new Queue[(Array[Byte], Array[Byte])]
      override def hasNext = 
        if (isDone) !buf.isEmpty 
        else {
          if (buf.isEmpty) {
            // need to fetch and update buffer/isDone flag
            val recs = getKeys(lastFetchKey, None, Some(windowSize), lastFetchKey.map(_ => 1).orElse(Some(0)), true) 
            lastFetchKey = Some(recs.last._1)
            if (recs.size < windowSize)
              isDone = true
            buf ++= recs
            !buf.isEmpty
          } else true // if buf is not empty then we have more
        }
      override def next = {
        if (!hasNext)
          throw new RuntimeException("next on empty iterator")
        val (k, v) = buf.dequeue()
        (bytesToKey(k), bytesToValue(v))
      }
    }
  }

}

trait RecordStore[RecType <: IndexedRecord] extends Namespace
  with KeyPartitionable
  with GlobalMetadata
  with BaseRangeKeyValueStoreImpl[IndexedRecord, IndexedRecord, RecType] {

  def schema: Schema

  def lookupRecord(fields: Any*): Option[RecType] = {
    val rec = new GenericData.Record(keySchema)
    fields.map {
      case s: String => new Utf8(s)
      case o => o
    }.zipWithIndex.foreach {
      case (v, idx) => rec.put(idx, v)
    }

    getRecord(rec)
  }

  def asyncGetRecord(key: IndexedRecord): ScadsFuture[Option[RecType]] = {
    val keyBytes = keyToBytes(key)
    asyncGetBytes(keyBytes) map (_.map(bytesToBulk(keyBytes, _)))
  }

  def getRecord(key: IndexedRecord): Option[RecType] = {
    val keyBytes = keyToBytes(key)

    getBytes(keyBytes).map(bytesToBulk(keyBytes, _))
  }

  /**
   * Reads the first 1000 * clustersize / replication factor keys from the sequence and
   * repartitions the cluster so the data is distributed evenly.  Note the data in the
   * sequence must be randomly distributed for this to work.
   */
  def repartition(data: Seq[RecType], replicationFactor: Int): Unit = {
    val servers = cluster.getAvailableServers
    val samplesPerServer = 10000
    require(servers.size % replicationFactor == 0, "num servers must divide evenly by replication factor")
    val numPartitions = servers.size / replicationFactor
    val numSamples = samplesPerServer * numPartitions

    val samples = data.take(numSamples).sortWith(_ < _).toIndexedSeq
    val splitSize = samples.size / numPartitions
    val splits = None +: (splitSize until splitSize * numPartitions by splitSize).map(samples).map(Some(_))

    logger.info("Sampled Key Splits for %s: %s", namespace, splits)
    val partitions = splits zip servers.grouped(replicationFactor).toList
    logger.info("Sampled Partition Scheme for %s: %s", namespace , partitions)

    setPartitionScheme(partitions)
  }
}
