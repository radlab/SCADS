package edu.berkeley.cs
package scads
package storage

import comm._
import org.apache.avro.generic.IndexedRecord
 
trait NamespaceIterator[BulkType] {
  self: Namespace 
    with KeyRangeRoutable
    with RecordMetadata
    with Serializer[IndexedRecord, IndexedRecord, BulkType] =>

  def iterateOverRange(startKey: Option[IndexedRecord], endKey: Option[IndexedRecord]): Iterator[BulkType] = {
    val (keyBytes, valueBytes) = (startKey.map(keyToBytes), endKey.map(valueToBytes))
    val partitions = serversForKeyRange(keyBytes, valueBytes)
    partitions.map(p => new ActorlessPartitionIterator(p.servers.head, keyBytes, valueBytes))
	      .foldLeft(Iterator.empty.asInstanceOf[Iterator[Record]])(_ ++ _)
	      .map(r => bytesToBulk(r.key, extractRecordFromValue(r.value.get)))
  }
}
