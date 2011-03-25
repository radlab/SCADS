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
    val partitions = serversForKeyRange(startKey.map(keyToBytes), endKey.map(keyToBytes))
    partitions.map(p => new ActorlessPartitionIterator(p.servers.head, p.startKey, p.endKey))
	      .foldLeft(Iterator.empty.asInstanceOf[Iterator[Record]])(_ ++ _)
	      .map(r => bytesToBulk(r.key, extractRecordFromValue(r.value.get)))
  }
}
