package edu.berkeley.cs.scads.storage.routing

import edu.berkeley.cs.scads.storage.{Namespace, AvroSerializing}
import org.apache.avro.generic.IndexedRecord
import edu.berkeley.cs.scads.comm.PartitionService

trait RangeRouting[KeyType <: IndexedRecord,
                      ValueType <: IndexedRecord,
                      RecordType <: IndexedRecord,
                      RType] extends RoutingTableProtocol[KeyType,
                      ValueType,
                      RecordType,
                      RType,
                      KeyType] {

  this: Namespace[KeyType, ValueType, RecordType, RType] with AvroSerializing[KeyType, ValueType, RecordType, RType] =>

  onCreate{
    ranges => {
      setup(ranges)
    }
  }

  override  protected def deserializeRoutingKey(key: Array[Byte]) : KeyType = deserializeKey(key)

  override protected def serializeRoutingKey(key : KeyType) : Array[Byte] = serializeKey(key)

  override def serversForKey(key: KeyType): Seq[PartitionService] = serversForKeyImpl(key)

  override  def serversForRange(startKey: Option[KeyType], endKey: Option[KeyType]): Seq[FullRange] = {
    var ranges = routingTable.valuesForRange(startKey, endKey)
    val result = new  Array[FullRange](ranges.size)
    var sKey: Option[KeyType] = None
    var eKey: Option[KeyType] = endKey
    for (i <- ranges.size - 1 to 0 by -1){
      if(i == 0)
        sKey = startKey
      else
        sKey = ranges(i).startKey
      result(i) = new FullRange(sKey, eKey, ranges(i).values)
      eKey = sKey
    }
    result.toSeq
  }


}
