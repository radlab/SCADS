package edu.berkeley.cs.scads.storage.routing

import org.apache.avro.generic.{GenericData, IndexedRecord}
import edu.berkeley.cs.scads.storage.{AvroSpecificReaderWriter, Namespace, AvroSerializing}
import org.apache.avro.io.ResolvingDecoder
import edu.berkeley.cs.scads.comm.{StorageService, IntRec, PartitionService}

trait HashRouting[KeyType <: IndexedRecord,
                      ValueType <: IndexedRecord,
                      RecordType <: IndexedRecord,
                      RType] extends RoutingTableProtocol[KeyType,
                      ValueType,
                      RecordType,
                      RType,
                      IntRec] {

  this: Namespace[KeyType, ValueType, RecordType, RType] with AvroSerializing[KeyType, ValueType, RecordType, RType] =>

  onCreate{
    ranges : Seq[(Option[KeyType], Seq[StorageService])]  => {
      setup(ranges.map(a => (a._1.map(createRoutingKey(_)) , a._2 )))
    }
  }


  protected var fieldPositions: Seq[Int]


  override protected def deserializeRoutingKey(data: Array[Byte]) : IntRec = {
    var key = new IntRec
    key.parse(data)
    key
  }

  protected def createRoutingKey(key : KeyType) : IntRec = {
    var hash = fieldPositions.map(key.get(_).hashCode).reduceLeft(_ + _)
    IntRec(hash)
  }

  override protected def serializeRoutingKey(key : IntRec) : Array[Byte] = {
    key.toBytes
  }

  override def serversForKey(key: KeyType): Seq[PartitionService] =  {
    val lookupKey = createRoutingKey(key)
    serversForKeyImpl(lookupKey)
  }




  override  def serversForRange(startKey: Option[KeyType], endKey: Option[KeyType]): Seq[FullRange] = {
    throw new RuntimeException("Range scans are not supported with hash partitioning")
  }


}

