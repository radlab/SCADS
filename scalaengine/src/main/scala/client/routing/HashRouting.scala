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
      val values = ranges.map(a => (a._1.map(createRoutingKey(_)) , a._2 ))

      val sortedVal = values.sortWith((e1: Tuple2[Option[IntRec], Seq[StorageService]], e2: Tuple2[Option[IntRec], Seq[StorageService]]) =>
        if(e2._1.isEmpty) false
        else if (e1._1.isEmpty) true
        else e1._1.get.f1 < e2._1.get.f1
        )

      setup(sortedVal)
    }
  }

  var routingFieldPos: Seq[Int]


  override protected def deserializeRoutingKey(data: Array[Byte]) : IntRec = {
    var key = new IntRec
    key.parse(data)
    key
  }

  protected def createRoutingKey(key : KeyType) : IntRec = {
    var hash = routingFieldPos.map(key.get(_).hashCode).reduceLeft(_ + _)
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

