package edu.berkeley.cs.scads.storage.routing

import org.apache.avro.generic.IndexedRecord
import edu.berkeley.cs.scads.comm.{IntRec, PartitionService}
import edu.berkeley.cs.scads.storage.{Namespace, AvroSerializing}

///**
// * Created by IntelliJ IDEA.
// * User: tim
// * Date: Nov 18, 2010
// * Time: 3:15:42 PM
// * To change this template use File | Settings | File Templates.
// */
//
//trait HashRouting[KeyType <: IndexedRecord,
//                      ValueType <: IndexedRecord,
//                      RecordType <: IndexedRecord,
//                      RType]  extends RoutingProtocol[KeyType, ValueType, RecordType, RType] {
//
//  this: Namespace[KeyType, ValueType, RecordType, RType] with AvroSerializing[KeyType, ValueType, RecordType, RType] =>
//
//  override def serversForKey(key: KeyType): Seq[PartitionService] = {
//    //val hash = new IntRec(key.hashCode);
//    super.serversForKey(key)
//  }
//
//}