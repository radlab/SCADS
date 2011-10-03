package edu.berkeley.cs
package scads
package storage

import avro.marker.{AvroPair, AvroRecord, AvroUnion}
import comm._

sealed trait StorageMessage extends AvroUnion

object StorageService {
  def apply(s: RemoteServiceProxy[StorageMessage]):StorageService =
    StorageService(s.host, s.port, s.id)
}

/* Specific types for different services. Note: these types are mostly for readability as typesafety isn't enforced when serialized individualy*/
case class StorageService(var host: String,
                          var port: Int,
                          var id: ServiceId) extends AvroRecord with RemoteServiceProxy[StorageMessage] {
  registry = StorageRegistry
}

object PartitionService {
  def apply(s: RemoteServiceProxy[StorageMessage], partitionId: String, storageService: StorageService): PartitionService =
    PartitionService(s.host, s.port, s.id, partitionId, storageService)
}

case class PartitionService(var host: String,
                            var port: Int,
                            var id: ServiceId,
                            var partitionId: String,
                            var storageService: StorageService) extends AvroRecord with RemoteServiceProxy[StorageMessage] {
  registry = StorageRegistry
}

case class Record(var key: Array[Byte], var value: Option[Array[Byte]]) extends AvroRecord with StorageMessage

/* Exceptions */
sealed trait RemoteException extends StorageMessage
case class ProcessingException(var cause: String, var stacktrace: String) extends RemoteException with AvroRecord
case class RequestRejected(var reason: String, var req: StorageMessage) extends RemoteException with AvroRecord
case class InvalidPartition(var partitionId: String) extends RemoteException with AvroRecord
case class InvalidNamespace(var namespace: String) extends RemoteException with AvroRecord

/* KVStore Operations */
sealed trait KeyValueStoreOperation extends PartitionServiceOperation
case class GetRequest(var key: Array[Byte]) extends AvroRecord with KeyValueStoreOperation
case class GetResponse(var value: Option[Array[Byte]]) extends AvroRecord with KeyValueStoreOperation

case class PutRequest(var key: Array[Byte], var value: Option[Array[Byte]]) extends AvroRecord with KeyValueStoreOperation
case class PutResponse() extends AvroRecord with KeyValueStoreOperation

case class BulkUrlPutReqest(var parser:Array[Byte], var locations:Seq[String]) extends AvroRecord with KeyValueStoreOperation
case class BulkPutRequest(var records: Seq[PutRequest]) extends AvroRecord with KeyValueStoreOperation
case class BulkPutResponse() extends AvroRecord with KeyValueStoreOperation

case class GetRangeRequest(var minKey: Option[Array[Byte]], var maxKey: Option[Array[Byte]], var limit: Option[Int] = None, var offset: Option[Int] = None, var ascending: Boolean = true) extends AvroRecord with KeyValueStoreOperation
case class GetRangeResponse(var records: Seq[Record]) extends AvroRecord with KeyValueStoreOperation

case class CursorScanRequest(var cursorId: Option[Int], var recsPerRequest: Int) extends AvroRecord with KeyValueStoreOperation
case class CursorScanResponse(var cursorId: Option[Int], var records: IndexedSeq[Record]) extends AvroRecord with KeyValueStoreOperation

case class BatchRequest(var ranges : Seq[StorageMessage]) extends AvroRecord with KeyValueStoreOperation
case class BatchResponse(var ranges : Seq[StorageMessage]) extends AvroRecord with KeyValueStoreOperation

case class CountRangeRequest(var minKey: Option[Array[Byte]], var maxKey: Option[Array[Byte]]) extends AvroRecord with KeyValueStoreOperation
case class CountRangeResponse(var count: Int) extends AvroRecord with KeyValueStoreOperation

case class TestSetRequest(var key: Array[Byte], var value: Option[Array[Byte]], var expectedValue: Option[Array[Byte]]) extends AvroRecord with KeyValueStoreOperation
case class TestSetResponse(var success: Boolean) extends AvroRecord with KeyValueStoreOperation

case class GetWorkloadStats() extends AvroRecord with KeyValueStoreOperation
case class GetWorkloadStatsResponse(var getCount:Int, var putCount:Int, var statsSince:Long) extends AvroRecord with KeyValueStoreOperation

case class AggFilter(var obj:Array[Byte]) extends AvroRecord
case class AggOp(var codename:String, var code:Array[Byte], var obj:Array[Byte], var raw:Boolean) extends AvroRecord
case class AggRequest(var groups: Seq[String], var keyType:String, var valueType:String, var filters:Seq[AggFilter], var aggs:Seq[AggOp]) extends AvroRecord with KeyValueStoreOperation
case class GroupedAgg(var group:Option[Array[Byte]], var groupVals:Seq[Array[Byte]]) extends AvroRecord
case class AggReply(var results:Seq[GroupedAgg]) extends AvroRecord with KeyValueStoreOperation

/* Storage Handler Operations */
sealed trait StorageServiceOperation extends StorageMessage
case class CreatePartitionRequest(var namespace: String, var partitionType:String, var startKey: Option[Array[Byte]] = None, var endKey: Option[Array[Byte]] = None) extends AvroRecord with StorageServiceOperation
case class CreatePartitionResponse(var partitionActor: PartitionService) extends AvroRecord with StorageServiceOperation

case class DeletePartitionRequest(var partitionId: String) extends AvroRecord with StorageServiceOperation
case class DeletePartitionResponse() extends AvroRecord with StorageServiceOperation

case class GetPartitionsRequest() extends AvroRecord with StorageServiceOperation
case class GetPartitionsResponse(var partitions: List[PartitionService]) extends AvroRecord with StorageServiceOperation

case class DeleteNamespaceRequest(var namespace: String) extends AvroRecord with StorageServiceOperation
case class DeleteNamespaceResponse() extends AvroRecord with StorageServiceOperation

case class ShutdownStorageHandler() extends AvroRecord with StorageServiceOperation

/* Partition Handler Operations */
sealed trait PartitionServiceOperation extends StorageMessage
case class CopyDataRequest(var src: PartitionService, var overwrite: Boolean) extends AvroRecord with PartitionServiceOperation
case class CopyDataResponse() extends AvroRecord with PartitionServiceOperation

case class GetResponsibilityRequest() extends AvroRecord with PartitionServiceOperation
case class GetResponsibilityResponse(var startKey: Option[Array[Byte]], var endKey: Option[Array[Byte]]) extends AvroRecord with PartitionServiceOperation

/* Test Record Types.  Note: they are here due to problems with the typer (i.e. generated methods aren't visable in the same compilation cycle */
case class IntRec(var f1: Int) extends AvroRecord
case class IntRec2(var f1: Int, var f2: Int) extends AvroRecord
case class IntRec3(var f1: Int, var f2: Int, var f3: Int) extends AvroRecord
case class StringRec(var f1: String) extends AvroRecord
case class StringRec2(var f1: String, var f2: String) extends AvroRecord
case class StringRec3(var f1: String, var f2: String, var f3: String) extends AvroRecord
case class CompIntStringRec(var intRec: IntRec, var stringRec: StringRec) extends AvroRecord
case class PairRec(var username: String) extends AvroPair {
  var password: String = _
  var hometown: String = _
}

case class QuorumProtocolConfig(var readQuorum : Double, var writeQuorum : Double) extends AvroRecord

/* Routing Table Types.  Note: they are here due to problems with the typer (i.e. generated methods aren't visable in the same compilation cycle */

case class KeyRange(var startKey: Option[Array[Byte]], var servers : Seq[PartitionService]) extends AvroRecord
case class RoutingTableMessage(var partitions: Seq[KeyRange]) extends AvroRecord

