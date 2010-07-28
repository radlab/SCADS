package edu.berkeley.cs.scads.comm

import com.googlecode.avro.marker.AvroRecord
import com.googlecode.avro.annotation.AvroUnion

/* Base message type for all scads messages */
@AvroUnion sealed trait MessageBody

/* General message types */
@AvroUnion sealed trait ActorId
case class ActorNumber(var num: Long) extends AvroRecord with ActorId
case class ActorName(var name: String) extends AvroRecord with ActorId

case class Message(var src: Option[ActorId], var dest: ActorId, var id: Option[Long], var body: MessageBody) extends AvroRecord
case class Record(var key: Array[Byte], var value: Option[Array[Byte]]) extends AvroRecord with MessageBody

/* Exceptions */
@AvroUnion sealed trait RemoteException extends MessageBody
case class ProcessingException(var cause: String, var stacktrace: String) extends RemoteException with AvroRecord
case class RequestRejected(var reason: String, var req: MessageBody) extends RemoteException with AvroRecord
case class InvalidPartition(var partitionId: String) extends RemoteException with AvroRecord

/* KVStore Operations */
@AvroUnion sealed trait KeyValueStoreOperation extends PartitionServiceOperation
case class GetRequest(var key: Array[Byte]) extends AvroRecord with KeyValueStoreOperation
case class GetResponse(var value: Option[Array[Byte]]) extends AvroRecord with KeyValueStoreOperation

case class PutRequest(var key: Array[Byte], var value: Option[Array[Byte]]) extends AvroRecord with KeyValueStoreOperation
case class PutResponse() extends AvroRecord with KeyValueStoreOperation

case class GetRangeRequest(var minKey: Option[Array[Byte]], var maxKey: Option[Array[Byte]], var limit: Option[Int] = None, var offset: Option[Int] = None, var ascending: Boolean = true) extends AvroRecord with KeyValueStoreOperation
case class GetRangeResponse(var records: List[Record]) extends AvroRecord with KeyValueStoreOperation

case class CountRangeRequest(var minKey: Option[Array[Byte]], var maxKey: Option[Array[Byte]]) extends AvroRecord with KeyValueStoreOperation
case class CountRangeResponse(var count: Int) extends AvroRecord with KeyValueStoreOperation

case class GetPrefixRequest(var prefix: Array[Byte], var prefixSize: Int, var limit: Option[Int] = None, var offset: Option[Int] = None, var ascending: Boolean = true) extends AvroRecord with KeyValueStoreOperation
case class GetPrefixResponse(var records: List[Record]) extends AvroRecord with KeyValueStoreOperation

case class TestSetRequest(var key: Array[Byte], var value: Option[Array[Byte]], var expectedValue: Option[Array[Byte]]) extends AvroRecord with KeyValueStoreOperation
case class TestSetResponse(var success: Boolean) extends AvroRecord with KeyValueStoreOperation

/* Storage Handler Operations */
@AvroUnion sealed trait StorageServiceOperation extends MessageBody
case class CreatePartitionRequest(var namespace: String, var paritionId: String, var startKey: Option[Array[Byte]] = None, var endKey: Option[Array[Byte]] = None) extends AvroRecord with StorageServiceOperation
case class CreatePartitionResponse(var partitionActor: PartitionService) extends AvroRecord with StorageServiceOperation

case class SplitPartitionRequest(var partitionId: String, var newPartitionId: String, var splitPoint: Array[Byte]) extends AvroRecord with StorageServiceOperation
case class SplitPartitionResponse(var partition1: PartitionService, var partition2: PartitionService) extends AvroRecord with StorageServiceOperation

case class MergePartitionRequest(var paritionId1: String, var partitionId2: String) extends AvroRecord with StorageServiceOperation
case class MergePartitionResponse(var mergedPartitionService: PartitionService) extends AvroRecord with StorageServiceOperation

case class DeletePartitionRequest(var partitionId: String) extends AvroRecord with StorageServiceOperation
case class DeletePartitionResponse() extends AvroRecord with StorageServiceOperation

/* Partition Handler Operations */
@AvroUnion sealed trait PartitionServiceOperation extends MessageBody
case class CopyDataRequest(var src: PartitionService, var overwrite: Boolean) extends AvroRecord with PartitionServiceOperation
case class CopyDataResponse() extends AvroRecord with PartitionServiceOperation

case class GetResponsibilityRequest() extends AvroRecord with PartitionServiceOperation
case class GetResponsibilityResponse(var startKey: Option[Array[Byte]], var endKey: Option[Array[Byte]]) extends AvroRecord with PartitionServiceOperation


/* Test Record Types.  Note: they are here due to problems with the typer (i.e. generated methods aren't visable in the same compilation cycle */
case class IntRec(var f1: Int) extends AvroRecord
case class StringRec(var f1: String) extends AvroRecord
