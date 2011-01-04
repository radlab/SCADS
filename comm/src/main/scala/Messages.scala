package edu.berkeley.cs.scads.comm

import edu.berkeley.cs.avro.marker.{ AvroRecord, AvroUnion, AvroPair }

/* Base message type for all scads messages */
sealed trait MessageBody extends AvroUnion

/* General message types */
sealed trait ActorId extends AvroUnion
case class ActorNumber(var num: Long) extends AvroRecord with ActorId
case class ActorName(var name: String) extends AvroRecord with ActorId

case class Message(var src: Option[ActorId], var dest: ActorId, var id: Option[Long], var body: MessageBody) extends AvroRecord
case class Record(var key: Array[Byte], var value: Option[Array[Byte]]) extends AvroRecord with MessageBody

/* Exceptions */
sealed trait RemoteException extends MessageBody
case class ProcessingException(var cause: String, var stacktrace: String) extends RemoteException with AvroRecord
case class RequestRejected(var reason: String, var req: MessageBody) extends RemoteException with AvroRecord
case class InvalidPartition(var partitionId: String) extends RemoteException with AvroRecord
case class InvalidNamespace(var namespace: String) extends RemoteException with AvroRecord

/* KVStore Operations */
sealed trait KeyValueStoreOperation extends PartitionServiceOperation
case class GetRequest(var key: Array[Byte]) extends AvroRecord with KeyValueStoreOperation
case class GetResponse(var value: Option[Array[Byte]]) extends AvroRecord with KeyValueStoreOperation

case class PutRequest(var key: Array[Byte], var value: Option[Array[Byte]]) extends AvroRecord with KeyValueStoreOperation
case class PutResponse() extends AvroRecord with KeyValueStoreOperation

case class BulkPutRequest(var records: Seq[PutRequest]) extends AvroRecord with KeyValueStoreOperation
case class BulkPutResponse() extends AvroRecord with KeyValueStoreOperation

case class GetRangeRequest(var minKey: Option[Array[Byte]], var maxKey: Option[Array[Byte]], var limit: Option[Int] = None, var offset: Option[Int] = None, var ascending: Boolean = true) extends AvroRecord with KeyValueStoreOperation
case class GetRangeResponse(var records: Seq[Record]) extends AvroRecord with KeyValueStoreOperation

case class BatchRequest(var ranges : Seq[MessageBody]) extends AvroRecord with KeyValueStoreOperation
case class BatchResponse(var ranges : Seq[MessageBody]) extends AvroRecord with KeyValueStoreOperation

case class CountRangeRequest(var minKey: Option[Array[Byte]], var maxKey: Option[Array[Byte]]) extends AvroRecord with KeyValueStoreOperation
case class CountRangeResponse(var count: Int) extends AvroRecord with KeyValueStoreOperation

case class TestSetRequest(var key: Array[Byte], var value: Option[Array[Byte]], var expectedValue: Option[Array[Byte]]) extends AvroRecord with KeyValueStoreOperation
case class TestSetResponse(var success: Boolean) extends AvroRecord with KeyValueStoreOperation

/* Storage Handler Operations */
sealed trait StorageServiceOperation extends MessageBody
case class CreatePartitionRequest(var namespace: String, var startKey: Option[Array[Byte]] = None, var endKey: Option[Array[Byte]] = None) extends AvroRecord with StorageServiceOperation
case class CreatePartitionResponse(var partitionActor: PartitionService) extends AvroRecord with StorageServiceOperation

case class DeletePartitionRequest(var partitionId: String) extends AvroRecord with StorageServiceOperation
case class DeletePartitionResponse() extends AvroRecord with StorageServiceOperation

case class GetPartitionsRequest() extends AvroRecord with StorageServiceOperation
case class GetPartitionsResponse(var partitions: List[PartitionService]) extends AvroRecord with StorageServiceOperation

case class DeleteNamespaceRequest(var namespace: String) extends AvroRecord with StorageServiceOperation
case class DeleteNamespaceResponse() extends AvroRecord with StorageServiceOperation

case class ShutdownStorageHandler() extends AvroRecord with StorageServiceOperation

/* Partition Handler Operations */
sealed trait PartitionServiceOperation extends MessageBody
case class CopyDataRequest(var src: PartitionService, var overwrite: Boolean) extends AvroRecord with PartitionServiceOperation
case class CopyDataResponse() extends AvroRecord with PartitionServiceOperation

case class GetResponsibilityRequest() extends AvroRecord with PartitionServiceOperation
case class GetResponsibilityResponse(var startKey: Option[Array[Byte]], var endKey: Option[Array[Byte]]) extends AvroRecord with PartitionServiceOperation

/* Messages for the Remote Experiment Running Daemon. Located here due to limitations in MessageHandler */
sealed trait ClassSource extends AvroUnion
case class ServerSideJar(var path: String) extends AvroRecord with ClassSource
case class S3CachedJar(var url: String) extends AvroRecord with ClassSource

sealed trait JvmTask extends AvroUnion
case class JvmWebAppTask(var warFile: ClassSource) extends AvroRecord with JvmTask
case class JvmMainTask(var classpath: Seq[ClassSource], var mainclass: String, var args: Seq[String], var props: Map[String, String] = Map.empty) extends AvroRecord with JvmTask

sealed trait ExperimentOperation extends MessageBody
case class RunExperiment(var processes: List[JvmTask]) extends AvroRecord with ExperimentOperation

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

/* Schema for PIQL modeling */
case class ExecutionTrace(var timestamp: Long, var thread: String, var event: TraceEvent) extends AvroRecord

sealed trait TraceEvent extends AvroUnion
case class QueryEvent(var queryName: String, var start: Boolean) extends AvroRecord with TraceEvent
case class IteratorEvent(var iteratorName: String, var planId: Int, var operation: String, var start: Boolean) extends AvroRecord with TraceEvent
case class MessageEvent(var message: Message) extends AvroRecord with TraceEvent
