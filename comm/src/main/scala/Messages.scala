package edu.berkeley.cs
package scads
package comm

import avro.marker.{ AvroRecord, AvroUnion, AvroPair }
import java.nio.ByteBuffer

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

case class BulkUrlPutReqest(var parser:Array[Byte], var locations:Seq[String]) extends AvroRecord with KeyValueStoreOperation
case class BulkPutRequest(var records: Seq[PutRequest]) extends AvroRecord with KeyValueStoreOperation
case class BulkPutResponse() extends AvroRecord with KeyValueStoreOperation

case class GetRangeRequest(var minKey: Option[Array[Byte]], var maxKey: Option[Array[Byte]], var limit: Option[Int] = None, var offset: Option[Int] = None, var ascending: Boolean = true) extends AvroRecord with KeyValueStoreOperation
case class GetRangeResponse(var records: Seq[Record]) extends AvroRecord with KeyValueStoreOperation

case class CursorScanRequest(var cursorId: Option[Int], var recsPerRequest: Int) extends AvroRecord with KeyValueStoreOperation
case class CursorScanResponse(var cursorId: Option[Int], var records: IndexedSeq[Record]) extends AvroRecord with KeyValueStoreOperation

case class BatchRequest(var ranges : Seq[MessageBody]) extends AvroRecord with KeyValueStoreOperation
case class BatchResponse(var ranges : Seq[MessageBody]) extends AvroRecord with KeyValueStoreOperation

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

/* Transaction KVStore Metadata */
case class MDCCBallot(var round: Long, var vote: Int, var server: StorageService, var fast: Boolean) extends AvroRecord
case class MDCCBallotRange(var startRound: Long, var endRound: Option[Long], var vote: Int, var server: StorageService, var fast: Boolean) extends AvroRecord
case class MDCCMetadata(var currentRound: Long, var ballots: Seq[MDCCBallotRange]) extends AvroRecord
case class CStructCommand(var xid: ScadsXid, var command: RecordUpdate, var pending: Boolean) extends AvroRecord
// TODO: Does 'value' need to be an MDCCRecord?
case class CStruct(var value: Option[Array[Byte]], var commands: Seq[CStructCommand]) extends AvroRecord
// The value is the serialized version of the namespace's value type.
case class MDCCRecord(var value: Option[Array[Byte]], var metadata: MDCCMetadata) extends AvroRecord with MessageBody

/* Transaction KVStore Updates */
// key is the avro serialized key type
sealed trait RecordUpdate extends AvroUnion {
  var key: Array[Byte]
}
case class LogicalUpdate(var key: Array[Byte], var schema: String, var delta: Array[Byte]) extends AvroRecord with RecordUpdate
// The newValue is the avro serialized TxRecord type
sealed trait PhysicalUpdate extends RecordUpdate {
  var key: Array[Byte]
  var newValue: Array[Byte]
}
// The new metadata (including the record version) is embedded in newValue
// Update will check that the new version is the next version after the
// stored version.
case class VersionUpdate(var key: Array[Byte], var newValue: Array[Byte]) extends AvroRecord with PhysicalUpdate
// Update will check that the oldValue matches the stored value.  If oldValue
// is None, it means there was no record, and the update is an insert.
case class ValueUpdate(var key: Array[Byte], var oldValue: Option[Array[Byte]], var newValue: Array[Byte]) extends AvroRecord with PhysicalUpdate

/* Transaction MDCC Paxos */
case class ScadsXid(var tid: Long, var bid: Long) extends AvroRecord {
  def serialized(): Array[Byte] = {
    // Long is 8 bytes
    ByteBuffer.allocate(16).putLong(tid).putLong(bid).array
  }
}

case class Transaction(var xid: ScadsXid, var updates: Seq[RecordUpdate])  extends AvroRecord

/* Transaction MDCC Paxos */
sealed trait MDCCProtocol extends KeyValueStoreOperation

case class Propose(var trx: Transaction) extends AvroRecord with MDCCProtocol

case class Phase1a(var key: Array[Byte], var ballot: MDCCBallotRange) extends AvroRecord with MDCCProtocol

case class Phase1b(var ballot: MDCCBallot, var value: CStruct) extends AvroRecord with MDCCProtocol

case class Phase2a(var key: Array[Byte], var ballot: MDCCBallot, var value: CStruct) extends AvroRecord with MDCCProtocol

//case class Phase2aClassic(var key: Array[Byte], var ballot: MDCCBallot, var command: RecordUpdate) extends AvroRecord with MDCCProtocol

case class Phase2bClassic(var ballot: MDCCBallot, var value: CStruct) extends AvroRecord with MDCCProtocol

case class Phase2bFast(var ballot: MDCCBallot, var value: CStruct) extends AvroRecord with MDCCProtocol

/* Transaction KVStore Operations */
sealed trait TxProtocol2pc extends KeyValueStoreOperation
case class PrepareRequest(var xid: ScadsXid, var updates: Seq[RecordUpdate]) extends AvroRecord with TxProtocol2pc
//case class PrepareRequest(var xid: ScadsXid, var updates: Seq[PutRequest]) extends AvroRecord with TxProtocol2pc
case class PrepareResponse(var success: Boolean) extends AvroRecord with TxProtocol2pc
case class CommitRequest(var xid: ScadsXid, var updates: Seq[RecordUpdate], var commit: Boolean) extends AvroRecord with TxProtocol2pc
//case class CommitRequest(var xid: ScadsXid, var commit: Boolean) extends AvroRecord with TxProtocol2pc
case class CommitResponse(var success: Boolean) extends AvroRecord with TxProtocol2pc

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
case class JvmWebAppTask(var warFile: ClassSource, var properties: Map[String, String]) extends AvroRecord with JvmTask
case class JvmMainTask(var classpath: Seq[ClassSource], var mainclass: String, var args: Seq[String], var props: Map[String, String] = Map.empty, var env: Map[String, String] = Map.empty) extends AvroRecord with JvmTask

sealed trait ExperimentOperation extends MessageBody
case class RunExperimentRequest(var processes: Seq[JvmTask]) extends AvroRecord with ExperimentOperation
case class RunExperimentResponse() extends AvroRecord with ExperimentOperation

case class KillTaskRequest(var taskId: Int) extends AvroRecord with ExperimentOperation
case class KillTaskResponse() extends AvroRecord with ExperimentOperation

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

