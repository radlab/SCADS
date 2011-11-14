package edu.berkeley.cs
package scads
package storage

import avro.marker.{AvroPair, AvroRecord, AvroUnion}
import comm._
import java.nio.ByteBuffer
import java.util.UUID
import edu.berkeley.cs.scads.storage.transactions.{MDCCBallot, MDCCBallotRange, MDCCMetadata}

object StorageEnvelope{
  def unapply(e : Envelope[StorageMessage]) : Option[(RemoteServiceProxy[StorageMessage], StorageMessage)] = e match {
    case Envelope(src, msg) => Some((src.get.asInstanceOf[RemoteServiceProxy[StorageMessage]], msg.asInstanceOf[StorageMessage]) )
    case _ => None
  }
}

sealed trait StorageMessage extends AvroUnion

sealed trait SCADSService extends AvroUnion with RemoteServiceProxy[StorageMessage] {
  registry = StorageRegistry
}

object StorageService {
  def apply(s: RemoteServiceProxy[StorageMessage]):StorageService =
    StorageService(s.remoteNode, s.id)
}

/* Specific types for different services. Note: these types are mostly for readability as typesafety isn't enforced when serialized individualy*/
case class StorageService(var remoteNode: RemoteNode,
                          var id: ServiceId) extends AvroRecord with SCADSService {
  registry = StorageRegistry
}

object PartitionService {
  def apply(s: RemoteServiceProxy[StorageMessage], partitionId: String, storageService: StorageService): PartitionService =
    PartitionService(s.remoteNode, s.id, partitionId, storageService)
}

case class PartitionService(var remoteNode: RemoteNode,
                            var id: ServiceId,
                            var partitionId: String,
                            var storageService: StorageService) extends AvroRecord with SCADSService

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
case class CreatePartitionRequest(var namespace: String, var partitionType:String, var startKey: Option[Array[Byte]] = None, var endKey: Option[Array[Byte]] = None, var trxProtocol : String = "") extends AvroRecord with StorageServiceOperation
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


// Pending means the command is not decided yet.
case class CStructCommand(var xid: ScadsXid, var command: RecordUpdate, var pending: Boolean, var commit: Boolean) extends AvroRecord {
  override def toString = "(" + xid + ":" + command + " p:" + pending + " c:" + commit + ")"
}

// TODO: Does 'value' need to be an MDCCRecord?
case class CStruct(var value: Option[Array[Byte]], var commands: Seq[CStructCommand]) extends AvroRecord {
  override def toString = "CStruct[" + value + "->" + commands.mkString(",") + "]"
}
// TODO: For now, split out the cstructs from the actual record.
// The value is the serialized version of the namespace's value type.
case class MDCCRecord(var value: Option[Array[Byte]], var metadata: MDCCMetadata) extends AvroRecord with StorageMessage

/* Transaction KVStore Updates */
// key is the avro serialized key type
sealed trait RecordUpdate extends AvroUnion {
  var key: Array[Byte]
}

case class LogicalUpdate(var key: Array[Byte], var delta: Array[Byte]) extends AvroRecord with RecordUpdate
// The newValue is the avro serialized MDCCRecord type
sealed trait PhysicalUpdate extends RecordUpdate {
  var key: Array[Byte]
  var newValue: Array[Byte]
}
// The new metadata (including the record version) is embedded in newValue
// Update will check that the new version is the next version after the
// stored version.
case class VersionUpdate(var key: Array[Byte], var newValue: Array[Byte]) extends AvroRecord with PhysicalUpdate
// Update will check that the oldValue matches the stored value.  If oldValue
// is None, it means either there was no record, and the update is an insert,
// or it is a blind write.
case class ValueUpdate(var key: Array[Byte], var oldValue: Option[Array[Byte]], var newValue: Array[Byte]) extends AvroRecord with PhysicalUpdate

// Simple integrity constraints
// TODO: Could not get hierarchical traits to work.
//       Also, why did making changes to this file become so unstable?  I am
//       forced to clean for every change (even just a comment change).
sealed trait FieldRestriction extends AvroUnion
case class FieldRestrictionGT(var value: Double)
     extends AvroRecord with FieldRestriction
case class FieldRestrictionGE(var value: Double)
     extends AvroRecord with FieldRestriction
case class FieldRestrictionLT(var value: Double)
     extends AvroRecord with FieldRestriction
case class FieldRestrictionLE(var value: Double)
     extends AvroRecord with FieldRestriction
case class FieldIC(var fieldPos: Int, var lower: Option[FieldRestriction],
                   var upper: Option[FieldRestriction]) extends AvroRecord
case class FieldICList(var ics: Seq[FieldIC]) extends AvroRecord

/* Transaction MDCC Paxos */
case class ScadsXid(var tid: Long, var bid: Long) extends AvroRecord {
  def serialized(): Array[Byte] = {
    // Long is 8 bytes
    ByteBuffer.allocate(16).putLong(tid).putLong(bid).array
  }
  override def toString = "TID:"  + tid % 100 + ":" + bid % 100
}

object ScadsXid {
  var count = 0
  def createUniqueXid() : ScadsXid = {
    val uuid = UUID.randomUUID()
    new ScadsXid(uuid.getMostSignificantBits, uuid.getLeastSignificantBits)
  }
}

case class Transaction(var xid: ScadsXid, var updates: Seq[RecordUpdate])  extends AvroRecord

sealed trait TrxMessage extends KeyValueStoreOperation

/* Transaction MDCC Paxos */
sealed trait MDCCProtocol extends TrxMessage

sealed trait Propose extends MDCCProtocol

case class SinglePropose(var xid: ScadsXid, var update: RecordUpdate) extends AvroRecord with Propose

case class MultiPropose(var proposes : Seq[SinglePropose]) extends AvroRecord with Propose

/**
 * Request a conflict resolution
 * Proposes are the critical updates
 * Init indicates if only a fast round has to be opened up
 */
case class ResolveConflict(var key: Array[Byte], var ballots: Seq[MDCCBallotRange], var propose : Propose, var requester : SCADSService) extends AvroRecord with MDCCProtocol

case class Recovered(var key: Array[Byte], var value: CStruct, var meta: MDCCMetadata) extends AvroRecord with MDCCProtocol

case class BeMaster(var key: Array[Byte], var startRound: Long, var endRound: Long, var fast : Boolean) extends AvroRecord with MDCCProtocol

case class GotMastership(var ballots: Seq[MDCCBallotRange]) extends AvroRecord with MDCCProtocol


case class Phase1a(var key: Array[Byte], var ballots: Seq[MDCCBallotRange]) extends AvroRecord with MDCCProtocol

case class Phase1b(var meta: MDCCMetadata, var value: CStruct) extends AvroRecord with MDCCProtocol

//TODO we could make the updates part of te CStruct, if we would have the chance to execute accepts locally
case class Phase2a(var key: Array[Byte], var ballot: MDCCBallot, var safeValue: CStruct, var newUpdates : Seq[SinglePropose]) extends AvroRecord with MDCCProtocol

object Phase2a{
  def apply(key: Array[Byte], ballot: MDCCBallot, safeValue: CStruct) : Phase2a = new Phase2a(key, ballot, safeValue, Nil )
}

case class Phase2b(var ballot : MDCCBallot, var value: CStruct) extends AvroRecord with MDCCProtocol

//Used to indicate that the ballot was not valid
case class Phase2bMasterFailure(var ballots: Seq[MDCCBallotRange], var confirmed : Boolean)  extends AvroRecord with MDCCProtocol

case class Learned(var xid: ScadsXid, var key: Array[Byte], var status : Boolean)  extends AvroRecord with MDCCProtocol

case class Commit(var xid: ScadsXid) extends AvroRecord with MDCCProtocol
case class Abort(var xid: ScadsXid) extends AvroRecord with MDCCProtocol
case class Exit() extends AvroRecord with MDCCProtocol

/* Transaction KVStore Operations */
sealed trait TxProtocol2pc extends TrxMessage
case class PrepareRequest(var xid: ScadsXid, var updates: Seq[RecordUpdate]) extends AvroRecord with TxProtocol2pc
//case class PrepareRequest(var xid: ScadsXid, var updates: Seq[PutRequest]) extends AvroRecord with TxProtocol2pc
case class PrepareResponse(var success: Boolean) extends AvroRecord with TxProtocol2pc
case class CommitRequest(var xid: ScadsXid, var updates: Seq[RecordUpdate], var commit: Boolean) extends AvroRecord with TxProtocol2pc
//case class CommitRequest(var xid: ScadsXid, var commit: Boolean) extends AvroRecord with TxProtocol2pc
case class CommitResponse(var success: Boolean) extends AvroRecord with TxProtocol2pc
