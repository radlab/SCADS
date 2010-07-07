package edu.berkeley.cs.scads.comm

import com.googlecode.avro.marker.AvroRecord
import com.googlecode.avro.annotation.AvroUnion

@AvroUnion sealed trait MessageBody
case class Record(var key: Array[Byte], var value: Array[Byte]) extends AvroRecord with MessageBody
case class RecordSet(var records: List[Record]) extends AvroRecord with MessageBody

case class ProcessingException(var cause: String, var stacktrace: String) extends AvroRecord with MessageBody
case class TestAndSetFailure(var key: Array[Byte], var currentValue: Option[Array[Byte]]) extends AvroRecord with MessageBody

case class KeyPartition(var minKey: Option[Array[Byte]], var maxKey: Option[Array[Byte]]) extends AvroRecord with MessageBody
case class PartitionedPolicy(var partitions: List[KeyPartition]) extends AvroRecord with MessageBody

case class KeyRange(var minKey: Option[Array[Byte]], var maxKey: Option[Array[Byte]], var limit: Option[Int], var offset: Option[Int], var backwards: Boolean) extends AvroRecord with MessageBody
case class GetRequest(var namespace: String, var key: Array[Byte]) extends AvroRecord with MessageBody
case class GetRangeRequest(var namespace: String, var range: KeyRange) extends AvroRecord with MessageBody
case class GetPrefixRequest(var namespace: String, var start: Array[Byte], var limit: Option[Int], var ascending: Boolean, var fields: Int) extends AvroRecord with MessageBody
case class CountRangeRequest(var namespace: String, var range: KeyRange) extends AvroRecord with MessageBody
case class CountRangeResponse(var count: Int) extends AvroRecord with MessageBody
case class RemoveRangeRequest(var namespace: String, var range: KeyRange) extends AvroRecord with MessageBody
case class PutRequest(var namespace: String, var key: Array[Byte], var value: Option[Array[Byte]]) extends AvroRecord with MessageBody
case class TestSetRequest(var namespace: String, var key: Array[Byte], var value: Array[Byte], var expectedValue: Option[Array[Byte]], var prefixMatch: Boolean) extends AvroRecord with MessageBody
case class ConfigureRequest(var namespace: String, var partition: String) extends AvroRecord with MessageBody
case class EmptyResponse() extends AvroRecord with MessageBody

//Copy&Sync Code
case class CopyRangesRequest(var namespace: String, var destinationHost: String, var destinationPort: Int, var rateLimit: Int, var ranges: List[KeyRange]) extends AvroRecord with MessageBody
case class CopyStartRequest(var namespace: String, var ranges: List[KeyRange]) extends AvroRecord with MessageBody
case class TransferStartReply(var recvActorId: ActorId) extends AvroRecord with MessageBody
case class TransferFinished(var sendActorId: ActorId) extends AvroRecord with MessageBody
case class SyncRangeRequest(var namespace: String, var method: Int, var destinationHost: String, var destinationPort: Int, var range: KeyRange) extends AvroRecord with MessageBody
case class SyncStartRequest(var namespace: String, var recvIterId: ActorId, var range: KeyRange) extends AvroRecord with MessageBody
case class TransferStarted(var sendActorId: ActorId) extends AvroRecord with MessageBody
case class TransferSucceeded(var sendActorId: ActorId, var recordsSent: Long, var milliseconds: Long) extends AvroRecord with MessageBody
case class TransferFailed(var sendActorId: ActorId, var reason: String) extends AvroRecord with MessageBody
case class BulkData(var seqNum: Int, var sendActorId: ActorId, var records: RecordSet) extends AvroRecord with MessageBody
case class BulkDataAck(var seqNum: Int, var sendActorId: ActorId) extends AvroRecord with MessageBody
case class RecvIterClose(var sendActorId: ActorId) extends AvroRecord with MessageBody
case class FlatMapRequest(var namespace: String, var keyType: String, var valueType: String, var codename: String, var closure: Array[Byte]) extends AvroRecord with MessageBody
case class FlatMapResponse(var records: List[Array[Byte]]) extends AvroRecord with MessageBody
case class FilterRequest(var namespace: String, var keyType: String, var valueType: String, var codename: String, var code: Array[Byte]) extends AvroRecord with MessageBody
case class FoldRequest(var namespace: String, var keyType: String, var valueType: String, var initValueOne: Array[Byte], var initValueTwo: Array[Byte], var direction: Int, var codename: String, var code: Array[Byte]) extends AvroRecord with MessageBody
case class FoldRequest2L(var namespace: String, var keyType: String, var valueType: String, var initType: String, var initValue: Array[Byte], var direction: Int, var codename: String, var code: Array[Byte]) extends AvroRecord with MessageBody
case class Fold2Reply(var reply: Array[Byte]) extends AvroRecord with MessageBody

@AvroUnion sealed trait ActorId
case class ActorNumber(var num: Long) extends AvroRecord with ActorId
case class ActorName(var name: String) extends AvroRecord with ActorId

case class Message(var src: ActorId, var dest: ActorId, var id: Option[Long], var body: MessageBody) extends AvroRecord
