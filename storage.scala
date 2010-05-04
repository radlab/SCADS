package edu.berkeley.cs.scads.comm 

import com.googlecode.avro.annotation.{AvroRecord,AvroUnion}

import java.nio.ByteBuffer

@AvroUnion
sealed trait MessageType

@AvroRecord
case class Record(var key: Array[Byte], var value: Array[Byte]) extends MessageType

@AvroRecord
case class RecordSet(var records: List[Record]) extends MessageType

@AvroRecord
case class ProcessingException(var cause: String, var stacktrace: String) extends MessageType

@AvroRecord
case class TestAndSetFailure(var key: Array[Byte], var currentValue: Array[Byte]) extends MessageType

@AvroRecord
case class KeyPartition(var minKey: Array[Byte], var maxKey: Array[Byte])

@AvroRecord
case class PartitionedPolicy(var partitions: List[KeyPartition])

@AvroRecord
case class KeyRange(
        var minKey: Array[Byte],
        var maxKey: Array[Byte],
        var limit: Int,
        var offset: Int,
        var backwards: Boolean)

@AvroRecord
case class GetRequest(var namespace: String, var key: Array[Byte]) extends MessageType

@AvroRecord
case class GetRangeRequest(var namespace: String, var range: KeyRange) extends MessageType

@AvroRecord
case class CountRangeRequest(var namespace: String, var range: KeyRange) extends MessageType

@AvroRecord
case class RemoveRangeRequest(var namespace: String, var range: KeyRange) extends MessageType

@AvroRecord
case class PutRequest(var namespace: String, var key: Array[Byte], var value: Array[Byte]) extends MessageType

@AvroRecord
case class TestSetRequest(
        var namespace: String,
        var key: Array[Byte],
        var value: Array[Byte],
        var expectedValue: Array[Byte],
        var prefixMatch: Boolean) extends MessageType

@AvroRecord
case class ConfigureRequest(var namespace: String, var partition: String) extends MessageType

@AvroRecord
case class CopyRangeRequest(var namespace: String,
                            var destinationHost: String,
                            var destinationPort: Int,
                            var rateLimit: Int,
                            var ranges: List[KeyRange]) extends MessageType

@AvroRecord
case class TransferStartReply(var recvActorId: Long) extends MessageType

@AvroRecord
case class TransferFinished(var sendActorId: Long) extends MessageType

@AvroRecord
case class SyncRangeRequest(var namespace: String,
                            var method: Int, /** Was enum in previous of SIMPLE, MERKLE */
                            var destinationHost: String,
                            var destinationPort: Int,
                            var range: KeyRange) extends MessageType

@AvroRecord
case class SyncStartRequest(var namespace: String, var recvIterId: Long, var range: KeyRange) extends MessageType

@AvroRecord
case class TransferStarted(var sendActorId: Long) extends MessageType

@AvroRecord
case class TransferSucceeded(var sendActorId: Long, var recordsSent: Long, var milliseconds: Long) extends MessageType

@AvroRecord
case class TransferFailed(var sendActorId: Long, var reason: String) extends MessageType

@AvroRecord
case class MerkleSyncPrepare(var range: KeyRange, var recvActorId: Long, var sinkActorId: Long) extends MessageType

@AvroRecord
case class MerkleSyncReady(var srcActorId: Long) extends MessageType

@AvroRecord
case class MerkleRequest(var id: Int, 
                         var idsrc: Int, 
                         var children: List[MerkleRequestChild]) extends MessageType /** children was originally a nested record */

@AvroRecord
case class MerkleRequestChild(var edge: Int, var digest: Long) extends MessageType /** Nested record promoted to top level */

@AvroRecord
case class MerkleResponse(var id: Int, var mismatch: List[Int]) extends MessageType

@AvroRecord
case class MerkleSyncFinish

@AvroRecord
case class FloretEstimator(var buckets: List[Int]) extends MessageType

@AvroRecord
case class BulkData(var seqNum: Int, var sendActorId: Long, var records: RecordSet) extends MessageType

@AvroRecord
case class BulkDataAck(var seqNum: Int, var sendActorId: Long) extends MessageType

@AvroRecord
case class RecvIterClose(var sendActorId: Long) extends MessageType

@AvroRecord
case class FlatMapRequest(var namespace: String,
                          var keyType: String,
                          var valueType: String,
                          var codename: String,
                          var closure: Array[Byte]) extends MessageType

@AvroRecord
case class FlatMapResponse(var records: List[ByteBuffer]) extends MessageType

@AvroRecord
case class FilterRequest(var namespace: String,
                         var keyType: String,
                         var valueType: String,
                         var codename: String,
                         var code: Array[Byte]) extends MessageType

@AvroRecord
case class FoldRequest(var namespace: String,
                       var keyType: String,
                       var valueType: String,
                       var initValueOne: Array[Byte],
                       var initValueTwo: Array[Byte],
                       var direction: Int,
                       var codename: String,
                       var code: Array[Byte]) extends MessageType

@AvroRecord
case class FoldRequest2L(var namespace: String,
                         var keyType: String,
                         var valueType: String,
                         var initType: String,
                         var initValue: Array[Byte],
                         var direction: Int,
                         var codename: String,
                         var code: Array[Byte]) extends MessageType

@AvroRecord
case class Fold2Reply(var reply: Array[Byte]) extends MessageType

@AvroRecord
case class MessageNode(var idLong: Long, var idString: String)

@AvroRecord
case class Message(var src: MessageNode, /** Was originally long, string, null */
                   var dest: MessageNode, /** Was originally long, string */
                   var id: Long,
                   var body: MessageType)
