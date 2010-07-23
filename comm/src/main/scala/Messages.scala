package edu.berkeley.cs.scads.comm

import com.googlecode.avro.marker.AvroRecord
import com.googlecode.avro.annotation.AvroUnion

@AvroUnion sealed trait MessageBody
/* General message types */
@AvroUnion sealed trait ActorId
case class ActorNumber(var num: Long) extends AvroRecord with ActorId
case class ActorName(var name: String) extends AvroRecord with ActorId

case class Message(var src: Option[ActorId], var dest: ActorId, var id: Option[Long], var body: MessageBody) extends AvroRecord
case class ProcessingException(var cause: String, var stacktrace: String) extends AvroRecord with MessageBody
case class Record(var key: Array[Byte], var value: Option[Array[Byte]]) extends AvroRecord with MessageBody

/* KVStore Operations */
case class GetRequest(var key: Array[Byte]) extends AvroRecord with MessageBody
case class GetResponse(var value: Option[Array[Byte]]) extends AvroRecord with MessageBody

case class PutRequest(var key: Array[Byte], var value: Option[Array[Byte]]) extends AvroRecord with MessageBody
case class PutResponse() extends AvroRecord with MessageBody

case class GetRangeRequest(var minKey: Option[Array[Byte]], var maxKey: Option[Array[Byte]], var limit: Option[Int] = None, var offset: Option[Int] = None, var ascending: Boolean = true) extends AvroRecord with MessageBody
case class GetRangeResponse(var records: List[Record]) extends AvroRecord with MessageBody

case class CountRangeRequest(var startKey: Option[Array[Byte]], var endKey: Option[Array[Byte]]) extends AvroRecord with MessageBody
case class CountRangeResponse(var count: Int) extends AvroRecord with MessageBody

case class GetPrefixRequest(var prefix: Array[Byte], var prefixSize: Int, var limit: Option[Int] = None, var offset: Option[Int] = None, var ascending: Boolean = true) extends AvroRecord with MessageBody
case class GetPrefixResponse(var records: List[Record]) extends AvroRecord with MessageBody

case class TestSetRequest(var key: Array[Byte], var value: Option[Array[Byte]], var expectedValue: Option[Array[Byte]]) extends AvroRecord with MessageBody
case class TestSetResponse(var success: Boolean) extends AvroRecord with MessageBody

/* Storage Handler Operations */
case class CreatePartitionRequest(var namespace: String, var paritionId: String, var startKey: Option[Array[Byte]], var endKey: Option[Array[Byte]]) extends AvroRecord with MessageBody
case class CreatePartitionResponse(var partitionActor: PartitionService) extends AvroRecord with MessageBody

case class SplitPartitionRequest(var partitionId: String, var splitPoint: Array[Byte]) extends AvroRecord with MessageBody
case class SplitPartitionResponse(var newPartitionService: PartitionService) extends AvroRecord with MessageBody

case class MergePartitionRequest(var paritionId1: String, var partitionId2: String) extends AvroRecord with MessageBody
case class MergePartitionResponse(var mergedPartitionService: PartitionService) extends AvroRecord with MessageBody

case class DeletePartition(var partitionId: String) extends AvroRecord with MessageBody

/* Partition Handler Operations */
case class CopyData(var dest: PartitionService, var overwrite: Boolean) extends AvroRecord with MessageBody

case class GetResponsibilityRequest() extends AvroRecord with MessageBody
case class GetResponsibilityResponse(var startKey: Option[Array[Byte]], var endKey: Option[Array[Byte]]) extends AvroRecord with MessageBody
