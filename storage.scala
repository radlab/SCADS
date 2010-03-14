package edu.berkeley.cs.scads.comm 

import com.googlecode.avro.annotation.{AvroRecord,AvroUnion}

@AvroUnion
sealed trait StorageRequestUnion

@AvroUnion
sealed trait StorageResponseUnion

@AvroRecord
case class Record(var key: Array[Byte], var value: Array[Byte]) extends StorageResponseUnion

@AvroRecord
case class RecordSet(var records: List[Record]) extends StorageResponseUnion

@AvroRecord
case class ProcessingException(var cause: String, var stacktrace: String) extends StorageResponseUnion

@AvroRecord
case class TestAndSetFailure(var key: Array[Byte], var currentValue: Array[Byte]) extends StorageResponseUnion

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
case class GetRequest(var namespace: String, var key: Array[Byte]) extends StorageRequestUnion

@AvroRecord
case class GetRangeRequest(var namespace: String, var range: KeyRange) extends StorageRequestUnion

@AvroRecord
case class CountRangeRequest(var namespace: String, var range: KeyRange) extends StorageRequestUnion

@AvroRecord
case class RemoveRangeRequest(var namespace: String, var range: KeyRange) extends StorageRequestUnion

@AvroRecord
case class PutRequest(var namespace: String, var key: Array[Byte], var value: Array[Byte]) extends StorageRequestUnion

@AvroRecord
case class TestSetRequest(
        var namespace: String,
        var key: Array[Byte],
        var value: Array[Byte],
        var expectedValue: Array[Byte],
        var prefixMatch: Boolean) extends StorageRequestUnion

@AvroRecord
case class ConfigureRequest(var namespace: String, var partition: String) extends StorageRequestUnion


@AvroRecord
case class StorageRequest(var src: Long, var body: StorageRequestUnion)

/**
 * The StorageResponseUnion for this class cannot encapsulate the "int"
 * possibility of the union.
 */
@AvroRecord
case class StorageResponse(var dest: Long, var body: StorageResponseUnion)
