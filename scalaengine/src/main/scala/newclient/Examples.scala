package edu.berkeley.cs.scads.storage.newclient

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema
import org.apache.avro.generic._

abstract class GenericNamespace(
    val root: ZooKeeperProxy#ZooKeeperNode,
    val keySchema: Schema,
    val valueSchema: Schema)
  extends Namespace 
  with RangeKeyValueStore[GenericRecord, GenericRecord]
  with AvroGenericKeyValueSerializer
  with QuorumRangeProtocol
  with DefaultKeyRangeRoutable
  with ZooKeeperGlobalMetadata
  with SimpleRecordMetadata

abstract class GenericHashNamespace(
    val root: ZooKeeperProxy#ZooKeeperNode,
    val keySchema: Schema,
    val valueSchema: Schema)
  extends Namespace 
  with KeyValueStore[GenericRecord, GenericRecord]
  with AvroGenericKeyValueSerializer
  with QuorumProtocol
  with DefaultHashKeyRoutable
  with ZooKeeperGlobalMetadata
  with SimpleRecordMetadata
