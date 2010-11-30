package edu.berkeley.cs.scads.storage.newclient

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema
import org.apache.avro.generic._

abstract class GenericNamespace(
    val name: String,
    val root: ZooKeeperProxy#ZooKeeperNode,
    val keySchema: Schema,
    val valueSchema: Schema)
  extends Namespace 
  with RangeKeyValueStore[GenericRecord, GenericRecord]
  with AvroKeyValueSerializer[GenericRecord, GenericRecord]
  with QuorumRangeProtocol
  with DefaultKeyRangeRoutable
  with ZooKeeperGlobalMetadata
  with SimpleRecordMetadata

abstract class GenericHashNamespace(
    val name: String,
    val root: ZooKeeperProxy#ZooKeeperNode,
    val keySchema: Schema,
    val valueSchema: Schema)
  extends Namespace 
  with KeyValueStore[GenericRecord, GenericRecord]
  with AvroKeyValueSerializer[GenericRecord, GenericRecord]
  with QuorumProtocol
  with DefaultKeyRoutable
  with ZooKeeperGlobalMetadata
  with SimpleRecordMetadata
