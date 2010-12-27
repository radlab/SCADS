package edu.berkeley.cs.scads.storage.newclient

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import org.apache.avro._
import generic._
import specific._

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

abstract class SpecificNamespace[Key <: SpecificRecord : Manifest, Value <: SpecificRecord : Manifest](
    val root: ZooKeeperProxy#ZooKeeperNode)
  extends Namespace 
  with RangeKeyValueStore[Key, Value]
  with AvroSpecificKeyValueSerializer[Key, Value]
  with QuorumRangeProtocol
  with DefaultKeyRangeRoutable
  with ZooKeeperGlobalMetadata
  with SimpleRecordMetadata {

  override protected val keyManifest = manifest[Key]
  override protected val valueManifest = manifest[Value] 

}

abstract class PairNamespace[Pair <: AvroPair : Manifest](
    val root: ZooKeeperProxy#ZooKeeperNode)
  extends Namespace 
  with RecordStore[Pair]
  with AvroPairSerializer[Pair]
  with QuorumRangeProtocol
  with DefaultKeyRangeRoutable
  with ZooKeeperGlobalMetadata
  with SimpleRecordMetadata {
  
  override protected val pairManifest = manifest[Pair]

}
