package edu.berkeley.cs.scads.storage.newclient

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.comm._

import org.apache.avro.Schema 
import org.apache.avro.generic._

trait ZooKeeperGlobalMetadata extends GlobalMetadata with Namespace with KeyRoutable {
  val root: ZooKeeperProxy#ZooKeeperNode
  def name: String = root.name
  override def remoteKeySchema: Schema = error("remoteKeySchema")
  override def remoteValueSchema: Schema = error("remoteValueSchema")
}

trait SimpleRecordMetadata extends RecordMetadata {
  override def compareKey(x: Array[Byte], y: Array[Byte]): Int = error("compareKey")
  override def hashKey(x: Array[Byte]): Int = error("hashKey")

  override def createMetadata(rec: Array[Byte]) = error("createMetadata")
  override def compareMetadata(lhs: Array[Byte], rhs: Array[Byte]): Int = error("compareMetadata")
  override def extractMetadataFromValue(value: Array[Byte]): (Array[Byte], Array[Byte]) = error("extractMetadataFromValue")
}
