package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.routing._

import scala.actors._
import scala.actors.Actor._
import java.util.Arrays

import scala.collection.mutable.HashMap
import scala.concurrent.SyncVar
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.io.AbstractFile

import org.apache.log4j.Logger

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.util.Utf8
import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{BinaryData, DecoderFactory, BinaryEncoder, BinaryDecoder}
import org.apache.avro.io.DecoderFactory
import com.googlecode.avro.runtime.AvroScala._
import com.googlecode.avro.runtime.ScalaSpecificRecord

import org.apache.zookeeper.CreateMode

/**
 * Implementation of Scads Namespace that returns ScalaSpecificRecords
 */
class SpecificNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord](namespace:String, timeout:Int, root: ZooKeeperProxy#ZooKeeperNode)(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]) extends Namespace[KeyType, ValueType](namespace, timeout, root) with RepartitioningProtocol[KeyType] {
  protected val keyClass = keyType.erasure.asInstanceOf[Class[ScalaSpecificRecord]]
  protected val valueClass = valueType.erasure.asInstanceOf[Class[ScalaSpecificRecord]]

  protected def serializeKey(key: KeyType): Array[Byte] = key.toBytes
  protected def serializeValue(value: ValueType): Array[Byte] = value.toBytes

  protected def deserializeKey(key: Array[Byte]): KeyType = {
    val ret = keyClass.newInstance.asInstanceOf[KeyType]
    ret.parse(key)
    ret
  }

  protected def deserializeValue(value: Array[Byte]): ValueType = {
    val ret = valueClass.newInstance.asInstanceOf[ValueType]
    ret.parse(value)
    ret
  }
}

class GenericNamespace(namespace:String, timeout:Int, root: ZooKeeperProxy#ZooKeeperNode) extends Namespace[GenericData.Record, GenericData.Record](namespace, timeout, root) with RepartitioningProtocol[GenericData.Record] {
  val decoderFactory = DecoderFactory.defaultFactory()
  val keyReader = new GenericDatumReader[GenericData.Record](keySchema)
  val valueReader = new GenericDatumReader[GenericData.Record](valueSchema)
  val keyWriter = new GenericDatumWriter[GenericData.Record](keySchema)
  val valueWriter = new GenericDatumWriter[GenericData.Record](valueSchema)

  protected def serializeKey(key: GenericData.Record): Array[Byte] = key.toBytes
  protected def serializeValue(value: GenericData.Record): Array[Byte] = value.toBytes

  protected def deserializeKey(key: Array[Byte]): GenericData.Record = {
    val decoder = decoderFactory.createBinaryDecoder(key, null)
    keyReader.read(null, decoder)
  }

  protected def deserializeValue(value: Array[Byte]): GenericData.Record = {
    val decoder = decoderFactory.createBinaryDecoder(value, null)
    valueReader.read(null, decoder)
  }
}

/**
 * KVStore Trait + Quorum Protocol
 * TODO: Add functions for splitting/merging partitions (protocol for moving data safely)
 * TODO: Handle the need for possible schema resolutions
 */
abstract class Namespace[KeyType <: IndexedRecord, ValueType <: IndexedRecord](val namespace:String, val timeout:Int, val root: ZooKeeperProxy#ZooKeeperNode) extends KeyValueStore[KeyType, ValueType] {
  protected val logger = Logger.getLogger("Namespace")
  protected val nsNode = root("namespaces/"+namespace)
  protected val keySchema = Schema.parse(new String(nsNode("keySchema").data))
  protected val valueSchema = Schema.parse(new String(nsNode("valueSchema").data))

  protected def getKeySchema() : Schema = throw new RuntimeException("Unimplemented")
  /* DeSerialization Methods */
  protected def serializeKey(key: KeyType): Array[Byte]
  protected def serializeValue(value: ValueType): Array[Byte]
  protected def deserializeKey(key: Array[Byte]): KeyType
  protected def deserializeValue(value: Array[Byte]): ValueType

  protected def serversForKey(key: KeyType): List[PartitionService]

  protected val rand = new scala.util.Random
  protected def pickRandomServer(key: KeyType): PartitionService = {
    val servers = serversForKey(key)
    servers(rand.nextInt(servers.size))
  }

  def put[K <: KeyType, V <: ValueType](key: K, value: Option[V]): Unit =
    serversForKey(key).foreach(_ !? PutRequest(serializeKey(key), value.map(serializeValue)))

  def get[K <: KeyType](key: K): Option[ValueType] =
    pickRandomServer(key) !? GetRequest(serializeKey(key)) match {
      case GetResponse(value) => value map deserializeValue
      case _ => throw new RuntimeException("Unexpected Message")
    }

  def getPrefix[K <: KeyType](key: K, prefixSize: Int, limit: Option[Int] = None, ascending: Boolean = true):Seq[(KeyType,ValueType)] = throw new RuntimeException("Unimplemented")
  def getRange(start: Option[KeyType], end: Option[KeyType], limit: Option[Int] = None, offset: Option[Int] = None, backwards:Boolean = false): Seq[(KeyType,ValueType)] = throw new RuntimeException("Unimplemented")
  def size():Int = throw new RuntimeException("Unimplemented")
  def ++=(that:Iterable[(KeyType,ValueType)]): Unit = throw new RuntimeException("Unimplemented")
}
