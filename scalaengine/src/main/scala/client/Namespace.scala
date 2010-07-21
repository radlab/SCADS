package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

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
class SpecificNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord](namespace:String, timeout:Int, root: ZooKeeperProxy#ZooKeeperNode)(implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]) extends Namespace[KeyType, ValueType](namespace, timeout, root) with PartitionPolicy[KeyType] {
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

class GenericNamespace(namespace:String, timeout:Int, root: ZooKeeperProxy#ZooKeeperNode) extends Namespace[GenericData.Record, GenericData.Record](namespace, timeout, root) with PartitionPolicy[GenericData.Record] {
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
 * Handles interaction with a single SCADS Namespace
 * TODO: Add functions for splitting/merging partitions (protocol for moving data safely)
 * TODO: Handle the need for possible schema resolutions
 * TODO: Create KVStore Trait that namespace implements
 */
abstract class Namespace[KeyType <: IndexedRecord, ValueType <: IndexedRecord](val namespace:String, val timeout:Int, val root: ZooKeeperProxy#ZooKeeperNode) extends KeyValueStore[KeyType, ValueType] {
  protected val logger = Logger.getLogger("Namespace")
  protected val nsNode = root("namespaces/"+namespace)
  protected val keySchema = Schema.parse(new String(nsNode("keySchema").data))
  protected val valueSchema = Schema.parse(new String(nsNode("valueSchema").data))

  /* Partition Policy Methods */
  protected def serversForKey(key:KeyType):List[RemoteActor]
  protected def splitRange(startKey: Option[KeyType],endKey: Option[KeyType]): PartitionPolicy[KeyType]#RangeIterator

  /* DeSerialization Methods */
  protected def serializeKey(key: KeyType): Array[Byte]
  protected def serializeValue(value: ValueType): Array[Byte]
  protected def deserializeKey(key: Array[Byte]): KeyType
  protected def deserializeValue(value: Array[Byte]): ValueType

  protected def decodeKey(b:Array[Byte]): GenericData.Record = {
    val decoder = DecoderFactory.defaultFactory().createBinaryDecoder(b, null)
    val reader = new org.apache.avro.generic.GenericDatumReader[GenericData.Record](keySchema)
    reader.read(null,decoder)
  }

  def put[K <: KeyType, V <: ValueType](key: K, value: Option[V]): Unit = {
    val nodes = serversForKey(key)
    serversForKey(key).foreach(_ !? PutRequest(namespace, serializeKey(key), value map serializeValue))
  }

  def get[K <: KeyType](key: K): Option[ValueType] = {
    val server = serversForKey(key).first
    server !? GetRequest(namespace, serializeKey(key)) match {
      case Record(_, value) => value.map(v => deserializeValue(v))
      case _ => throw new RuntimeException("Unexpected Message")
    }
  }

  /* TODO: this logic should be in the storage handler as its nessesity is really a side effect of the B-Tree Traversal primatives available */
  protected def minRecord(rec:IndexedRecord, prefix:Int, ascending:Boolean):Unit = {
    val fields = rec.getSchema.getFields
    for (i <- (prefix to (fields.size() - 1))) { // set remaining values to min/max
      fields.get(i).schema.getType match {
        case org.apache.avro.Schema.Type.ARRAY =>
          if (ascending)
            rec.put(i,new GenericData.Array(0,fields.get(i).schema))
          else
            throw new Exception("Can't do descending search with an array in the prefix")
        case org.apache.avro.Schema.Type.BOOLEAN =>
          if (ascending)
            rec.put(i,false)
          else
            rec.put(i,true)
        case org.apache.avro.Schema.Type.BYTES =>
          if (ascending)
            rec.put(i,"".getBytes)
          else
            throw new Exception("Can't do descending search with bytes the prefix")
        case org.apache.avro.Schema.Type.DOUBLE =>
          if (ascending)
            rec.put(i,java.lang.Double.MIN_VALUE)
          else
            rec.put(i,java.lang.Double.MAX_VALUE)
        case org.apache.avro.Schema.Type.ENUM =>
          throw new Exception("ENUM not supported at the moment")
        case org.apache.avro.Schema.Type.FIXED =>
          throw new Exception("FIXED not supported at the moment")
        case org.apache.avro.Schema.Type.FLOAT =>
          if (ascending)
            rec.put(i,java.lang.Float.MIN_VALUE)
          else
            rec.put(i,java.lang.Float.MAX_VALUE)
        case org.apache.avro.Schema.Type.INT =>
          if (ascending)
            rec.put(i,java.lang.Integer.MIN_VALUE)
          else
            rec.put(i,java.lang.Integer.MAX_VALUE)
        case org.apache.avro.Schema.Type.LONG =>
          if (ascending)
            rec.put(i,java.lang.Long.MIN_VALUE)
          else
            rec.put(i,java.lang.Long.MAX_VALUE)
        case org.apache.avro.Schema.Type.MAP =>
          throw new Exception("MAP not supported at the moment")
        case org.apache.avro.Schema.Type.NULL =>
          // null is only null, so it's already min, has no max
          if (!ascending)
            throw new Exception("Can't do descending search with null in the prefix")
        case org.apache.avro.Schema.Type.RECORD =>
          if (rec.get(i) != null)
            minRecord(rec.get(i).asInstanceOf[ScalaSpecificRecord],0,ascending)
        case org.apache.avro.Schema.Type.STRING =>
          if (ascending)
            rec.put(i,new Utf8(""))
          else {
            // NOTE: We make the "max" string 20 max char values.  This won't work if you're putting big, max valued strings in your db
            rec.put(i,new Utf8(new String(Array.fill[Byte](20)(127.asInstanceOf[Byte]))))
          }
        case org.apache.avro.Schema.Type.UNION =>
          throw new Exception("UNION not supported at the moment")
        case other =>
          logger.warn("Got a type I don't know how to set to minimum, this getPrefix might not behave as expected: "+other)
      }
    }
  }

  def getPrefix[K <: KeyType](key: K, prefixSize: Int, limit: Option[Int] = None, ascending: Boolean = true):Seq[(KeyType,ValueType)] = throw new RuntimeException("Unimplemented")
  def getRange(start: Option[KeyType], end: Option[KeyType], limit: Option[Int] = None, offset: Option[Int] = None, backwards:Boolean = false): Seq[(KeyType,ValueType)] = throw new RuntimeException("Unimplemented")
  def size():Int = throw new RuntimeException("Unimplemented")
  def ++=(that:Iterable[(KeyType,ValueType)]): Unit = throw new RuntimeException("Unimplemented")
}
