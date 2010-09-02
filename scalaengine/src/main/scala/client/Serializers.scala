package edu.berkeley.cs.scads.storage

import java.io._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.routing._

import org.apache.avro.Schema
import org.apache.avro.generic.{IndexedRecord, GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{BinaryData, DecoderFactory, BinaryEncoder, BinaryDecoder, DatumReader}
import org.apache.avro.specific.SpecificDatumReader

import com.googlecode.avro.runtime._

private[storage] trait AvroSerializing[KeyType <: IndexedRecord, ValueType <: IndexedRecord] {

  val keySchema: Schema
  val valueSchema: Schema

  def getKeySchema() : Schema = keySchema
  def getValueSchema() : Schema = valueSchema

  val keyReader: DatumReader[KeyType]
  val valueReader: DatumReader[ValueType] 

  protected def newKeyInstance: KeyType = 
    null.asInstanceOf[KeyType]
  protected def newValueInstance: ValueType = 
    null.asInstanceOf[ValueType]

  protected val decoderFactory = (new DecoderFactory).configureDirectDecoder(true)

  protected def deserializeKey(key: Array[Byte]): KeyType = {
    val dec = decoderFactory.createBinaryDecoder(key, null)
    keyReader.read(newKeyInstance, dec)
  }

  protected def deserializeValue(value: Array[Byte]): ValueType = {
    val dec = decoderFactory.createBinaryDecoder(value, null)
    valueReader.read(newValueInstance, dec)
  }

}

/**
 * Implementation of Scads Namespace that returns ScalaSpecificRecords
 */
class SpecificNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord]
    (namespace: String, timeout: Int, root: ZooKeeperProxy#ZooKeeperNode)
    (implicit cluster: ScadsClusterManager, keyType: Manifest[KeyType], valueType: Manifest[ValueType])
        extends QuorumProtocol[KeyType, ValueType](namespace, timeout, root)(cluster) 
        with    RoutingProtocol[KeyType, ValueType] 
        with    SimpleMetaData[KeyType, ValueType]
        with    AvroSerializing[KeyType, ValueType] {

  protected val keyClass   = keyType.erasure.asInstanceOf[Class[KeyType]]
  protected val valueClass = valueType.erasure.asInstanceOf[Class[ValueType]]

  val keySchema   = keyClass.newInstance.getSchema
  val valueSchema = valueClass.newInstance.getSchema

  val keyReader   = new SpecificDatumReader[KeyType](keySchema)
  val valueReader = new SpecificDatumReader[ValueType](valueSchema)

  override def newKeyInstance =
    keyClass.newInstance

  override def newValueInstance =
    valueClass.newInstance

  protected def serializeKey(key: KeyType): Array[Byte] = key.toBytes
  protected def serializeValue(value: ValueType): Array[Byte] = value.toBytes
}

class GenericNamespace(namespace: String,
                       timeout: Int,
                       root: ZooKeeperProxy#ZooKeeperNode,
                       val keySchema: Schema,
                       val valueSchema: Schema)
                      (implicit cluster : ScadsClusterManager)
    extends QuorumProtocol[GenericData.Record, GenericData.Record](namespace, timeout, root)(cluster)
    with    RoutingProtocol[GenericData.Record, GenericData.Record] 
    with    SimpleMetaData[GenericData.Record, GenericData.Record]
    with    AvroSerializing[GenericData.Record, GenericData.Record] {

  val keyReader   = new GenericDatumReader[GenericData.Record](keySchema)
  val valueReader = new GenericDatumReader[GenericData.Record](valueSchema)

  val keyWriter   = new GenericDatumWriter[GenericData.Record](keySchema)
  val valueWriter = new GenericDatumWriter[GenericData.Record](valueSchema)

  implicit def toRichIndexedRecord[T <: IndexedRecord](i: T) = new RichIndexedRecord[T](i)

  protected def serializeKey(key: GenericData.Record): Array[Byte] = key.toBytes
  protected def serializeValue(value: GenericData.Record): Array[Byte] = value.toBytes
}
