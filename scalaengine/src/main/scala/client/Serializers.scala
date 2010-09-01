package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.routing._

import org.apache.avro.Schema
import org.apache.avro.generic.{IndexedRecord, GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{BinaryData, DecoderFactory, BinaryEncoder, BinaryDecoder}
import com.googlecode.avro.runtime._

/**
 * Implementation of Scads Namespace that returns ScalaSpecificRecords
 */
class SpecificNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord]
    (namespace:String, timeout:Int, root: ZooKeeperProxy#ZooKeeperNode)
    (implicit  cluster : ScadsClusterManager, keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType])
        extends QuorumProtocol[KeyType, ValueType](namespace, timeout, root)(cluster) with RoutingProtocol[KeyType, ValueType] with SimpleMetaData[KeyType, ValueType] {
  protected val keyClass = keyType.erasure.asInstanceOf[Class[ScalaSpecificRecord]]
  protected val valueClass = valueType.erasure.asInstanceOf[Class[ScalaSpecificRecord]]
  val keySchema = keyType.erasure.newInstance.asInstanceOf[KeyType].getSchema()
  val valueSchema = valueType.erasure.newInstance.asInstanceOf[ValueType].getSchema()


  protected def serializeKey(key: KeyType): Array[Byte] = key.toBytes
  protected def serializeValue(value: ValueType): Array[Byte] = value.toBytes

  protected def getKeySchema() : Schema = keySchema

  protected def getValueSchema() : Schema = valueSchema

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

class GenericNamespace(namespace:String,
                       timeout:Int,
                       root: ZooKeeperProxy#ZooKeeperNode,
                       val keySchema:Schema,
                       val valueSchema:Schema)
                      (implicit cluster : ScadsClusterManager)
        extends QuorumProtocol[GenericData.Record, GenericData.Record](namespace, timeout, root)(cluster)
                with RoutingProtocol[GenericData.Record, GenericData.Record] with SimpleMetaData[GenericData.Record, GenericData.Record] {
  val decoderFactory = DecoderFactory.defaultFactory()
  val keyReader = new GenericDatumReader[GenericData.Record](keySchema)
  val valueReader = new GenericDatumReader[GenericData.Record](valueSchema)
  val keyWriter = new GenericDatumWriter[GenericData.Record](keySchema)
  val valueWriter = new GenericDatumWriter[GenericData.Record](valueSchema)



  protected def getKeySchema() =  keySchema
  protected def getValueSchema() = valueSchema

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
