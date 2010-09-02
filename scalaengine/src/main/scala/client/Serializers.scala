package edu.berkeley.cs.scads.storage

import java.io._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.routing._

import org.apache.avro.Schema
import Schema.Type
import org.apache.avro.generic.{IndexedRecord, GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{BinaryData, DecoderFactory, BinaryEncoder, BinaryDecoder, DatumReader, ResolvingDecoder}
import org.apache.avro.specific.SpecificDatumReader

import com.googlecode.avro.runtime._

private[storage] trait AvroSerializing[KeyType <: IndexedRecord, ValueType <: IndexedRecord] 
  extends Namespace[KeyType, ValueType] {

  val keySchema: Schema
  val valueSchema: Schema

  def getKeySchema(): Schema = keySchema
  def getValueSchema(): Schema = valueSchema

  // remoteKeySchema and remoteValueSchema should be lazy, since nsRoot cannot
  // be accessed until either load or create has been called

  private lazy val remoteKeySchema =
    Schema.parse(new String(nsRoot("keySchema").data))

  private lazy val remoteValueSchema =
    Schema.parse(new String(nsRoot("valueSchema").data))

  // server is the writer, client is the reader
  private lazy val keySchemaResolver =
    ResolvingDecoder.resolve(remoteKeySchema, keySchema)

  // server is the writer, client is the reader
  private lazy val valueSchemaResolver =
    ResolvingDecoder.resolve(remoteValueSchema, valueSchema)

  private def validate() {
    // check to see if remote schemas are *prefixes* of our schemas
    // this is a limitation of avro's BinaryData.compare() API- it tries to
    // compare byte streams in order, and gives no provision for using a
    // resolved decoder to interpret the byte stream in compare(). Therefore,
    // in order for the server to understand our serialized form, we mandate
    // that the remote schema byte format is a prefix of the client schemas.

    // ensures a is a prefix of b
    def validatePrefix(a: Schema, b: Schema) = {
      require(a.getType == b.getType, "Schemas are not of same type: %s and %s".format(a.getType, b.getType))
      a.getType match {
        case Type.RECORD =>
          import scala.collection.JavaConversions._
          val afields = a.getFields.toSeq
          val bfields = b.getFields.toSeq.take(afields.size)
          afields == bfields
        case Type.MAP =>
          throw new AssertionError("SCADS schemas cannot be Map types, because they cannot be compared by Avro")
        case Type.ARRAY =>
          // require elements to be equal in this case, since the avro format
          // for arrays does not allow us to get away with prefix matching
          a.getElementType == b.getElementType
        case Type.UNION =>
          // TODO: need to check that the union types are also a prefix
          true
      }
    }

    if (!validatePrefix(remoteKeySchema, keySchema))
      throw new RuntimeException("Server key schema %s is not a prefix of client key schema %s".format(remoteKeySchema, keySchema))

    if (!validatePrefix(remoteValueSchema, valueSchema))
      throw new RuntimeException("Server value schema %s is not a prefix of client value schema %s".format(remoteValueSchema, valueSchema))

    logger.debug("Prefix Validation Complete")

    // force initialization of resolvers
    keySchemaResolver
    valueSchemaResolver

    logger.debug("Schema ResolvingDecoders computed")
  }

  override def load() {
    super.load()
    validate()
  }

  override def create(ranges: List[(Option[KeyType], List[StorageService])]) {
    super.create(ranges)
    validate()
  }

  val keyReader: DatumReader[KeyType]
  val valueReader: DatumReader[ValueType] 

  protected def newKeyInstance: KeyType = 
    null.asInstanceOf[KeyType]
  protected def newValueInstance: ValueType = 
    null.asInstanceOf[ValueType]

  protected val decoderFactory = (new DecoderFactory).configureDirectDecoder(true)

  protected def deserializeKey(key: Array[Byte]): KeyType = {
    val dec = decoderFactory.createBinaryDecoder(key, null)
    keyReader.read(newKeyInstance, new ResolvingDecoder(keySchemaResolver, dec))
  }

  protected def deserializeValue(value: Array[Byte]): ValueType = {
    val dec = decoderFactory.createBinaryDecoder(value, null)
    valueReader.read(newValueInstance, new ResolvingDecoder(valueSchemaResolver, dec))
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
