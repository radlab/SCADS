package edu.berkeley.cs.scads.storage

import java.io._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.routing._

import org.apache.avro.Schema
import Schema.Type
import org.apache.avro.generic.{IndexedRecord, GenericData, GenericRecord, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{BinaryData, DecoderFactory, BinaryEncoder, BinaryDecoder, 
                           DatumReader, DatumWriter, ResolvingDecoder}
import org.apache.avro.specific.{SpecificRecord, SpecificDatumReader, SpecificDatumWriter}

import edu.berkeley.cs.avro.marker.AvroPair
import edu.berkeley.cs.avro.runtime._

abstract class AvroReaderWriter[T <: IndexedRecord] {

  trait ExposedDatumReader {
    def exposedNewRecord(old: AnyRef, schema: Schema): AnyRef
  }

  val reader: DatumReader[T] with ExposedDatumReader
  val writer: DatumWriter[T]
  val schema: Schema
  val resolver: AnyRef

  protected val bufferSize = 128

  val decoderFactory: DecoderFactory = (new DecoderFactory).configureDirectDecoder(true)

  def newInstance: T

  def serialize(rec: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream(bufferSize)
    val enc  = new BinaryEncoder(baos)
    writer.write(rec, enc)
    baos.toByteArray
  }

  def deserialize(bytes: Array[Byte]): T = {
    val dec = decoderFactory.createBinaryDecoder(bytes, null)
    reader.read(newInstance, new ResolvingDecoder(resolver, dec))
  }

  def deserialize(stream: InputStream): T = {
    val dec = decoderFactory.createBinaryDecoder(stream, null)
    reader.read(newInstance, new ResolvingDecoder(resolver, dec))
  }

  /** Given schema, return a new instance of a record which has the given
   * schema */
  def newRecordInstance(schema: Schema): IndexedRecord = {
    reader.exposedNewRecord(null, schema).asInstanceOf[IndexedRecord]
  }

}

abstract class AvroGenericReaderWriterLike[T <: IndexedRecord](val schema: Schema) 
  extends AvroReaderWriter[T] {
  assert(schema ne null)
  val reader = new GenericDatumReader[T](schema) with ExposedDatumReader {
    def exposedNewRecord(old: AnyRef, schema: Schema): AnyRef = 
      newRecord(old, schema)
  }
  val writer = new GenericDatumWriter[T](schema)
}

abstract class AvroGenericReaderWriter(_schema: Schema) 
  extends AvroGenericReaderWriterLike[GenericRecord](_schema) {
  def newInstance = new GenericData.Record(schema)
}

abstract class AvroIndexedReaderWriter(_schema: Schema) 
  extends AvroGenericReaderWriterLike[IndexedRecord](_schema) {
  def newInstance = new GenericData.Record(schema)
}

abstract class AvroSpecificReaderWriter[T <: SpecificRecord](implicit tpe: Manifest[T])
  extends AvroReaderWriter[T] {
  val recClz = tpe.erasure.asInstanceOf[Class[T]]
  val schema = recClz.newInstance.getSchema 
  val reader = new SpecificDatumReader[T](schema) with ExposedDatumReader {
    def exposedNewRecord(old: AnyRef, schema: Schema): AnyRef = 
      newRecord(old, schema)
  }
  val writer = new SpecificDatumWriter[T](schema)
  def newInstance = recClz.newInstance
}

trait AvroSerializing[KeyType <: IndexedRecord, 
                      ValueType <: IndexedRecord, 
                      RecordType <: IndexedRecord,
                      RangeType] {
  this: Namespace[KeyType, ValueType, RecordType, RangeType] =>

  val keyReaderWriter: AvroReaderWriter[KeyType]
  val valueReaderWriter: AvroReaderWriter[ValueType]

  val keySchema = keyReaderWriter.schema
  val valueSchema = valueReaderWriter.schema

  // remoteKeySchema and remoteValueSchema should be lazy, since nsRoot cannot
  // be accessed until either load or create has been called

  private lazy val remoteKeySchema =
    Schema.parse(new String(nsRoot("keySchema").data))

  private lazy val remoteValueSchema =
    Schema.parse(new String(nsRoot("valueSchema").data))

  // server is the writer, client is the reader
  protected lazy val keySchemaResolver =
    ResolvingDecoder.resolve(remoteKeySchema, getKeySchema)

  // server is the writer, client is the reader
  protected lazy val valueSchemaResolver =
    ResolvingDecoder.resolve(remoteValueSchema, getValueSchema)

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

    if (!validatePrefix(remoteKeySchema, getKeySchema))
      throw new RuntimeException("Server key schema %s is not a prefix of client key schema %s".format(remoteKeySchema, getKeySchema))

    if (!validatePrefix(remoteValueSchema, getValueSchema))
      throw new RuntimeException("Server value schema %s is not a prefix of client value schema %s".format(remoteValueSchema, getValueSchema))

    logger.debug("Prefix Validation Complete")

    // force initialization of resolvers
    keySchemaResolver
    valueSchemaResolver

    logger.debug("Schema ResolvingDecoders computed")
  }

  // TODO: be less eager about validation
  onLoad {
    validate()
  }

  // TODO: be less eager about validation
  onCreate {
    ranges => {
      validate()
    }
  }

  // Key ops
  protected def deserializeKey(key: Array[Byte]): KeyType =
    keyReaderWriter.deserialize(key)

  protected def serializeKey(key: KeyType): Array[Byte] =
    keyReaderWriter.serialize(key)

  // Value ops                                            
  protected def deserializeValue(value: Array[Byte]): ValueType =
    valueReaderWriter.deserialize(value)

  protected def serializeValue(value: ValueType): Array[Byte] =
    valueReaderWriter.serialize(value)

  // Ret ops
  protected def deserializeRecordType(key: Array[Byte], value: Array[Byte]): RecordType
  
  // Range ops
  protected def deserializeRangeType(key: Array[Byte], value: Array[Byte]): RangeType

  protected def extractKeyValueFromRangeType(rangeType: RangeType): (KeyType, ValueType)

  def newRecordInstance(schema: Schema): IndexedRecord = 
    keyReaderWriter.newRecordInstance(schema)

  def newKeyInstance: KeyType =
    keyReaderWriter.newInstance
}

class PairNamespace[PairType <: AvroPair]
  (namespace: String, 
   timeout: Int, 
   root: ZooKeeperProxy#ZooKeeperNode)
  (implicit cluster: ScadsCluster, pairType: Manifest[PairType])
    extends Namespace[IndexedRecord, IndexedRecord, PairType, PairType](namespace, timeout, root)
    with    RangeRouting[IndexedRecord, IndexedRecord, PairType, PairType]
    with    SimpleMetaData[IndexedRecord, IndexedRecord, PairType, PairType]
    with    QuorumProtocol[IndexedRecord, IndexedRecord, PairType, PairType]
    with    AvroSerializing[IndexedRecord, IndexedRecord, PairType, PairType]
    with    IndexManager[PairType] {

  import IndexManager._

  lazy val pairClz = pairType.erasure.asInstanceOf[Class[PairType]]
  lazy val (pairSchema, pairKeySchema, pairValueSchema) = {
    val p = pairClz.newInstance
    (p.getSchema, p.key.getSchema, p.value.getSchema)
  }

  lazy val keyReaderWriter = new AvroIndexedReaderWriter(pairKeySchema) {
    lazy val resolver = keySchemaResolver
  }

  lazy val valueReaderWriter = new AvroIndexedReaderWriter(pairValueSchema) {
    lazy val resolver = valueSchemaResolver
  }

  lazy val pairReaderWriter = new AvroSpecificReaderWriter[PairType] {
    lazy val resolver = ResolvingDecoder.resolve(pairSchema, pairSchema)
  }

  @inline private def bytesToPair(key: Array[Byte], value: Array[Byte]): PairType = {
    // We could create an input stream here which concats key and value w/o
    // copying, but that is probably more work than is worth, since
    // System.arraycopy is alreay pretty optimized
    val bytes = new Array[Byte](key.length + value.length)
    System.arraycopy(key, 0, bytes, 0, key.length)
    System.arraycopy(value, 0, bytes, key.length, value.length)
    pairReaderWriter.deserialize(bytes)
  }

  protected def deserializeRecordType(key: Array[Byte], value: Array[Byte]) =
    bytesToPair(key, value)

  protected def deserializeRangeType(key: Array[Byte], value: Array[Byte]) = 
    bytesToPair(key, value)

  protected def extractKeyValueFromRangeType(rangeType: PairType) = 
    (rangeType.key, rangeType.value)

  /** Override put to do index maintainence. 
   * Note that this issues a get request first, to determine how to update the
   * index
   */
  override def put(key: IndexedRecord, value: Option[IndexedRecord]): Unit = {
    val optOldValue = get(key)
    indexCache.values.foreach { case (ns, mapping) =>
      val optOldIndex = optOldValue.map(oldValue => makeIndexFor(key, oldValue.value, ns.keySchema, mapping))
      value match {
        case None => // delete old index (if it exists)
          optOldIndex.foreach(oldIndex => ns.put(oldIndex, None))
        case Some(newValue) => // update index if necessary, deleting stale ones if necessary
          val newIndex = makeIndexFor(key, newValue, ns.keySchema, mapping)
          val optStaleValue = optOldIndex.flatMap(oldIndex => if (oldIndex != newIndex) Some(oldIndex) else None)
          optStaleValue.foreach(staleValue => ns.put(staleValue, None))
          if (optOldValue.isEmpty || optStaleValue.isDefined)
            ns.put(newIndex, dummyIndexValue)
      }
    }
    // put the actual key/value pair AFTER index maintainence
    super.put(key, value)
  }

  /** Override bulkLoadCallback to do index maintainence. Note that ++= for
   * PairNamespace assumes that all the records passed to it are new records,
   * and thus makes no effort to delete old indexes. */
  override protected def bulkLoadCallback(elems: Seq[PairType]): Unit = {
    // TODO: use pforeach?
    indexCache.values.foreach { case (ns, mapping) =>
      ns ++= elems.map(e => makeIndexFor(e, ns.keySchema, mapping))
    }
  }

}


trait SpecificNamespaceTrait[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord] extends
    AvroSerializing[KeyType, ValueType, ValueType, (KeyType, ValueType)]
{
 this: Namespace[KeyType, ValueType, ValueType, (KeyType, ValueType)] =>

 implicit val keyType: Manifest[KeyType]
 implicit val valueType: Manifest[ValueType]
  
 lazy val keyReaderWriter = new AvroSpecificReaderWriter[KeyType] {
    lazy val resolver = keySchemaResolver
  }

  lazy val valueReaderWriter = new AvroSpecificReaderWriter[ValueType] {
    lazy val resolver = valueSchemaResolver
  }

  protected def deserializeRecordType(key: Array[Byte], value: Array[Byte]) =
    deserializeValue(value)

  protected def deserializeRangeType(key: Array[Byte], value: Array[Byte]) =
    (deserializeKey(key), deserializeValue(value))

  protected def extractKeyValueFromRangeType(rangeType: (KeyType, ValueType)) =
    rangeType

  lazy val genericNamespace = createGenericVersion()

  private def createGenericVersion(): GenericNamespace = {
    val genericNs = new GenericNamespace(namespace, timeout, root, getKeySchema, getValueSchema)
    genericNs.load
    genericNs
  }
}

/**
 * Implementation of Scads Namespace that returns ScalaSpecificRecords
 * TODO: no reason to restrict to ScalaSpecificRecord. We could make this for
 * any SpecificRecords to be more general
 */
class SpecificNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord]
    (namespace: String,
     timeout: Int,
     root: ZooKeeperProxy#ZooKeeperNode)
    (implicit  override  val cluster: ScadsCluster, override val keyType: Manifest[KeyType], override val valueType: Manifest[ValueType])
        extends Namespace[KeyType, ValueType, ValueType, (KeyType, ValueType)] (namespace, timeout, root)
        with    RangeRouting[KeyType, ValueType, ValueType, (KeyType, ValueType)]
        with    SimpleMetaData[KeyType, ValueType, ValueType, (KeyType, ValueType)]
        with    QuorumProtocol[KeyType, ValueType, ValueType, (KeyType, ValueType)]
        with    SpecificNamespaceTrait[KeyType, ValueType] {


}

/**
 * Implementation of Scads Namespace that returns ScalaSpecificRecords
 * TODO: no reason to restrict to ScalaSpecificRecord. We could make this for
 * any SpecificRecords to be more general
 */
class SpecificHashRoutingNamespace[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord]
    (namespace: String,
     timeout: Int,
     root: ZooKeeperProxy#ZooKeeperNode,
     override var routingFieldPos : Seq[Int])
    (implicit  override  val cluster: ScadsCluster, override val keyType: Manifest[KeyType], override val valueType: Manifest[ValueType])
        extends Namespace[KeyType, ValueType, ValueType, (KeyType, ValueType)] (namespace, timeout, root)
        with    HashRouting[KeyType, ValueType, ValueType, (KeyType, ValueType)] 
        with    SimpleMetaData[KeyType, ValueType, ValueType, (KeyType, ValueType)]
        with    QuorumProtocol[KeyType, ValueType, ValueType, (KeyType, ValueType)]
        with    SpecificNamespaceTrait[KeyType, ValueType] {

}

class IndexNamespace(namespace: String,
                     timeout: Int,
                     root: ZooKeeperProxy#ZooKeeperNode,
		     keySchema: Schema)
                    (implicit cluster: ScadsCluster)
    extends Namespace[IndexedRecord, IndexedRecord, IndexedRecord, IndexedRecord](namespace, timeout, root)
    with    RangeRouting[IndexedRecord, IndexedRecord, IndexedRecord, IndexedRecord] 
    with    SimpleMetaData[IndexedRecord, IndexedRecord, IndexedRecord, IndexedRecord]
    with    QuorumProtocol[IndexedRecord, IndexedRecord, IndexedRecord, IndexedRecord]
    with    AvroSerializing[IndexedRecord, IndexedRecord, IndexedRecord, IndexedRecord] {

  import IndexManager._

  lazy val keyReaderWriter = new AvroIndexedReaderWriter(keySchema) {
    lazy val resolver = keySchemaResolver
  }

  lazy val valueReaderWriter = new AvroIndexedReaderWriter(indexValueSchema) {
    lazy val resolver = valueSchemaResolver
  }

  protected def deserializeRecordType(key: Array[Byte], value: Array[Byte]) =
    deserializeValue(value) 

  protected def deserializeRangeType(key: Array[Byte], value: Array[Byte]) =
    deserializeKey(key)

  protected def extractKeyValueFromRangeType(key: IndexedRecord) = 
    (key, dummyIndexValue)

}

class GenericNamespace(namespace: String,
                       timeout: Int,
                       root: ZooKeeperProxy#ZooKeeperNode,
                       override val keySchema: Schema,
                       override val valueSchema: Schema)
                      (implicit  override  val cluster : ScadsCluster)
    extends Namespace[GenericRecord, GenericRecord, GenericRecord, (GenericRecord, GenericRecord)] (namespace, timeout, root)
    with    RangeRouting[GenericRecord, GenericRecord, GenericRecord, (GenericRecord, GenericRecord)] 
    with    WorkloadStatsProtocol[GenericRecord, GenericRecord, GenericRecord, (GenericRecord, GenericRecord)]
    with    SimpleMetaData[GenericRecord, GenericRecord, GenericRecord, (GenericRecord, GenericRecord)]
    with    QuorumProtocol[GenericRecord, GenericRecord, GenericRecord, (GenericRecord, GenericRecord)]
    with    AvroSerializing[GenericRecord, GenericRecord, GenericRecord, (GenericRecord, GenericRecord)] {

  lazy val keyReaderWriter = new AvroGenericReaderWriter(keySchema) {
    lazy val resolver = keySchemaResolver
  }

  lazy val valueReaderWriter = new AvroGenericReaderWriter(valueSchema) {
    lazy val resolver = valueSchemaResolver
  }

  protected def deserializeRecordType(key: Array[Byte], value: Array[Byte]) =
    deserializeValue(value) 

  protected def deserializeRangeType(key: Array[Byte], value: Array[Byte]) =
    (deserializeKey(key), deserializeValue(value))

  protected def extractKeyValueFromRangeType(rangeType: (GenericRecord, GenericRecord)) = 
    rangeType
}
