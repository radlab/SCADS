package edu.berkeley.cs.scads.storage.newclient

import java.io._

import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.avro.runtime._

import org.apache.avro._
import generic._
import io._
import specific._

abstract class AvroReaderWriter[T <: IndexedRecord](val remoteSchema: Option[Schema]) {

  trait ExposedDatumReader {
    def exposedNewRecord(old: AnyRef, schema: Schema): AnyRef
  }

  protected val reader: DatumReader[T] with ExposedDatumReader
  protected val writer: DatumWriter[T]

  def schema: Schema
  private val resolver = remoteSchema.map(rs => ResolvingDecoder.resolve(rs, schema))

  protected val bufferSize = 128

  private val decoderFactory: DecoderFactory = (new DecoderFactory).configureDirectDecoder(true)

  def newInstance: T

  def serialize(rec: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream(bufferSize)
    val enc  = new BinaryEncoder(baos)
    writer.write(rec, enc)
    baos.toByteArray
  }

  def deserialize(bytes: Array[Byte]): T = {
    val dec = decoderFactory.createBinaryDecoder(bytes, null)
    reader.read(newInstance, resolver.map(rs => new ResolvingDecoder(rs, dec)).getOrElse(dec))
  }

  def deserialize(stream: InputStream): T = {
    val dec = decoderFactory.createBinaryDecoder(stream, null)
    reader.read(newInstance, resolver.map(rs => new ResolvingDecoder(rs, dec)).getOrElse(dec))
  }

  /** Given schema, return a new instance of a record which has the given
   * schema */
  def newRecordInstance(schema: Schema): IndexedRecord = {
    reader.exposedNewRecord(null, schema).asInstanceOf[IndexedRecord]
  }

}

class AvroGenericReaderWriter[T >: GenericRecord <: IndexedRecord](_remoteSchema: Option[Schema], val schema: Schema) 
  extends AvroReaderWriter[T](_remoteSchema) {
  require(schema ne null)
  val reader = new GenericDatumReader[T](schema) with ExposedDatumReader {
    def exposedNewRecord(old: AnyRef, schema: Schema): AnyRef = 
      newRecord(old, schema)
  }
  val writer = new GenericDatumWriter[T](schema)
  def newInstance = new GenericData.Record(schema)
}

class AvroSpecificReaderWriter[T <: SpecificRecord](_remoteSchema: Option[Schema])(implicit tpe: Manifest[T])
  extends AvroReaderWriter[T](_remoteSchema) {
  @inline private def recClz = tpe.erasure.asInstanceOf[Class[T]]
  lazy val schema = recClz.newInstance.getSchema 
  val reader = new SpecificDatumReader[T](schema) with ExposedDatumReader {
    def exposedNewRecord(old: AnyRef, schema: Schema): AnyRef = 
      newRecord(old, schema)
  }
  val writer = new SpecificDatumWriter[T](schema)
  def newInstance = recClz.newInstance
}

trait AvroKeyValueSerializerLike[K <: IndexedRecord, V <: IndexedRecord]
  extends KeyValueSerializer[K, V] 
  with GlobalMetadata {

  protected val keyReaderWriter: AvroReaderWriter[K] 
  protected val valueReaderWriter: AvroReaderWriter[V]

  override def bytesToKey(bytes: Array[Byte]): K = 
    keyReaderWriter.deserialize(bytes)

  override def bytesToValue(bytes: Array[Byte]): V =
    valueReaderWriter.deserialize(bytes)

  override def bytesToBulk(k: Array[Byte], v: Array[Byte]): (K, V) =
    (bytesToKey(k), bytesToValue(v))

  override def keyToBytes(key: K): Array[Byte] =
    keyReaderWriter.serialize(key)

  override def valueToBytes(value: V): Array[Byte] = 
    valueReaderWriter.serialize(value)

  override def bulkToBytes(b: (K, V)): (Array[Byte], Array[Byte]) =
    (keyToBytes(b._1), valueToBytes(b._2))

  override def newKeyInstance = keyReaderWriter.newInstance 

  override def newRecordInstance(schema: Schema) = 
    // can use either key or value R/W, it doesn't matter (they share the same
    // global cache anyways)
    keyReaderWriter.newRecordInstance(schema)
}

trait AvroGenericKeyValueSerializer 
  extends AvroKeyValueSerializerLike[GenericRecord, GenericRecord] {
  protected lazy val keyReaderWriter = new AvroGenericReaderWriter[GenericRecord](Some(remoteKeySchema), keySchema)
  protected lazy val valueReaderWriter = new AvroGenericReaderWriter[GenericRecord](Some(remoteValueSchema), valueSchema)
}

trait AvroIndexedKeyValueSerializer 
  extends AvroKeyValueSerializerLike[IndexedRecord, IndexedRecord] {
  protected lazy val keyReaderWriter = new AvroGenericReaderWriter[IndexedRecord](Some(remoteKeySchema), keySchema)
  protected lazy val valueReaderWriter = new AvroGenericReaderWriter[IndexedRecord](Some(remoteValueSchema), valueSchema)
}

trait AvroSpecificKeyValueSerializer[K <: SpecificRecord, V <: SpecificRecord]
  extends AvroKeyValueSerializerLike[K, V] {
  implicit protected def keyManifest: Manifest[K]
  implicit protected def valueManifest: Manifest[V]
  protected lazy val keyReaderWriter = new AvroSpecificReaderWriter[K](Some(remoteKeySchema))
  protected lazy val valueReaderWriter = new AvroSpecificReaderWriter[V](Some(remoteValueSchema))
  override lazy val keySchema = 
    keyManifest.erasure.asInstanceOf[Class[K]].newInstance.getSchema

  override lazy val valueSchema =
    valueManifest.erasure.asInstanceOf[Class[V]].newInstance.getSchema
}

trait AvroPairSerializer[P <: AvroPair]
  extends PairSerializer[P]
  with GlobalMetadata {

  private lazy val keyReaderWriter = new AvroGenericReaderWriter[IndexedRecord](Some(remoteKeySchema), keySchema)
  private lazy val valueReaderWriter = new AvroGenericReaderWriter[IndexedRecord](Some(remoteValueSchema), valueSchema)

  implicit protected def pairManifest: Manifest[P]

  // TODO: need to recreate the pair schema from key/value schema
  private val pairReaderWriter = new AvroSpecificReaderWriter[P](None)

  override def bytesToKey(bytes: Array[Byte]): IndexedRecord = 
    keyReaderWriter.deserialize(bytes)

  override def bytesToValue(bytes: Array[Byte]): IndexedRecord =
    valueReaderWriter.deserialize(bytes)

  override def bytesToBulk(key: Array[Byte], value: Array[Byte]): P = {
    // We could create an input stream here which concats key and value w/o
    // copying, but that is probably more work than is worth, since
    // System.arraycopy is alreay pretty optimized
    val bytes = new Array[Byte](key.length + value.length)
    System.arraycopy(key, 0, bytes, 0, key.length)
    System.arraycopy(value, 0, bytes, key.length, value.length)
    pairReaderWriter.deserialize(bytes)
  }

  override def keyToBytes(key: IndexedRecord): Array[Byte] =
    keyReaderWriter.serialize(key)

  override def valueToBytes(value: IndexedRecord): Array[Byte] = 
    valueReaderWriter.serialize(value)

  override def bulkToBytes(b: P): (Array[Byte], Array[Byte]) = 
    (keyToBytes(b.key), valueToBytes(b.value))

  override def newKeyInstance = keyReaderWriter.newInstance

  override def newRecordInstance(schema: Schema) =
    keyReaderWriter.newRecordInstance(schema)

  override lazy val keySchema = 
    pairManifest.erasure.asInstanceOf[Class[P]].newInstance.key.getSchema

  override lazy val valueSchema = 
    pairManifest.erasure.asInstanceOf[Class[P]].newInstance.value.getSchema
}
