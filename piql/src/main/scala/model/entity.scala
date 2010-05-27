package edu.berkeley.cs.scads.piql

import org.apache.avro.Schema
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.specific.SpecificDatumReader

import java.io.InputStream
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

import scala.reflect.Manifest

import edu.berkeley.cs.scads.storage.{ScadsCluster, Namespace}
import edu.berkeley.cs.scads.piql.parser.BoundValue

class PiqlDataReader(schema: Schema) extends SpecificDatumReader[Any](schema) {
	override def newRecord(old: Object, schema: Schema): Object = {
		return old
	}
}

trait KeyValueLike {
    def get(k: String): Any
    def put(k: String, v: Any): Unit
}

abstract class KeyPart[T <: Entity[_,_]](implicit manifest: Manifest[T]) 
extends EntityPart with QueryExecutor { 
  protected val namespace$: String
  protected def flatBoundValues$: List[BoundValue]
  def retrieve()(implicit env: Environment): T = {
    qLogger.debug("Executing KeyPart Get") 
    val result = 
      materialize(
        manifest.erasure.asInstanceOf[Class[Entity[_,_]]],
        singleGet(namespace$, flatBoundValues$))
    val s = result.asInstanceOf[Seq[T]]
    if (s.isEmpty)
      null.asInstanceOf[T]
    else
      s.head
  }
}

abstract class EntityPart extends SpecificRecordBase
                          with    SpecificRecord
                          with    KeyValueLike {
	val reader = new PiqlDataReader(getSchema)

  override def parse(bytes: ByteBuffer): Unit = {
    val buff = bytes.duplicate
    val in = new InputStream() {
      override def read(): Int = {
        if(buff.remaining > 0)
          buff.get & 0x00FF
        else
          -1
      }
    }
		val dec = new BinaryDecoder(in)
    reader.read(this, dec)
  }

	override def parse(bytes: Array[Byte]): Unit = {
		val in = new ByteArrayInputStream(bytes)
		val dec = new BinaryDecoder(in)
		reader.read(this, dec)
	}

  def flatPut(idx: Int, value: Any)
  def flatValues: List[Any]
}

abstract class SecondaryIndexType[EntityType <: Entity[_,_], TargetType <: KeyPart[_]] extends EntityPart {

  val namespace: String

  def save(implicit env: Environment) {
    env.namespaces(namespace).put(this, mkTargetKey)
  }

  def delete(implicit env: Environment) {
    env.namespaces(namespace).put(this, null)
  }

  def fillFromEntity(entity: EntityType): Unit
  def mkTargetKey: TargetType

  def get(k: String): Any = throw new UnsupportedOperationException("get")
  def put(k: String, v: Any): Unit = throw new UnsupportedOperationException("get")

}

abstract class Entity[KeyType <: SpecificRecordBase, ValueType <: SpecificRecordBase] (implicit keyType: scala.reflect.Manifest[KeyType], valueType: scala.reflect.Manifest[ValueType]) extends KeyValueLike {

  val namespace: String

  val key: KeyType
  val value: ValueType

  val indexes: Map[String, Schema]
  val secondaryIndexes: List[SecondaryIndexType[EntityType, _]]

  type EntityType <: Entity[KeyType, ValueType]
  /* private[piql] */ var oldEntity: EntityType

  protected def newEntityInstance: EntityType

  def deleteCurrentSecondaryIndexes(implicit env: Environment) {
    secondaryIndexes.foreach(_.fillFromEntity(this.asInstanceOf[EntityType]))
    secondaryIndexes.foreach(_.delete)
  }

  def deleteOldSecondaryIndexes(implicit env: Environment) {
    if (oldEntity ne null) {
      secondaryIndexes.foreach(_.fillFromEntity(oldEntity))
      secondaryIndexes.foreach(_.delete)
    }
  }

  def updateSecondaryIndexes(implicit env: Environment) {
    secondaryIndexes.foreach(_.fillFromEntity(this.asInstanceOf[EntityType]))
    secondaryIndexes.foreach(_.save)
  }

  /** TODO: remove load */
  def load(pk: KeyType)(implicit cluster: ScadsCluster) {
    key.parse(pk.toBytes)
    cluster.getNamespace[KeyType, ValueType](namespace).get(pk, value)
  }

  def save(implicit env: Environment) {
    // 1) save the new values
    env.namespaces(namespace).put(key, value)
    // 2) delete the old indexes
    deleteOldSecondaryIndexes
    // 3) save the new indexes
    updateSecondaryIndexes 
    // 4) set oldEntity to reflect this newly saved object
    oldEntity = newEntityInstance
    oldEntity.key.parse(key.toBytes) 
    oldEntity.value.parse(value.toBytes)
  }

  def delete(implicit env: Environment) {
    deleteOldSecondaryIndexes
    deleteCurrentSecondaryIndexes
    env.namespaces(namespace).put(key, null)
  }

  override def toString(): String = {
    "<Entity " + key.toString + " " + value.toString + ">"
  }
}
