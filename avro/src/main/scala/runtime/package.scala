package edu.berkeley.cs.avro

import _root_.edu.berkeley.cs.avro.marker.{AvroUnion, AvroRecord}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, IndexedRecord}
import org.apache.avro.specific.{SpecificData, SpecificRecord}

package object runtime {

  implicit def toRichIndexedRecord[RecType <: IndexedRecord](record: RecType) = new RichIndexedRecord(record)

  implicit def toJsonObject(json: String) = new JsonObject(json)

  implicit def toOption[A](a: A) = Option(a)

  /**
   * Type Parameterized Schema wrapper
   */
  case class TypedSchema[T](schema: Schema, classLoader: ClassLoader)

  /* implicit to unwrap typed schemas */
  implicit def untypeSchema[A](t: TypedSchema[A]): Schema = t.schema


  implicit def findSchema[C <: GenericContainer](implicit r: Manifest[C]): TypedSchema[C] = {
    val clz = r.erasure.asInstanceOf[Class[C]]
    if (classOf[AvroRecord].isAssignableFrom(clz))
      TypedSchema[C](clz.newInstance().getSchema, clz.getClassLoader)
    else if (classOf[SpecificRecord].isAssignableFrom(clz))
      TypedSchema[C](SpecificData.get.getSchema(clz), clz.getClassLoader)
    else if (classOf[AvroUnion].isAssignableFrom(clz)) {
      // yes, this is disturbing
      val implClassName = clz.getName + "$class"
      val unionClz = Class.forName(implClassName)
      val schema = unionClz.getMethod("getSchema", clz).invoke(null, null).asInstanceOf[Schema]
      TypedSchema[C](schema, unionClz.getClassLoader)
    } else {
      throw new RuntimeException("Don't know how to handle class: " + clz)
    }
  }

  def schemaOf[C <: GenericContainer](implicit s: TypedSchema[C]): Schema = s
}
