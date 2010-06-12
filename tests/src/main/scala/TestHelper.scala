package com.googlecode.avro
package test

import org.apache.avro.Schema
import Schema.{ Field, Type }

import org.apache.avro.io._
import org.apache.avro.specific._
import org.apache.avro.generic.GenericContainer

import java.util.{ Arrays => JArrays, List => JList }
import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }
import scala.reflect.Manifest

import junit.framework._
import Assert._

object SchemaCompare {

  def extractSchema[T <: GenericContainer](clazz: Class[T]): Schema = {
    clazz.newInstance.getSchema
  }

  def assertSchemaEquals[T <: GenericContainer](expected: Schema, actual: Class[T]) {
    assertSchemaEquals(expected, extractSchema(actual))
  }

  def assertSchemaEquals(expected: Schema, actual: Schema) {
    println("expected: " + expected)
    println("actual: " + actual)
    Assert.assertEquals(expected, actual)
  }

}

object RecordCompare {

  def toBytes[T <: SpecificRecord](obj: T): Array[Byte] = {
    val writer = new SpecificDatumWriter[T](obj.getSchema)
    val buffer = new ByteArrayOutputStream(1024)
    val encoder = new BinaryEncoder(buffer)
    writer.write(obj, encoder) 
    buffer.toByteArray
  }

  def fromBytes[T <: SpecificRecord](bytes: Array[Byte], clz: Class[T]): T = {
    val decoderFactory = new DecoderFactory
    val reader = new SpecificDatumReader[T](SchemaCompare.extractSchema(clz))
    val inStream = decoderFactory.createBinaryDecoder(bytes, null)
    val newInstance = clz.newInstance 
    reader.read(newInstance, inStream)
    newInstance
  }

  def assertReadWriteEquals[T <: SpecificRecord](record: T)(implicit manifest: Manifest[T]) {
    println("orig record: " + record)
    val bytes = toBytes(record)
    /** Type inference requires explicit T here to infer T */
    val copy: T = fromBytes(bytes, manifest.erasure.asInstanceOf[Class[T]])
    println("new record: " + record)
    Assert.assertEquals(record, copy)
  }

}

/**
 * Small DSL to generate schemas programatically w/o the verboseness
 */
object SchemaDSL {

  class SchemaField(val name: String, val schema: SchemaSchema) {
    def toField: Field = {
      new Field(name, schema.toSchema, "", null)
    }
  }

  abstract trait SchemaSchema {
    def toSchema: Schema
  }

  abstract class SchemaPrimitive(val tpe: Type) extends SchemaSchema {
    def toSchema = Schema.create(tpe)
  }

  class SchemaRecord(val name: String, val namespace: String, val fields: List[SchemaField]) extends SchemaSchema {
    def toSchema = {
      val schema = Schema.createRecord(name, "", namespace, false)
      schema.setFields(JArrays.asList(fields.map(_.toField).toArray:_*))
      schema
    }
  }

  class SchemaUnion(val schemas: List[SchemaSchema]) extends SchemaSchema {
    def toSchema = {
      Schema.createUnion(JArrays.asList(schemas.map(_.toSchema).toArray:_*))
    }
  }

  class SchemaArray(val schema: SchemaSchema) extends SchemaSchema {
    def toSchema = {
      Schema.createArray(schema.toSchema)
    }
  }

  class SchemaMap(val schema: SchemaSchema) extends SchemaSchema {
    def toSchema = {
      Schema.createMap(schema.toSchema)
    }
  }

  object INT_ extends SchemaPrimitive(Type.INT)
  object LONG_ extends SchemaPrimitive(Type.LONG)
  object FLOAT_ extends SchemaPrimitive(Type.FLOAT)
  object DOUBLE_ extends SchemaPrimitive(Type.DOUBLE)
  object BOOLEAN_ extends SchemaPrimitive(Type.BOOLEAN)
  object BYTES_ extends SchemaPrimitive(Type.BYTES)
  object STRING_ extends SchemaPrimitive(Type.STRING)
  object NULL_ extends SchemaPrimitive(Type.NULL)

  object ARRAY_ {
    def apply(schema: SchemaSchema) = new SchemaArray(schema)
  }

  object MAP_ {
    def apply(schema: SchemaSchema) = new SchemaMap(schema)
  }

  final class DoubleArrow(val name: String) {
    def ~~>(schema: SchemaSchema) = new SchemaField(name, schema)
    def ==>(fields: List[SchemaField]) = new SchemaRecord(mkNameAndNamespace._1, mkNameAndNamespace._2, fields)
    private def mkNameAndNamespace = {
      val idx = name.lastIndexOf('.')
      if (idx == -1)
        (name, "")
      else
        (name.substring(idx + 1), name.substring(0, idx))
    }
  }

  final class OrWrapper(val schema: SchemaSchema) {
    var schemas = List(schema)
    def |(schema: SchemaSchema): OrWrapper = {
      schemas = schemas ::: List(schema) 
      this
    }
  }


  private def class2string(clazz: Class[_]) = clazz.getName
  implicit def class2DoubleArrow(clazz: Class[_]) = new DoubleArrow(class2string(clazz))
  implicit def string2DoubleArrow(name: String) = new DoubleArrow(name)
  implicit def schemaRecord2Schema(rec: SchemaRecord) = rec.toSchema
  implicit def schemaSchema2OrWrapper(schema: SchemaSchema) = new OrWrapper(schema)
  implicit def orWrapper2SchemaSchema(orWrapper: OrWrapper) = new SchemaUnion(orWrapper.schemas)
  implicit def schemaSchema2Schema(schema: SchemaSchema) = schema.toSchema

}
