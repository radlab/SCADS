package com.googlecode.avro
package runtime

import org.apache.avro.generic.{GenericData, IndexedRecord}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.util.Utf8

import org.codehaus.jackson.{JsonFactory, JsonParser, JsonToken}

import scala.collection.JavaConversions._

import org.apache.log4j.Logger

trait JsonRecordParser[T <: IndexedRecord] {
  self: RichIndexedRecord[T] => 
  val logger = Logger.getLogger("scads.avro.jsonparser")

  def parseJson(json: String): T = {
    val factory = new JsonFactory
    val parser = factory.createJsonParser(json)

    if(parser.nextToken != JsonToken.START_OBJECT)
      throw new RuntimeException("Expected object for json record")

    parseJson(parser)
  }

  def parseJson(parser: JsonParser): T = {
    val schema = rec.getSchema

    while(parser.nextToken != JsonToken.END_OBJECT) {
      val fieldname = parser.getCurrentName()
      val field = schema.getField(fieldname)
      val valueType = parser.nextToken

      if(field == null) {
        logger.warn("Ignoring missing field: " + fieldname)
      }
      else if(valueType == JsonToken.START_OBJECT) {
        val subRecordSchema: Schema = 
          if(field.schema.getType == Type.RECORD)
            field.schema
          else if(field.schema.getType == Type.UNION)
            field.schema.getTypes.find(_.getType == Type.RECORD).getOrElse(throw new RuntimeException("Unexpected subrecord for field: " + fieldname))
          else
            throw new RuntimeException("Unexpected sub record")

        val className = subRecordSchema.getNamespace + "." + subRecordSchema.getName 
        val subRecordClass = Thread.currentThread.getContextClassLoader.loadClass(className).asInstanceOf[Class[IndexedRecord]]
        val subRecord = subRecordClass.newInstance()

        subRecord.parseJson(parser)
        rec.put(field.pos, subRecord)
      } else if(valueType == JsonToken.START_ARRAY) {
        val array = new GenericData.Array[Any](1, field.schema)
        while(parser.nextToken == JsonToken.END_ARRAY) {
          parser.getCurrentToken match {
            case JsonToken.VALUE_NUMBER_FLOAT => array.add(parser.getDoubleValue)
          }
        }

        rec.put(field.pos, array)
      } else  {
        /* Helper function to test if a given schema (union or otherwise) can represent a specific type */
        def canBe(fieldType: Schema.Type): Boolean = {
          if(fieldType == field.schema.getType) true
          else if(field.schema.getType == Type.UNION && field.schema.getTypes.find(_.getType == fieldType).isDefined) true
          else false
        }

        valueType match {
          case JsonToken.VALUE_STRING     if(canBe(Type.STRING)) =>
            rec.put(field.pos, new Utf8(parser.getText))
          case JsonToken.VALUE_NUMBER_INT if(canBe(Type.LONG)) => 
            rec.put(field.pos, parser.getLongValue)
          case JsonToken.VALUE_NUMBER_INT if(canBe(Type.INT)) =>
            rec.put(field.pos, parser.getIntValue)
          case JsonToken.VALUE_TRUE       if(canBe(Type.BOOLEAN)) => 
            rec.put(field.pos, true)
          case JsonToken.VALUE_FALSE      if(canBe(Type.BOOLEAN)) => 
            rec.put(field.pos, true)
          case JsonToken.VALUE_NULL       if(canBe(Type.NULL)) =>
            rec.put(field.pos, null)
          case unexp => logger.warn("Don't know how to populate field '" + fieldname + "', found: " + valueType + " expected: " + field.schema)
        }
      }
    }

    return rec
  }
}
