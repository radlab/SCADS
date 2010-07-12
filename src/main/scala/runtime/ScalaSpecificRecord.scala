package com.googlecode.avro
package runtime

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.io.{BinaryEncoder, BinaryDecoder, DecoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}
import org.apache.avro.generic.{GenericArray, GenericData}
import org.apache.avro.util.Utf8

import scala.collection.mutable.{Map => MMap, ListBuffer}
import scala.collection.immutable.{Map => ImmMap}
import scala.collection.JavaConversions._
import scala.reflect.Manifest

import java.nio.ByteBuffer
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.{Map => JMap, HashMap => JHashMap}

private[runtime] class UnimplementedFunctionalityException extends RuntimeException("Unimplemented")

trait ScalaSpecificRecord extends SpecificRecord {
  def toBytes: Array[Byte] = {
    val out = new ByteArrayOutputStream
    val enc = new BinaryEncoder(out)
    val writer = new SpecificDatumWriter[ScalaSpecificRecord](getSchema)
    writer.write(this, enc)
    out.toByteArray
  }

  def parse(data: Array[Byte]): Unit = {
    val stream = new ByteArrayInputStream(data)
    val factory = new DecoderFactory
    val dec = factory.createBinaryDecoder(stream, null) // new decoder
    val reader = new SpecificDatumReader[this.type](getSchema());
    reader.read(this, dec)
  }
}

trait AvroConversions {

  def mkUtf8(p: String): Utf8 = 
    if (p eq null)
      null
    else
      new Utf8(p)

  def mkByteBuffer(bytes: Array[Byte]): ByteBuffer = 
    if (bytes eq null)
      null
    else
      ByteBuffer.wrap(bytes)

  def scalaMapToJMap(map: Map[_,_], schema: Schema): JMap[_,_] = {
    if (schema eq null)
      throw new IllegalArgumentException("schema must not be null")
    if (map eq null)
      return null.asInstanceOf[JMap[_,_]]
      
    val jHashMap = new JHashMap[Any,Any]
    map.foreach(kv => {
      val (key, value0) = kv
      val value = schema.getValueType.getType match {
        case Type.ARRAY => 
          scalaListToGenericArray(value0.asInstanceOf[List[_]], schema.getValueType)          
        case Type.MAP =>
          scalaMapToJMap(value0.asInstanceOf[Map[_,_]], schema.getValueType)
        case Type.STRING =>
          // assume String -> Utf8 for now
          new Utf8(value0.asInstanceOf[String])
        case Type.BYTES =>
          // assume Array[Byte] -> ByteBuffer for now
          ByteBuffer.wrap(value0.asInstanceOf[Array[Byte]])
        case _ => value0
      }
      // assume that key is String
      jHashMap.put(new Utf8(key.toString), value)
    })
    jHashMap
  }

  def jMapToScalaMap(jmap: JMap[_,_], schema: Schema): Map[_,_] = {
    if (schema eq null)
      throw new IllegalArgumentException("schema must not be null")
    if (jmap eq null)
      return null.asInstanceOf[Map[_,_]]

    val mMap = MMap[Any,Any]()
    val iter = jmap.keySet.iterator
    while (iter.hasNext) {
      val key = iter.next
      val value0 = jmap.get(key)
      val value = schema.getValueType.getType match {
        case Type.ARRAY =>
          genericArrayToScalaList(value0.asInstanceOf[GenericArray[_]])
        case Type.MAP =>
          jMapToScalaMap(value0.asInstanceOf[JMap[_,_]], schema.getValueType)
        case Type.STRING =>
          // assume Utf8 -> String for now
          value0.asInstanceOf[Utf8].toString
        case Type.BYTES =>
          // assume ByteBuffer -> Array[Byte] for now
          value0.asInstanceOf[ByteBuffer].array
        case _ => value0
      }
      // assume that key is Utf8
      mMap += key.asInstanceOf[Utf8].toString -> value
    }
    ImmMap[Any,Any](mMap.toList:_*)
  }

  /** NOTE: 
    * For the runtime, to get a list of byte buffers, you MUST do
    * List[ByteBuffer]. To get a list of strings you MUST do
    * List[String]. List[Array[Byte]] and List[Utf8] will NOT work. 
    * TODO: we'll have to resolve this! basically these two methods will
    * need to take a paramater which indicates how to resolve these issues.
    */

  /**
    * We can't do type safe lists here because the types can change based on runtime types.
    * For example: 
    * 1) List[Array[Byte]] -> GenericArray[ByteBuffer]
    * 2) List[List[Foo]] -> GenericArray[GenericArray[Foo]]
    */
  def scalaListToGenericArray(list: List[_], schema: Schema): GenericArray[_] = {
      if (list == null) return null
      val genericArray = new GenericData.Array[Any](10, schema)
      if (schema.getType != Type.ARRAY)
          throw new IllegalArgumentException("Not array type schema")        
      schema.getElementType.getType match {
          case Type.ARRAY => list.foreach( 
              elem => genericArray.add(scalaListToGenericArray(elem.asInstanceOf[List[_]], schema.getElementType)) )
          case Type.BYTES => list.foreach( 
              elem => 
                  //if (elem.isInstanceOf[Array[Byte]])
                  //    genericArray.add(ByteBuffer.wrap(elem.asInstanceOf[Array[Byte]])) 
                  //else
                      genericArray.add(elem.asInstanceOf[ByteBuffer])
              )
          case Type.STRING => list.foreach(
              elem => 
                  //if (elem.isInstanceOf[String])
                      genericArray.add(new Utf8(elem.asInstanceOf[String])) 
                  //else
                  //    genericArray.add(elem.asInstanceOf[Utf8])
              )
          case _ => list.foreach( elem => genericArray.add(elem) )
      }
      genericArray
  }

  def genericArrayToScalaList(genericArray: GenericArray[_]): List[_] = {
      if (genericArray eq null)
        return null
      val listBuffer = new ListBuffer[Any]
      val iter = genericArray.iterator
      while (iter.hasNext) {
          val next = iter.next
          if (next.isInstanceOf[GenericArray[_]])
              listBuffer += genericArrayToScalaList(next.asInstanceOf[GenericArray[_]])
          else if (next.isInstanceOf[ByteBuffer])
              listBuffer += next.asInstanceOf[ByteBuffer].array
          else if (next.isInstanceOf[Utf8]) 
              listBuffer += next.asInstanceOf[Utf8].toString
          else 
              listBuffer += next
      }
      listBuffer.toList
  }

  def castToGenericArray(obj: Any): GenericArray[_] = obj.asInstanceOf[GenericArray[_]]

  def castToJMap(obj: Any): JMap[_,_] = obj.asInstanceOf[JMap[_,_]]

  private def findOnceInUnion(schema: Schema)(test: Schema => Boolean): Schema = schema.getType match {
    case Type.UNION =>
      val found = schema.getTypes.filter(test)
      if (found.size != 1)
        throw new IllegalArgumentException("Found element " + found.size + "times")
      found.head
    case t =>
      throw new IllegalArgumentException("Bad type given: " + t)
  }

  private def findListInUnion(schema: Schema) = 
    findOnceInUnion(schema)(_.getType == Type.ARRAY)

  private def findMapInUnion(schema: Schema) = 
    findOnceInUnion(schema)(_.getType == Type.MAP)

  def unwrapOption(opt: Option[_], schema: Schema): AnyRef = opt match {
    case Some(inner) =>
      // NOTE: we don't have to worry about another nested union schema, since avro
      // disallowes nested unions
      inner match {
        case s: String      => new Utf8(s)
        case u: Utf8        => u
        case l: List[_]     => 
          scalaListToGenericArray(l, findListInUnion(schema))
        case m: Map[_,_]    =>
          scalaMapToJMap(m, findMapInUnion(schema))
        case b: ByteBuffer  => b
        case a: Array[Byte] => ByteBuffer.wrap(a)
        case _              => inner.asInstanceOf[AnyRef]
      }
    case None | null =>
      null.asInstanceOf[AnyRef]
  }
   
  def wrapOption(obj: Any, schema: Schema, returnNativeType: Boolean): Option[_] = {
    if (obj.asInstanceOf[AnyRef] eq null)
      None
    else {
      val obj0 = if (returnNativeType) obj else obj match {
        case u: Utf8            => u.toString
        case a: GenericArray[_] => 
          genericArrayToScalaList(a)
        case b: ByteBuffer      => b.array
        case m: JMap[_,_]       =>
          jMapToScalaMap(m, findMapInUnion(schema))
        case _ => obj
      }
      Some(obj0)
    }
  }

}
