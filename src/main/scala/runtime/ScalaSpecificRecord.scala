package com.googlecode.avro
package runtime

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.generic.{GenericArray, GenericData}
import org.apache.avro.util.Utf8

import scala.collection.mutable.ListBuffer

import scala.reflect.Manifest

import java.nio.ByteBuffer

private[runtime] class UnimplementedFunctionalityException extends RuntimeException("Unimplemented")

trait ScalaSpecificRecord extends SpecificRecord {
  def toBytes: Array[Byte] = {
      throw new UnimplementedFunctionalityException
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

}
