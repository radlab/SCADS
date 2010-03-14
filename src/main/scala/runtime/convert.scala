package com.googlecode.avro

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.generic.{GenericArray, GenericData}
import org.apache.avro.util.Utf8

import scala.collection.mutable.ListBuffer

import scala.reflect.Manifest

import java.nio.ByteBuffer

trait AvroConversions {

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
                elem => genericArray.add(ByteBuffer.wrap(elem.asInstanceOf[Array[Byte]])) )
            case Type.STRING => list.foreach(
                elem => genericArray.add(new Utf8(elem.asInstanceOf[String])) )
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
