package com.googlecode.avro

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.generic.{GenericArray, GenericData}

import scala.collection.mutable.ListBuffer

import scala.reflect.Manifest

trait AvroConversions {

    private val primitiveClasses = Map[Class[_],Schema](
        classOf[Int]         -> Schema.create(Type.INT),
        classOf[Long]        -> Schema.create(Type.LONG),
        classOf[Double]      -> Schema.create(Type.DOUBLE),
        classOf[Float]       -> Schema.create(Type.FLOAT),
        classOf[Boolean]     -> Schema.create(Type.BOOLEAN),
        classOf[Array[Byte]] -> Schema.create(Type.BYTES)
    )

    def scalaListToGenericArray[T](list: List[T])(implicit manifest: Manifest[T]): GenericArray[T] = {
        val tClass = manifest.erasure.asInstanceOf[Class[T]]
        val schema =
            if (primitiveClasses.contains(tClass)) 
                primitiveClasses.get(tClass).get
            else if (classOf[SpecificRecord].isAssignableFrom(tClass))
                tClass.asInstanceOf[Class[SpecificRecord]].newInstance.getSchema
            else
                throw new UnsupportedOperationException("Cannot handle: " + tClass)
        val genericArray = new GenericData.Array[T](10, Schema.createArray(schema))
        list.foreach( genericArray.add(_) )
        genericArray
    }

    def genericArrayToScalaList[T](genericArray: GenericArray[T]): List[T] = {
        val listBuffer = new ListBuffer[T]
        val iter = genericArray.iterator
        while (iter.hasNext) listBuffer += iter.next
        listBuffer.toList
    }

}
