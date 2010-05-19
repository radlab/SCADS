package edu.berkeley.cs.scads.avro.compiler

import java.util.{Map => JMap}
import scala.collection.{Map => MapIface}
import scala.collection.mutable.{Map => MMap}
import org.apache.avro.generic.{GenericData,GenericArray}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type

import scala.collection.JavaConversions._

abstract class PrimitiveWrapper[T](var value: T) 

trait UnionInterface

object ScalaLib {
    def convertListToGenericArray[T](list: List[T], schema: Schema): GenericArray[T] = {
        if (list == null) return null
        val g = new GenericData.Array[T](10, schema)
        list.foreach(g.add(_))
        g
    }

    def convertGenericArrayToList[T](genericArray: GenericArray[T]):List[T] = {
        if (genericArray == null) return null
        val m = new scala.collection.mutable.ListBuffer[T]
        val iter = genericArray.iterator
        while(iter.hasNext)
            m += iter.next
        m.toList
    }

    def convertJMapToMap[T](jmap: JMap[String, T]): MapIface[String, T] = {
        if (jmap == null) return null
        asMap(jmap)
    }

    def convertMapToJMap[T](map: MapIface[String, T]): JMap[String, T] = {
        if (map == null) return null
        val m = new java.util.HashMap[String,T]
        map.foreach(entry => m.put(entry._1, entry._2))
        m
    }

    def unwrapUnion(union: UnionInterface, unionSchema: Schema):Object = union match {
        case null => null
        case w: PrimitiveWrapper[_] => w.value match {
            case null => null
            case map: MapIface[_,_] => convertMapToJMap(map.asInstanceOf[MapIface[String,_]])
            case list: List[_] => convertListToGenericArray(list, 
                asList(unionSchema.getTypes).toList.filter(_.getType == Type.ARRAY).apply(0))
            case obj: Object => obj
        }
        case o: Object => o
    }
}
