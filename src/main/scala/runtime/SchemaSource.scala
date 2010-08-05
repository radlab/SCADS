package com.googlecode.avro
package runtime

import com.googlecode.avro.marker._

import org.apache.avro._
import generic._
import specific._

trait SchemaSource[R] {
  def getSchema(): Schema
}

object SchemaSource {
  implicit def genericContainerSchemaSource[C <: GenericContainer](implicit r: Manifest[C]): SchemaSource[C] = 
    new SchemaSource[C] {
      override def getSchema() = {
        val clz = r.erasure.asInstanceOf[Class[C]]
        if (classOf[AvroRecord].isAssignableFrom(clz))
          clz.newInstance().getSchema
        else if (classOf[SpecificRecord].isAssignableFrom(clz))
          SpecificData.get.getSchema(clz)
        else if (classOf[AvroUnion].isAssignableFrom(clz)) {
          // yes, this is disturbing
          val implClassName = clz.getName + "$class"
          Class.forName(implClassName)
            .getMethod("getSchema", clz)
            .invoke(null, null).asInstanceOf[Schema]
        } else {
          throw new RuntimeException("Don't know how to handle class: " + clz)
        }
      }
    }
}

