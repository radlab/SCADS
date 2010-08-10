package com.googlecode.avro
package runtime

import com.googlecode.avro.marker._

import org.apache.avro.{generic, specific, Schema}
import generic._
import specific._

object TypedSchemas {
  private[runtime] def findSchema[C <: GenericContainer](implicit r: Manifest[C]): Schema = {
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

  def schemaOf[C <: GenericContainer](implicit s: TypedSchema[C]): Schema = s
}
