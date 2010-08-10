package com.googlecode.avro

import org.apache.avro.generic.GenericContainer

package object runtime {
  type TypedSchema[T] = org.apache.avro.TypedSchema[T]
  implicit def genericContainerSchema[C <: GenericContainer](implicit r: Manifest[C]): TypedSchema[C] = {
    val schema = TypedSchemas.findSchema[C]
    new TypedSchema[C](schema)
  }
}
