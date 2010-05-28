package com.googlecode.avro
package plugin

abstract class ScalaAvroCompilationException(msg: String) extends RuntimeException(msg)

class NonCaseClassException(recordName: String) extends ScalaAvroCompilationException(
        "Implementation limitation: @AvroRecord classes must be case classes: " + recordName) 

class NonSealedClassException(recordName: String) extends ScalaAvroCompilationException(
        "@AvroUnion interfaces must be sealed: " + recordName)

class ImmutableFieldException(fieldName: String) extends ScalaAvroCompilationException(
        "Implemenation limitation: @AvroRecord classes must have mutable fields: " + fieldName)
