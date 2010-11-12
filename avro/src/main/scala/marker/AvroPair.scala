package edu.berkeley.cs.avro
package marker

import org.apache.avro.generic.GenericRecord

trait AvroPair extends AvroRecord {
  protected def keyImpl: GenericRecord
  protected def valueImpl: GenericRecord
  lazy val key: GenericRecord = keyImpl
  lazy val value: GenericRecord = valueImpl
}
