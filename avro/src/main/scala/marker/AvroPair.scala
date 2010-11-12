package edu.berkeley.cs.avro
package marker

import java.lang.RuntimeException
import org.apache.avro.generic.GenericRecord

trait AvroPair extends AvroRecord {
  protected def keyImpl: GenericRecord = throw new RuntimeException("Unimplemented")
  protected def valueImpl: GenericRecord = throw new RuntimeException("Unimplemented")
  lazy val key: GenericRecord = keyImpl
  lazy val value: GenericRecord = valueImpl
}
