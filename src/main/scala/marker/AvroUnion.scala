package com.googlecode.avro
package marker

import org.apache.avro.{ generic, Schema }
import generic.GenericContainer

trait AvroUnion extends GenericContainer {
  def getSchema(): Schema = {
    throw new NoImplementationException
  }
}
