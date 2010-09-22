package edu.berkeley.cs.avro
package marker

import org.apache.avro.{ generic, Schema }
import generic.GenericContainer

trait AvroUnion extends GenericContainer {
  def getSchema(): Schema = {
    throw new NoImplementationException
  }
}
