package com.googlecode.avro
package marker

import org.apache.avro.{specific, Schema}
import specific.SpecificRecord

private[marker] class NoImplementationException extends Exception("Need to run through compiler plugin")

trait AvroRecord extends SpecificRecord {

  def get(i: Int): AnyRef = {
    throw new NoImplementationException
  }

  def put(i: Int, v: Any) {
    throw new NoImplementationException
  }

  def getSchema(): Schema = {
    throw new NoImplementationException
  }

}

