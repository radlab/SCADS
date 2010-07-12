package com.googlecode.avro
package marker

import org.apache.avro.{specific, Schema}
import specific.SpecificRecord

import com.googlecode.avro.runtime.ScalaSpecificRecord

private[marker] class NoImplementationException extends Exception("Need to run through compiler plugin")

/** 
 * Provides dummy implementations which are rewritten by the plugin
 */
trait AvroRecord extends ScalaSpecificRecord {

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

