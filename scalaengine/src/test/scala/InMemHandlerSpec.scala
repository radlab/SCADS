package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Spec}
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.AvroRecord
import org.apache.avro.generic._
import org.apache.avro.util.Utf8
import net.lag.logging.Logger
import collection.mutable.HashSet
import edu.berkeley.cs.avro.runtime.ScalaSpecificRecord


/*
 * Just runs all the same tests and RangeKeyValueStoreSpec, but on an in-mem handler.
 */
@RunWith(classOf[JUnitRunner])
class InMemRangeSpec extends RangeKeyValueStoreSpec  {
  override
  def createNamespace[KType <: ScalaSpecificRecord : Manifest,VType <: ScalaSpecificRecord : Manifest](ns: String):SpecificNamespace[KType,VType] = {
    cluster.getInMemoryNamespace[KType,VType](ns)
  }

  //TODO: When we add a GenericInMemoryNamespace override the other createNamespace method
}
