package edu.berkeley.cs
package scads.comm
package test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import avro.runtime._
import avro.marker.{AvroUnion, AvroRecord}


//TODO: unify across tests?
sealed trait DispatchMessage extends AvroUnion
case class DispatchMessage1(var f1: Int) extends DispatchMessage with AvroRecord
case class DispatchMessage2(var f2: Int) extends DispatchMessage with AvroRecord

@RunWith(classOf[JUnitRunner])
class DispatchSpec extends Spec with ShouldMatchers {
  implicit object DispatchRegistry extends ServiceRegistry[DispatchMessage]

  describe("HawtDispatch") {
    it("should receive external messages") {
      val actor = DispatchRegistry.registerActorFunc {
        case Envelope(src, DispatchMessage1(x)) => src.foreach(_ !! DispatchMessage2(x))
      }
      (actor !! DispatchMessage1(1)).get(1000) should equal(Some(DispatchMessage2(1)))
    }
  }
}
