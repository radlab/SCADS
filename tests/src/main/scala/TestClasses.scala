package com.googlecode.avro
package test

import com.googlecode.avro.marker.AvroRecord
import com.googlecode.avro.annotation.AvroUnion

case class Test0(var i: Int) extends AvroRecord

@AvroUnion
sealed trait Test1_Union

case class Test1(var elems: Test1_Union) extends AvroRecord

case class Test1_UnionA(var i: Int) extends AvroRecord with Test1_Union

case class Test1_UnionB(var i: String) extends AvroRecord with Test1_Union

case class Test2(var ints: List[Boolean]) extends AvroRecord

object Test3Obj {
  def test3_meth0 {
    case class Test3(var i: Int) extends AvroRecord
  }
}
