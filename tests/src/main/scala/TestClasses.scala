package com.googlecode.avro
package test

import com.googlecode.avro.marker.{ AvroRecord, AvroUnion }

import java.nio.ByteBuffer

case class Test0(var i: Int) extends AvroRecord

sealed trait Test1_Union extends AvroUnion

case class Test1(var elems: Test1_Union) extends AvroRecord

case class Test1_UnionA(var i: Int) extends AvroRecord with Test1_Union

case class Test1_UnionB(var i: String) extends AvroRecord with Test1_Union

case class Test2(var ints: List[Boolean]) extends AvroRecord

object Test3Obj {
  def test3_meth0 = {
    case class Test3(var i: Int) extends AvroRecord
    Test3(133)
  }
}

class Test4Class {
  def test4_meth0 = {
    case class Test4(var b: ByteBuffer) extends AvroRecord
    Test4(ByteBuffer.wrap("HELLO WORLD".getBytes))    
  }
}

case class Test5(var map: Map[String, Test5Inner]) extends AvroRecord
case class Test5Inner(var i: Int) extends AvroRecord

case class Test6(var key0: String, var key1: Double)(var value0: String, var value1: Array[Byte]) extends AvroRecord

case class Test7(var opt: Option[String]) extends AvroRecord

case class Test8(var n0: Int, var n1: Long, var n2: Float, var n3: Double) extends AvroRecord

sealed trait Test9_Union extends AvroUnion

case class Test9_A(var int: Int) extends AvroRecord with Test9_Union

case class Test9_B(var string: String) extends AvroRecord with Test9_Union

case class Test9_C(var optBool: Option[Boolean]) extends AvroRecord with Test9_Union

case class Test9(var id: Option[Test9_Union]) extends AvroRecord

case class Test10(var optList: Option[List[Int]], var optMap: Option[Map[String, String]]) extends AvroRecord

case class Test11(var b: Byte, var c: Char, var s: Short) extends AvroRecord

case class Test12(var a0: Int) extends AvroRecord {
  var a1: String = _
}

case class Test13(var a0: Int)(var a1: Boolean) extends AvroRecord {
  var a2: Option[String] = None
}
