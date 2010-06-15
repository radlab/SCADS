package com.googlecode.avro
package test

import com.googlecode.avro.marker.AvroRecord
import com.googlecode.avro.annotation.AvroUnion

import java.nio.ByteBuffer

case class Test0(var i: Int) extends AvroRecord

@AvroUnion
sealed trait Test1_Union

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
