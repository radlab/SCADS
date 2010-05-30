package com.googlecode.avro
package test

import com.googlecode.avro.annotation._

@AvroRecord
case class Test0(var i: Int)

@AvroUnion
sealed trait Test1_Union

@AvroRecord
case class Test1(var elems: Test1_Union)

@AvroRecord
case class Test1_UnionA(var i: Int) extends Test1_Union

@AvroRecord
case class Test1_UnionB(var i: String) extends Test1_Union

@AvroRecord
case class Test2(var ints: List[Boolean])
