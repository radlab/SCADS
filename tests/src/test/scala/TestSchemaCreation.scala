package com.googlecode.avro
package test

import junit.framework._
import Assert._

import SchemaCompare._
import SchemaDSL._

object Helper {
  val PREFIX = "com.googlecode.avro.test"
  def mkClassName(c: String) = PREFIX + "." + c
}

class TestSchemaCreation extends TestCase {

  def test0() = {
    val expected = classOf[Test0] ==> "i" ~~> INT_ :: Nil
    SchemaCompare.assertSchemaEquals(expected, classOf[Test0])
  }

  def test1() = {
    val test1_unionA = classOf[Test1_UnionA] ==> "i" ~~> INT_ :: Nil
    val test1_unionB = classOf[Test1_UnionB] ==> "i" ~~> STRING_ :: Nil
    val test1 = classOf[Test1] ==> "elems" ~~> (test1_unionA | test1_unionB) :: Nil
    SchemaCompare.assertSchemaEquals(test1_unionA, classOf[Test1_UnionA])
    SchemaCompare.assertSchemaEquals(test1_unionB, classOf[Test1_UnionB])
    SchemaCompare.assertSchemaEquals(test1, classOf[Test1])
  }

  def test2() = {
    val test2 = classOf[Test2] ==> "ints" ~~> ARRAY_(BOOLEAN_) :: Nil
    SchemaCompare.assertSchemaEquals(test2, classOf[Test2])
  }

  def test3() = {
    val test3 = Helper.mkClassName("Test3Obj.test3_meth0.Test3") ==> "i" ~~> INT_ :: Nil  
    val testObj = Test3Obj.test3_meth0
    SchemaCompare.assertSchemaEquals(test3, testObj.getSchema)
  }

  def test4() = {
    val test4 = Helper.mkClassName("Test4Class.test4_meth0.Test4")  ==> "b" ~~> BYTES_ :: Nil
    val test4Obj = new Test4Class
    val testObj = test4Obj.test4_meth0
    SchemaCompare.assertSchemaEquals(test4, testObj.getSchema)
  }

  def test5() = {
    val test5Inner = classOf[Test5Inner] ==> "i" ~~> INT_ :: Nil
    val test5Outer = classOf[Test5] ==> "map" ~~> MAP_(test5Inner) :: Nil
    SchemaCompare.assertSchemaEquals(test5Outer, classOf[Test5])
  }

  def test6() = {
    val test6 = classOf[Test6] ==> 
      "key0" ~~> STRING_ :: "key1" ~~> DOUBLE_ ::
      "value0" ~~> STRING_ :: "value1" ~~> BYTES_ :: Nil
    SchemaCompare.assertSchemaEquals(test6, classOf[Test6])
  }

}
