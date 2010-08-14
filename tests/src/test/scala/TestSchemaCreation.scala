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

  def test0() {
    val expected = classOf[Test0] ==> "i" ~~> INT_ :: Nil
    SchemaCompare.assertSchemaEquals(expected, classOf[Test0])
  }

  def test1() {
    val test1_unionA = classOf[Test1_UnionA] ==> "i" ~~> INT_ :: Nil
    val test1_unionB = classOf[Test1_UnionB] ==> "i" ~~> STRING_ :: Nil
    val test1 = classOf[Test1] ==> "elems" ~~> (test1_unionA | test1_unionB) :: Nil
    SchemaCompare.assertSchemaEquals(test1_unionA, classOf[Test1_UnionA])
    SchemaCompare.assertSchemaEquals(test1_unionB, classOf[Test1_UnionB])
    SchemaCompare.assertSchemaEquals(test1, classOf[Test1])
  }

  def test2() {
    val test2 = classOf[Test2] ==> "ints" ~~> ARRAY_(BOOLEAN_) :: Nil
    SchemaCompare.assertSchemaEquals(test2, classOf[Test2])
  }

  def test3() {
    val test3 = Helper.mkClassName("Test3Obj.test3_meth0.Test3") ==> "i" ~~> INT_ :: Nil  
    val testObj = Test3Obj.test3_meth0
    SchemaCompare.assertSchemaEquals(test3, testObj.getSchema)
  }

  def test4() {
    val test4 = Helper.mkClassName("Test4Class.test4_meth0.Test4")  ==> "b" ~~> BYTES_ :: Nil
    val test4Obj = new Test4Class
    val testObj = test4Obj.test4_meth0
    SchemaCompare.assertSchemaEquals(test4, testObj.getSchema)
  }

  def test5() {
    val test5Inner = classOf[Test5Inner] ==> "i" ~~> INT_ :: Nil
    val test5Outer = classOf[Test5] ==> "map" ~~> MAP_(test5Inner) :: Nil
    SchemaCompare.assertSchemaEquals(test5Outer, classOf[Test5])
  }

  def test6() {
    val test6 = classOf[Test6] ==> 
      "key0" ~~> STRING_ :: "key1" ~~> DOUBLE_ ::
      "value0" ~~> STRING_ :: "value1" ~~> BYTES_ :: Nil
    SchemaCompare.assertSchemaEquals(test6, classOf[Test6])
  }

  def test7() {
    val test7 = classOf[Test7] ==>
      "opt" ~~> (NULL_ | STRING_) :: Nil
    SchemaCompare.assertSchemaEquals(test7, classOf[Test7])
  }

  def test8() {
    val test8 = classOf[Test8] ==>
      "n0" ~~> INT_ ::
      "n1" ~~> LONG_ ::
      "n2" ~~> FLOAT_ ::
      "n3" ~~> DOUBLE_ :: Nil
    SchemaCompare.assertSchemaEquals(test8, classOf[Test8])
  }

  def test9() {
    val test9_a = classOf[Test9_A] ==>
      "int" ~~> INT_ :: Nil
    val test9_b = classOf[Test9_B] ==>
      "string" ~~> STRING_ :: Nil
    var test9_c = classOf[Test9_C] ==>
      "optBool" ~~> (NULL_ | BOOLEAN_) :: Nil
    val test9 = classOf[Test9] ==>
      "id" ~~> (NULL_ | test9_a | test9_b | test9_c) :: Nil
    SchemaCompare.assertSchemaEquals(test9, classOf[Test9])
  }

  def test10() {
    val test10 = classOf[Test10] ==>
      "optList" ~~> (NULL_ | ARRAY_(INT_)) ::
      "optMap"  ~~> (NULL_ | MAP_(STRING_)) :: Nil
    SchemaCompare.assertSchemaEquals(test10, classOf[Test10])
  }

  def test11() {
    val test11 = classOf[Test11] ==>
      "b" ~~> INT_ ::
      "c" ~~> INT_ ::
      "s" ~~> INT_ :: Nil
    SchemaCompare.assertSchemaEquals(test11, classOf[Test11])
  }

  def test12() {
    val test12 = classOf[Test12] ==>
      "a0" ~~> INT_ ::
      "a1" ~~> STRING_ :: Nil
    SchemaCompare.assertSchemaEquals(test12, classOf[Test12])
  }

  def test13() {
    val test13 = classOf[Test13] ==>
      "a0" ~~> INT_ ::
      "a1" ~~> BOOLEAN_ ::
      "a2" ~~> (NULL_ | STRING_) :: Nil
    SchemaCompare.assertSchemaEquals(test13, classOf[Test13])
  }



}
