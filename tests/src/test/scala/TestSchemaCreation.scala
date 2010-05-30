package com.googlecode.avro
package test

import junit.framework._
import Assert._

import SchemaCompare._
import SchemaDSL._

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

}
