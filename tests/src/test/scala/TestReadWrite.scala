package com.googlecode.avro
package test

import junit.framework._

class TestReadWrite extends TestCase {
  import RecordCompare._

  def test0_rw() {
    assertReadWriteEquals(Test0(-135))
    assertReadWriteEquals(Test0(300000))
  }

  def test1_rw() {
    assertReadWriteEquals(Test1(Test1_UnionA(12)))
    assertReadWriteEquals(Test1(Test1_UnionB("ABCDEF")))
  }

  def test2_rw() {
    assertReadWriteEquals(Test2(Nil))
    assertReadWriteEquals(Test2(List(true, false, true)))
  }

  def test5_rw() {
    assertReadWriteEquals(Test5(Map((1 to 10).map(i => ("key" + i, Test5Inner(i))):_*)))
  }

  def test6_rw() {
    assertReadWriteEquals(Test6("key0", 1002.343)("value0", "value1".getBytes))
  }

  def test7_rw() {
    assertReadWriteEquals(Test7(None))
    assertReadWriteEquals(Test7(Some("MY STRING")))
  }

  def test8_rw() {
    assertReadWriteEquals(Test8(0, 0L, 0.f, 0.0)) 
    assertReadWriteEquals(Test8(1, 2L, 1.332540f, 1200.93290)) 
  }

  def test9_rw() {
    assertReadWriteEquals(Test9(None))
    assertReadWriteEquals(Test9(Some(Test9_A(10))))
    assertReadWriteEquals(Test9(Some(Test9_B("MYID"))))
    assertReadWriteEquals(Test9(Some(Test9_C(None))))
    assertReadWriteEquals(Test9(Some(Test9_C(Some(false)))))
  }

  def test10_rw() {
    assertReadWriteEquals(Test10(None, None))
    assertReadWriteEquals(Test10(Some(List(1,2,3)), None))
    assertReadWriteEquals(Test10(None, Some(Map("a" -> "b", "c" -> "d"))))
    assertReadWriteEquals(Test10(Some(List(5,6,7)), Some(Map("k1" -> "v1"))))
  }

}
