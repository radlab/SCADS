package edu.berkeley.cs.scads.test
import org.scalatest.WordSpec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import edu.berkeley.cs.scads.storage.RTable

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class RangeTableSpec extends WordSpec with ShouldMatchers {
"A Range Table" should {
    var rTable : RTable[Int, String] = new RTable[Int, String](List("S6"), (a: Int, b: Int) => if(a == b) 0 else if (a<b) -1 else 1)

    "return servers for a key" in {
      rTable.valuesForKey(10) should be === List("S6")
      rTable.valuesForKey(30) should be === List("S6")
      rTable.valuesForKey(100) should be === List("S6")
    }

    "split partitions left attached" in {
      rTable.split(20, List("S1"))
      var rList = rTable.ranges
      rTable.ranges.size should be === 2
      rTable.split(40, List("S3"))
      rTable.ranges.size should be === 3
      rTable.split(50, List("S5"))
      rTable.ranges.size should be === 4

      rTable.valuesForKey(5) should be === List("S1")
      rTable.valuesForKey(35) should be === List("S3")
      rTable.valuesForKey(45) should be === List("S5")
    }

    "split partitions right attached" in {
      rTable.split(10, List("S2"), false)
      rTable.ranges.size should be === 5
      rTable.split(30, List("S4"), false)
      rTable.ranges.size should be === 6
      rTable.split(60, List("S7"), false)
      rTable.ranges.size should be === 7

      rTable.valuesForKey(5) should be === List("S1")
      rTable.valuesForKey(15) should be === List("S2")
      rTable.valuesForKey(25) should be === List("S3")
      rTable.valuesForKey(35) should be === List("S4")
      rTable.valuesForKey(45) should be === List("S5")
      rTable.valuesForKey(55) should be === List("S6")
      rTable.valuesForKey(65) should be === List("S7")
    }

    "throw an error if the split key already exists" in {
      intercept[IllegalArgumentException] {
        rTable.split(30, List("S10"), false)
      }
    }


    "return the left range for split keys" in {
      rTable.valuesForKey(10) should be === List("S1")
      rTable.valuesForKey(20) should be === List("S2")
      rTable.valuesForKey(60) should be === List("S6")
    }

    "allow to add values" in {
      rTable.add(9, "S10")
      rTable.add(10, "S11")
      rTable.add(35, "S30")

      rTable.valuesForKey(5) should be === List("S11", "S10", "S1")
      rTable.valuesForKey(15) should be === List("S2")
      rTable.valuesForKey(25) should be === List("S3")
      rTable.valuesForKey(35) should be === List("S30", "S4")
      rTable.valuesForKey(45) should be === List("S5")
      rTable.valuesForKey(55) should be === List("S6")
      rTable.valuesForKey(65) should be === List("S7")

    }

    "prevent adding the same value" in {
      intercept[IllegalArgumentException] {
        rTable.add(10, "S11")
      }
    }

    "delete values" in {
      rTable.remove(9, "S1")
      rTable.remove(10, "S10")
      rTable.remove(35, "S30")
      rTable.valuesForKey(5) should be === List("S11")
      rTable.valuesForKey(15) should be === List("S2")
      rTable.valuesForKey(25) should be === List("S3")
      rTable.valuesForKey(35) should be === List("S4")
      rTable.valuesForKey(45) should be === List("S5")
      rTable.valuesForKey(55) should be === List("S6")
      rTable.valuesForKey(65) should be === List("S7")
    }

    "prevent deleting the last value in a range" in {
      intercept[RuntimeException] {
        rTable.remove(30, "S4")
      }
    }

    "merge partitions and delete left range nodes" in {
      rTable.merge(30)
      rTable.merge(10)
      rTable.merge(60)
      rTable.valuesForKey(5) should be === List("S2")
      rTable.valuesForKey(15) should be === List("S2")
      rTable.valuesForKey(25) should be === List("S4")
      rTable.valuesForKey(35) should be === List("S4")
      rTable.valuesForKey(45) should be === List("S5")
      rTable.valuesForKey(55) should be === List("S7")
      rTable.valuesForKey(65) should be === List("S7")
    }

     "throw an error if the key is not a split key" in {
       intercept[IllegalArgumentException] {
          rTable.merge(35)
       }
     }

    "merge partitions and delete right range nodes" in {
      rTable.merge(20, false)
      rTable.merge(50, false)
      rTable.valuesForKey(15) should be === List("S2")
      rTable.valuesForKey(25) should be === List("S2")
      rTable.valuesForKey(35) should be === List("S2")
      rTable.valuesForKey(45) should be === List("S5")
      rTable.valuesForKey(55) should be === List("S5")
    }

    "create a table from a serialized object" is (pending)
  }
}