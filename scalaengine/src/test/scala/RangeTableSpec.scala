package edu.berkeley.cs.scads.test
import org.scalatest.WordSpec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import edu.berkeley.cs.scads.storage.routing.RangeTable
import edu.berkeley.cs.scads.storage.routing.RangeType

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class RangeTableSpec extends WordSpec with ShouldMatchers {

"A Range Table" should {
    var rTable : RangeTable[Int, String] = new RangeTable[Int, String](List((None, List("S6"))),
      (a: Int, b: Int) => if(a == b) 0 else if (a<b) -1 else 1,
      (a : List[String], b : List[String])  => true)

    "return values for a key" in {
      rTable.valuesForKey(10) should be === List("S6")
      rTable.valuesForKey(30) should be === List("S6")
      rTable.valuesForKey(100) should be === List("S6")
    }

    "split ranges left attached" in {
      rTable = rTable.split(20, List("S1"))
      var rList = rTable.ranges
      rTable.ranges.size should be === 2
      rTable = rTable.split(40, List("S3"))
      rTable.ranges.size should be === 3
      rTable = rTable.split(50, List("S5"))
      rTable.ranges.size should be === 4

      rTable.valuesForKey(5) should be === List("S1")
      rTable.valuesForKey(35) should be === List("S3")
      rTable.valuesForKey(45) should be === List("S5")
    }

    "split ranges right attached" in {
      rTable = rTable.split(10, List("S2"), false)
      rTable.ranges.size should be === 5
      rTable = rTable.split(30, List("S4"), false)
      rTable.ranges.size should be === 6
      rTable = rTable.split(60, List("S7"), false)
      rTable.ranges.size should be === 7

      rTable.valuesForKey(5) should be === List("S1")
      rTable.valuesForKey(15) should be === List("S2")
      rTable.valuesForKey(25) should be === List("S3")
      rTable.valuesForKey(35) should be === List("S4")
      rTable.valuesForKey(45) should be === List("S5")
      rTable.valuesForKey(55) should be === List("S6")
      rTable.valuesForKey(65) should be === List("S7")
    }


    "return values for ranges" in {
      val r1 = rTable.valuesForRange(None, Option(25))
      transformRangeArray(r1) should be === List((Some(10),List("S1")), (Some(20),List("S2")), (Some(30),List("S3")))
      val r2 = rTable.valuesForRange(Some(0), Some(15))
      transformRangeArray(r2) should be === List((Some(10),List("S1")), (Some(20),List("S2")))
      val r3 = rTable.valuesForRange(Some(30), Some(45))
      transformRangeArray(r3) should be === List((Some(30),List("S3")), (Some(40),List("S4")), (Some(50),List("S5")))
      val r4 = rTable.valuesForRange(Some(45), Some(70))
      transformRangeArray(r4) should be === List((Some(50),List("S5")), (Some(60),List("S6")), (None, List("S7")))
      val r5 = rTable.valuesForRange(Some(55), None)
      transformRangeArray(r5) should be === List((Some(60),List("S6")), (None, List("S7")))

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
      rTable.addValueToRange(9, "S10")
      rTable.addValueToRange(10, "S11")
      rTable.addValueToRange(35, "S30")
      rTable.addValueToRange(None, "S100")

      rTable.valuesForKey(5) should be === List("S11", "S10", "S1")
      rTable.valuesForKey(15) should be === List("S2")
      rTable.valuesForKey(25) should be === List("S3")
      rTable.valuesForKey(35) should be === List("S30", "S4")
      rTable.valuesForKey(45) should be === List("S5")
      rTable.valuesForKey(55) should be === List("S6")
      rTable.valuesForKey(65) should be === List("S100", "S7")

    }

    "prevent adding the same value" in {
      intercept[IllegalArgumentException] {
        rTable.addValueToRange(10, "S11")
      }
    }

    "delete values" in {
      rTable.removeValueFromRange(9, "S1")
      rTable.removeValueFromRange(10, "S10")
      rTable.removeValueFromRange(35, "S30")
      rTable.removeValueFromRange(None, "S100")
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
        rTable.removeValueFromRange(30, "S4")
      }
    }

    "merge range and delete left range values" in {
      rTable = rTable.merge(30)
      rTable = rTable.merge(10)
      rTable = rTable.merge(60)
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

    "merge ranges and delete right range values" in {
      rTable = rTable.merge(20, false)
      rTable = rTable.merge(50, false)
      rTable.valuesForKey(15) should be === List("S2")
      rTable.valuesForKey(25) should be === List("S2")
      rTable.valuesForKey(35) should be === List("S2")
      rTable.valuesForKey(45) should be === List("S5")
      rTable.valuesForKey(55) should be === List("S5")
    }

    "merge ranges based on a merge condition" in {
      var rangeTable : RangeTable[Int, String] = new RangeTable[Int, String](
        List((None, List("S1"))),
        (a: Int, b: Int) => if(a == b) 0 else if (a<b) -1 else 1,
        (a : List[String], b : List[String]) => a.corresponds(b)(_.compareTo(_) == 0))
      rangeTable = rangeTable.split(10, List("S1"))
      rangeTable = rangeTable.split(20, List("S1"))

      rangeTable.addValueToRange(10, "S2")
      rangeTable.addValueToRange(20, "S3")

      rangeTable.merge(10) should be === null
      rangeTable.merge(20) should be === null

      rangeTable.valuesForKey(5) should be === List("S2", "S1")
      rangeTable.valuesForKey(15) should be === List("S3", "S1")
      rangeTable.valuesForKey(25) should be === List("S1")

      rangeTable.addValueToRange(10, "S3")
      rangeTable.removeValueFromRange(10, "S2")
      rangeTable = rangeTable.merge(10)
      rangeTable should not be === (null)

      rangeTable.valuesForKey(5) should be === List("S3", "S1")
      rangeTable.valuesForKey(15) should be === List("S3", "S1")
      rangeTable.valuesForKey(25) should be === List("S1")

      rangeTable.merge(20) should be === (null)

      rangeTable.removeValueFromRange(20, "S3")

      rangeTable = rangeTable.merge(20)
      rangeTable should not be (null)

      rangeTable.valuesForKey(5) should be === List("S1")
      rangeTable.valuesForKey(15) should be === List("S1")
      rangeTable.valuesForKey(25) should be === List("S1")
    }

  }

   def transformRangeArray(rangeTable: Array[RangeType[Int, String]]) : List[(Option[Int], List[String])] = {
    for(item <- rangeTable.toList)
        yield  (item.maxKey, item.values)
   }


}
