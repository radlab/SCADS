package edu.berkeley.cs.scads.test
import org.scalatest.WordSpec
import edu.berkeley.cs.scads.util.{RangeTable, RangeType}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest.WordSpec
import org.scalatest.matchers.{HavePropertyMatchResult, HavePropertyMatcher, ShouldMatchers}
//TODO Rewrite to independent tests

@RunWith(classOf[JUnitRunner])
class RangeTableSpec extends WordSpec with ShouldMatchers with RangeBoundMatchers {

"A Range Table" should {
    var rTable : RangeTable[Int, String] = new RangeTable[Int, String](List((None, List("S6"))),
      (a: Int, b: Int) => if(a == b) 0 else if (a < b) -1 else 1,
      (a : Seq[String], b : Seq[String])  => true)

    "return values for a key" in {
      var rangeTable = createRange()
      rangeTable.valuesForKey(None) should be === List("S0")
      rangeTable.valuesForKey(10) should be === List("S0")
      rangeTable.valuesForKey(30) should be === List("S0")
      rangeTable.valuesForKey(100) should be === List("S0")
      rangeTable = createRange((10, "S1"), (20, "S2"), (30, "S3"))
      rangeTable.valuesForKey(None) should be === List("S0")
      rangeTable.valuesForKey(5) should be === List("S0")
      rangeTable.valuesForKey(10) should be === List("S1")
      rangeTable.valuesForKey(20) should be === List("S2")
      rangeTable.valuesForKey(25) should be === List("S2")
      rangeTable.valuesForKey(30) should be === List("S3")
      rangeTable.valuesForKey(35) should be === List("S3")
      rangeTable.valuesForKey(100) should be === List("S3")
    }

    "split ranges left attached" in {
      rTable = rTable.split(20, List("S1"))
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

      val rangeTable = createRange((10, "S1"), (20, "S2"), (30, "S3"), (40, "S4"))
      val r1 = rangeTable.valuesForRange(None, Option(25))
      transformRangeArray(r1) should be === List((None,List("S0")), (Some(10),List("S1")), (Some(20),List("S2")))
      val r2 = rangeTable.valuesForRange(Some(0), Some(15))
      transformRangeArray(r2) should be === List((None,List("S0")), (Some(10),List("S1")))
      val r3 = rangeTable.valuesForRange(Some(20), Some(35))
      transformRangeArray(r3) should be === List((Some(20),List("S2")), (Some(30),List("S3")))
      val r4 = rangeTable.valuesForRange(Some(35), Some(70))
      transformRangeArray(r4) should be === List((Some(30),List("S3")), (Some(40),List("S4")))
      val r5 = rangeTable.valuesForRange(Some(35), None)
      transformRangeArray(r5) should be === List((Some(30),List("S3")), (Some(40), List("S4")))
      val r6 = rangeTable.valuesForRange(None, None)
      transformRangeArray(r6) should be === List((None, List("S0")), (Some(10), List("S1")), (Some(20), List("S2")), (Some(30),List("S3")), (Some(40), List("S4")))

    }


    "throw an error if the split key already exists" in {
      rTable.split(30, List("S10"), false) should be === null
    }


    "return the right range for split keys" in {
      rTable.valuesForKey(10) should be === List("S2")
      rTable.valuesForKey(20) should be === List("S3")
      rTable.valuesForKey(60) should be === List("S7")
    }

    "allow to add values" in {
      rTable = rTable.addValueToRange(10, "S10")
      rTable = rTable.addValueToRange(9, "S11")
      rTable = rTable.addValueToRange(None, "None")
      rTable = rTable.addValueToRange(35, "S30")
      rTable = rTable.addValueToRange(100, "S100")
      rTable.valuesForKey(5) should be === List("None", "S11", "S1")
      rTable.valuesForKey(15) should be === List("S10", "S2")
      rTable.valuesForKey(25) should be === List("S3")
      rTable.valuesForKey(35) should be === List("S30", "S4")
      rTable.valuesForKey(45) should be === List("S5")
      rTable.valuesForKey(55) should be === List("S6")
      rTable.valuesForKey(65) should be === List("S100", "S7")

    }

    "prevent adding the same value" in {
      intercept[IllegalArgumentException] {
       rTable.addValueToRange(10, "S10")
      }
    }

    "delete values" in {
      rTable = rTable.removeValueFromRange(9, "S1")
      rTable = rTable.removeValueFromRange(10, "S10")
      rTable = rTable.removeValueFromRange(35, "S30")
      rTable = rTable.removeValueFromRange(None, "None")
      rTable = rTable.removeValueFromRange(100, "S100")

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
      rTable.merge(35) should be === null
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
        (a: Int, b: Int) => if(a == b) 0 else if (a < b) -1 else 1,
        (a: Seq[String], b: Seq[String]) => a.corresponds(b)(_.compareTo(_) == 0))
      rangeTable = rangeTable.split(10, List("S1"))
      rangeTable = rangeTable.split(20, List("S1"))

      rangeTable = rangeTable.addValueToRange(5, "S2")
      rangeTable = rangeTable.addValueToRange(15, "S3")

      rangeTable.merge(10) should be === null
      rangeTable.merge(20) should be === null

      rangeTable.valuesForKey(5) should be === List("S2", "S1")
      rangeTable.valuesForKey(15) should be === List("S3", "S1")
      rangeTable.valuesForKey(25) should be === List("S1")

      rangeTable = rangeTable.addValueToRange(5, "S3")
      rangeTable = rangeTable.removeValueFromRange(5, "S2")
      rangeTable = rangeTable.merge(10)
      rangeTable should not be === (null)

      rangeTable.valuesForKey(5) should be === List("S3", "S1")
      rangeTable.valuesForKey(15) should be === List("S3", "S1")
      rangeTable.valuesForKey(25) should be === List("S1")

      rangeTable.merge(20) should be === (null)

      rangeTable = rangeTable.removeValueFromRange(15, "S3")

      rangeTable = rangeTable.merge(20)
      rangeTable should not be (null)

      rangeTable.valuesForKey(5) should be === List("S1")
      rangeTable.valuesForKey(15) should be === List("S1")
      rangeTable.valuesForKey(25) should be === List("S1")
    }

    "merge ranges and replace values" is pending

    "split ranges and replace values" is pending

    "check split keys" in {
      val rangeTable =   createRange((10, "S1"), (20, "S2"))
      rangeTable.isSplitKey(5) should be === false
      rangeTable.isSplitKey(10) should be === true
      rangeTable.isSplitKey(15) should be === false
      rangeTable.isSplitKey(20) should be === true
      rangeTable.isSplitKey(25) should be === false
    }

    "return left and right range" in {
      val rangeTable =   createRange((10, "S1"), (20, "S2"), (30, "S3"), (40, "S4"))
      rangeTable.lowerUpperBound(10) should have (
        left (None),
        center (Some(10)),
        right (Some(20))
      )

      rangeTable.lowerUpperBound(20)  should have (
        left (Some(10)),
        center (Some(20)),
        right (Some(30))
      )

      rangeTable.lowerUpperBound(40) should  have (
        left (Some(30)),
        center (Some(40)),
        right (None)
      )

      rangeTable.lowerUpperBound(5)  should have (
        left (None),
        center (None),
        right (Some(10))
      )

      rangeTable.lowerUpperBound(25) should  have (
        left (Some(10)),
        center (Some(20)),
        right (Some(30))
      )

      rangeTable.lowerUpperBound(45) should have (
        left (Some(30)),
        center (Some(40)),
        right (None)
      )


      val rangeTable2 =   createRange()
      rangeTable2.lowerUpperBound(10) should have (
        left (None),
        center (None),
        right (None)
      )

      val rangeTable3 =   createRange((10, "S1"))
      rangeTable3.lowerUpperBound(10) should have (
        left (None),
        center (Some(10)),
        right (None)
      )

      rangeTable3.lowerUpperBound(5) should have (
        left (None),
        center (None),
        right (Some(10))
      )

      rangeTable3.lowerUpperBound(15) should have (
        left (None),
        center (Some(10)),
        right (None)
      )

    }

    "replace values for range" is pending


  }

  /**
   * Creates a new test range and adds "S0" to the front
   */
  def createRange(values : (Int, String)*) = {
    var setup : List[(Option[Int], List[String])] = values.toList.map(a => (Some(a._1), a._2 :: Nil))
    setup = (None, List("S0")) :: setup

    new RangeTable[Int, String](
        setup,
        (a: Int, b: Int) => if(a == b) 0 else if (a < b) -1 else 1,
        (a :Seq[String], b :Seq[String]) => a.corresponds(b)(_.compareTo(_) == 0))
  }

   def transformRangeArray(rangeTable: Array[RangeType[Int, String]]) : List[(Option[Int], Seq[String])] = {
    for(item <- rangeTable.toList)
        yield  (item.startKey, item.values)
   }


}


trait RangeBoundMatchers {
  def left(expectedValue: Option[Int]) = new HavePropertyMatcher[RangeTable[Int, String]#RangeBound, Option[Int]] {
    def apply(rangeBound: RangeTable[Int, String]#RangeBound) =
      HavePropertyMatchResult( rangeBound.left.startKey == expectedValue, "left", expectedValue, rangeBound.left.startKey ) }

   def center(expectedValue: Option[Int]) = new HavePropertyMatcher[RangeTable[Int, String]#RangeBound, Option[Int]] {
    def apply(rangeBound: RangeTable[Int, String]#RangeBound) =
      HavePropertyMatchResult( rangeBound.center.startKey == expectedValue, "center", expectedValue, rangeBound.center.startKey ) }

   def right(expectedValue: Option[Int]) = new HavePropertyMatcher[RangeTable[Int, String]#RangeBound, Option[Int]] {
    def apply(rangeBound: RangeTable[Int, String]#RangeBound) =
      HavePropertyMatchResult( rangeBound.right.startKey == expectedValue, "right", expectedValue, rangeBound.right.startKey ) }

}
