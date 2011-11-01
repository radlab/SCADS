package edu.berkeley.cs.scads.storage
package transactions

import _root_.edu.berkeley.cs.avro.marker.AvroRecord
import scala.math.{min, max}
import collection.mutable.{ArrayBuffer, ArraySeq}

case class MDCCBallotRange(var startRound: Long, var endRound: Long, var vote: Int, var server: SCADSService, var fast: Boolean) extends AvroRecord {
  /**
   * Returns the first round as a ballot
   */
  def ballot() = MDCCBallot(startRound, vote, server, fast)
}

case class MDCCMetadata(var currentVersion: MDCCBallot, var ballots: Seq[MDCCBallotRange]) extends AvroRecord {
  def validate() =  {
    MDCCBallotRangeHelper.validate(currentVersion, ballots)
  }
}



/* Transaction KVStore Metadata */
case class MDCCBallot(var round: Long, var vote: Int, var server: SCADSService, var fast: Boolean) extends AvroRecord {

  def <= (that: MDCCBallot): Boolean = compare(that) <= 0

  def >= (that: MDCCBallot): Boolean = compare(that) >= 0

  def <  (that: MDCCBallot): Boolean = compare(that) <  0

  def >  (that: MDCCBallot): Boolean = compare(that) > 0

  def equals (that: MDCCBallot): Boolean = compare(that) == 0

  /**
   * Classic ballots always win over fast ballots. Ensures that every round ends on a fast round.
   */
  def compare(other : MDCCBallot) : Int = {
    if(this.round < other.round)
      return -1
    else if (this.round > other.round)
      return 1
    else if (this.fast && !other.fast)
      return -1
    else if (!this.fast && other.fast)
      return 1
    else if(this.vote < other.vote)
      return -1
    else if (this.vote > other.vote)
      return 1
    else
      return this.server.toString.compare(other.server.toString())
  }

  def isMaster()(implicit r: SCADSService) : Boolean= {
    server == r
  }

}


object MDCCBallotRangeHelper {
  def validate(ranges: Seq[MDCCBallotRange]): Boolean = {
    assert(ranges.size > 0)
    var curRange = ranges.head
    var restRange = ranges.tail
    assert(!curRange.fast || curRange.endRound - curRange.startRound == 0)
    assert(curRange.startRound <= curRange.endRound)
    while (!restRange.isEmpty) {
      assert(!restRange.head.fast)
      assert(restRange.head.startRound <= restRange.head.endRound)
      assert(curRange.endRound < restRange.head.startRound)
      assert(!restRange.head.fast)
      curRange = restRange.head
      restRange = restRange.tail
    }
    return true
  }

  def validate(currentVersion : MDCCBallot, ranges: Seq[MDCCBallotRange]) : Boolean = {
    MDCCBallotRangeHelper.validate(ranges)
    getBallot(ranges, currentVersion.round).map( ballot => assert(currentVersion.compare(ballot) <= 0))
    return true
  }

  def isFast(ranges: Seq[MDCCBallotRange], round: Long): Boolean = {
    if (ranges.isEmpty) {
      return false
    } else if (ranges.head.startRound <= round && round <= ranges.head.endRound) {
      return ranges.head.fast
    }
    isFast(ranges.tail, round)
  }

  def topBallot(ranges: Seq[MDCCBallotRange]): MDCCBallot = {
    var top = ranges.head
    MDCCBallot(top.startRound, top.vote, top.server, top.fast)
  }

  def getRange(ranges: Seq[MDCCBallotRange], round: Long): Option[MDCCBallotRange] = {
    if (ranges.isEmpty) {
      return None
    } else if (ranges.head.startRound <= round && round <= ranges.head.endRound) {
      return ranges.head
    }
    getRange(ranges.tail, round)
  }

  def isDefined(ranges: Seq[MDCCBallotRange], round: Long):Boolean =  {
    if (ranges.isEmpty) {
      return false
    } else if (ranges.head.startRound <= round && round <= ranges.head.endRound) {
      return true
    }
    isDefined(ranges.tail, round)
  }

  def getBallot(ranges: Seq[MDCCBallotRange], round: Long): Option[MDCCBallot] = {
    if (ranges.isEmpty) {
      return None
    } else if (ranges.head.startRound <= round && round <= ranges.head.endRound) {
      return MDCCBallot(round, ranges.head.vote, ranges.head.server, ranges.head.fast)
    }
    getBallot(ranges.tail, round)
  }

  def replace(ranges: Seq[MDCCBallotRange], newRange: MDCCBallotRange): Seq[MDCCBallotRange] = {
    val nRange = replace(ranges, newRange, false)
    newRange.vote += 1
    nRange
  }

  /**
   * Insert the newRange into ranges and increases the vote count
   */
  private def replace(ranges: Seq[MDCCBallotRange], newRange: MDCCBallotRange, inserted: Boolean): Seq[MDCCBallotRange] = {
    if (ranges.isEmpty) {
      if (inserted) {
        return Nil
      } else {
        return newRange :: Nil
      }
    }
    val nStart = newRange.startRound
    val nEnd = newRange.endRound
    val head = ranges.head

    if (head.endRound < nStart) {
      return head +: replace(ranges.tail, newRange, inserted)
    }
    newRange.vote = max(newRange.vote, head.vote)
    if (!inserted) {
      if (nStart <= head.startRound) {
        return newRange +: replace(ranges, newRange, true)
      } else {
        return MDCCBallotRange(head.startRound, nStart - 1, head.vote, head.server, head.fast) +: newRange +: replace(ranges, newRange, true)
      }
    }
    if (nEnd < head.endRound) {
      return MDCCBallotRange(nEnd + 1, head.endRound, head.vote, head.server, head.fast) +: ranges.tail
    } else {
      if (head.endRound == nEnd) {
        return ranges.tail
      } else {
        return replace(ranges.tail, newRange, true)
      }
    }
  }

  def adjustRound(ranges: Seq[MDCCBallotRange], curRound: Long): Seq[MDCCBallotRange] = {
    assert(ranges.size > 0)
    if (ranges.head.endRound < curRound) {
      adjustRound(ranges.tail, curRound)
    } else if (ranges.head.startRound < curRound) {
      MDCCBallotRange(max(ranges.head.startRound, curRound), ranges.head.endRound, ranges.head.vote, ranges.head.server, ranges.head.fast) +: ranges.tail
    } else {
      ranges
    }
  }

  /**
   * Returns the range to propose and the new sequence of ranges
   */
  def getOwnership(ranges: Seq[MDCCBallotRange], startRound: Long, endRound: Long, fast: Boolean)(implicit r: SCADSService): Seq[MDCCBallotRange] = {
    val newRange = MDCCBallotRange(startRound, endRound, 0, r, fast)
    replace(ranges, newRange)
  }

  def combine(ballot : MDCCBallot, ranges : Seq[MDCCBallotRange]): Seq[MDCCBallotRange] = {
    //TODO: Quite expensive -> Maybe rewrite
    val newRange = MDCCBallotRange(ballot.round, ballot.round, ballot.vote, ballot.server, ballot.fast)
    combine(ranges, newRange :: Nil, ballot.round)
  }

  def combine(lRange : Seq[MDCCBallotRange], rRange : Seq[MDCCBallotRange], firstRound : Long): Seq[MDCCBallotRange] = {
    var left = lRange
    var right = rRange
    val result = ArrayBuffer[MDCCBallotRange]()
    var curRange: MDCCBallotRange = null
    var curRound: Long = firstRound
    var nextRound: Long = 0

    while (!(left.isEmpty && right.isEmpty)) {
      if (!left.isEmpty && left.head.endRound < curRound) {
        left = left.tail
      } else if (!right.isEmpty && right.head.endRound < curRound) {
        right = right.tail
      } else {
        val dominant =
          if (left.isEmpty) {
            nextRound = right.head.endRound + 1
            right.head
          } else if (right.isEmpty) {
            nextRound = left.head.endRound + 1
            left.head
          } else if (curRound < left.head.startRound) {
            //Right is dominant
            nextRound = min(left.head.startRound, right.head.endRound + 1)
            right.head
          } else if (curRound < right.head.startRound) {
            //Left is dominant
            nextRound = min(right.head.startRound, left.head.endRound + 1)
            left.head
          } else {
            nextRound = min(left.head.endRound + 1, right.head.endRound + 1)
            if (compareRanges(left.head, right.head) < 0) {
              right.head
            } else {
              left.head
            }
          }
        if (curRange == null) {
          curRange = dominant.copy()
          curRange.startRound = curRound
          curRound = nextRound
        } else {
          if (compareRanges(curRange, dominant) != 0) {
            val copy = curRange.copy()
            copy.endRound = curRound - 1
            result += copy
            curRange = dominant.copy()
            curRange.startRound = curRound
          }
          curRound = nextRound
        }
      }
    }
    curRange.endRound = curRound - 1
    result += curRange
    result.head.startRound = max(firstRound, result.head.startRound)
    result
  }

  private def buildRange(ranges: Seq[MDCCBallotRange], startRound: Long, endRound: Long, r: SCADSService, fast: Boolean): MDCCBallotRange = {
    val maxVote = ranges.maxBy(_.vote).vote
    assert(maxVote < Long.MaxValue)
    new MDCCBallotRange(startRound, endRound, maxVote + 1, r, fast)
  }

    /**
   * Classic ballots always win over fast ballots. Ensures that every round ends on a fast round.
   */
  def compareRanges(lRange: MDCCBallotRange, rRange: MDCCBallotRange): Int = {
    if (lRange.fast && !rRange.fast)
      return -1
    else if (!lRange.fast && rRange.fast)
      return 1
    else if (lRange.vote < rRange.vote)
      return -1
    else if (lRange.vote > rRange.vote)
      return 1
    else
      return lRange.server.toString.compare(rRange.server.toString())
  }

 /**
   * Compares two ranges starting from a certain round
   * Returns 0 if both are equal
   * -1 if metaL is smaller
   * 1  if metaL is bigger
   * -2 if it is undefined
   */
  def compareRanges(metaL: Seq[MDCCBallotRange], metaR: Seq[MDCCBallotRange], round : Long): Int = {
    compareRanges(metaL.filter(_.startRound >= round), metaR.filter(_.startRound >= round)) //TODO we need to adjust the first round
  }

  /**
   * Returns 0 if both are equal
   * -1 if metaL is smaller
   * 1  if metaL is bigger
   * -2 if it is undefined
   */
  def compareRanges(metaL: Seq[MDCCBallotRange], metaR: Seq[MDCCBallotRange]): Int = {
    var status : Int = 0
    val validationPairs = metaL.zip(metaR)
    validationPairs.foreach(p => {
      if (p._1.startRound != p._2.startRound || p._1.endRound != p._2.endRound)
        return -2
      val cmp = compareRanges(p._1, p._2)
      if (status == 0)
        status = cmp
      else if (status != cmp) {
        return -2
      }
    })
    return status
  }




}
