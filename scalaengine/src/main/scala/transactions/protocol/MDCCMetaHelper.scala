package edu.berkeley.cs.scads.storage.transactions

import _root_.edu.berkeley.cs.scads.comm.{MDCCBallot, SCADSService, MDCCBallotRange, MDCCMetadata}
import scala.math.{min, max}
import collection.mutable.{ArrayBuffer, ArraySeq}

/**
 * Created by IntelliJ IDEA.
 * User: tim
 * Date: 9/25/11
 * Time: 5:05 PM
 * To change this template use File | Settings | File Templates.
 */

object MDCCMetaHelper {
  def validateMeta(meta : MDCCMetadata) : Boolean = {
    assert(meta.ballots.size > 0)
    var curRange = meta.ballots.head
    var restRange = meta.ballots.tail
    assert(!curRange.fast || curRange.endRound - curRange.startRound == 0)
    assert(curRange.startRound <= meta.currentRound )
    assert(meta.currentRound <= curRange.endRound  )
    assert(curRange.startRound <= curRange.endRound)
    while(restRange.size > 0){
      assert(!restRange.head.fast)
      assert(restRange.head.startRound <= restRange.head.endRound)
      assert(curRange.endRound < restRange.head.startRound)
      assert(!restRange.head.fast)
      curRange =  restRange.head
      restRange = restRange.tail
    }
    return true
  }

  def currentBallot(meta : MDCCMetadata ) : MDCCBallot = {
    validateMeta(meta)
    val range = meta.ballots.head
    MDCCBallot(meta.currentRound, range.vote, range.server, range.fast)
  }

  def makeNextRoundFast(meta : MDCCMetadata): MDCCMetadata = {
    assert(validateMeta(meta))
    var ballots = meta.ballots
    val nextRound = meta.currentRound + 1
    assert(!ballots.head.fast)
    val fastRound = MDCCBallotRange(nextRound, nextRound, ballots.head.vote, ballots.head.server, true)
    ballots = replace(ballots, fastRound)
    MDCCMetadata(nextRound, adjustRound(ballots, nextRound))
  }

  def makeNextRoundClassic(meta : MDCCMetadata, r: SCADSService): MDCCMetadata = {
    val next = makeClassicRounds(meta, meta.currentRound + 1, meta.currentRound + 1, r)
    next.currentRound += 1
    next.ballots =  adjustRound(next.ballots, next.currentRound)
    next
  }

  def makeClassicRounds(meta : MDCCMetadata, startRound: Long, endRound: Long, r: SCADSService) : MDCCMetadata  = {
    assert(validateMeta(meta))
    var ballots = meta.ballots
    val classicRound = MDCCBallotRange(startRound, endRound, 0, r, false)
    ballots = replace(ballots, classicRound) //sets the vote
    MDCCMetadata(meta.currentRound, ballots)
  }

  def increaseRound(meta : MDCCMetadata) : MDCCMetadata = {
    assert(validateMeta(meta))
    MDCCMetadata(meta.currentRound + 1, adjustRound(meta.ballots, meta.currentRound + 1))

  }

  def adjustRound(ranges : Seq[MDCCBallotRange], curRound: Long) : Seq[MDCCBallotRange] = {
    assert(ranges.size > 0)
    if(ranges.head.endRound < curRound){
      adjustRound(ranges.tail, curRound)
    }else if (ranges.head.startRound < curRound) {
       MDCCBallotRange(max(ranges.head.startRound, curRound), ranges.head.endRound,  ranges.head.vote, ranges.head.server, ranges.head.fast) +: ranges.tail
    }else{
      ranges
    }
  }

  def replace(ranges : Seq[MDCCBallotRange], newRange: MDCCBallotRange): Seq[MDCCBallotRange] = {
    val nRange = replace(ranges, newRange, false)
    newRange.vote += 1
    nRange
  }

  /**
   * Insert the newRange into ranges and increases the vote count
   */
  private def replace(ranges : Seq[MDCCBallotRange], newRange: MDCCBallotRange, inserted  : Boolean): Seq[MDCCBallotRange] = {
      if (ranges.isEmpty){
        if (inserted) {
          return Nil
        }else{
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
       if (nStart <= head.startRound){
        return newRange +:  replace(ranges, newRange, true)
       }else{
        return MDCCBallotRange(head.startRound, nStart - 1, head.vote, head.server, head.fast) +: newRange +:  replace(ranges, newRange, true)
       }
      }
      if (nEnd < head.endRound) {
        return MDCCBallotRange(nEnd + 1, head.endRound, head.vote, head.server, head.fast) +: ranges.tail
      }else{
        if (head.endRound == nEnd){
          return ranges.tail
        }else{
          return replace(ranges.tail, newRange, true)
        }
      }

  }

   def getOwnershipRange(meta : MDCCMetadata, startRound : Long, endRound :Long, fast : Boolean)(implicit r: SCADSService) : MDCCBallotRange = {
    var ballots = meta.ballots
    val newRange = MDCCBallotRange(startRound, endRound, 0, r, fast)
    ballots = replace(ballots, newRange)
    newRange
   }

   def getOwnershipRange(meta : MDCCMetadata)(implicit r: SCADSService) : MDCCBallotRange = {
    assert(validateMeta(meta))
    MDCCBallotRange(meta.currentRound, meta.currentRound, meta.ballots.head.vote + 1, r, meta.ballots.head.fast)
   }

  def getOwnership(meta : MDCCMetadata, startRound: Long, endRound: Long, fast : Boolean)(implicit r: SCADSService) : MDCCMetadata = {
    assert(!fast || endRound - startRound == 1)  //it is not possible to have more than one fast round assigned
    assert(!(startRound == meta.currentRound) || fast == meta.ballots.head.fast) //you are not allowed to change the current type
    var ballots = meta.ballots
    val newRange = MDCCBallotRange(startRound, endRound, 0, r, fast)

    ballots = replace(ballots, newRange)
    MDCCMetadata(meta.currentRound, ballots)
  }


  def combine(lMeta : MDCCMetadata, rMeta : MDCCMetadata) : MDCCMetadata  = {

    var left= lMeta.ballots
    var right = rMeta.ballots
    val result  = ArrayBuffer[MDCCBallotRange]()
    var curRange : MDCCBallotRange = null
    var firstRound =  max(lMeta.currentRound, rMeta.currentRound)
    var curRound : Long =  firstRound
    var nextRound : Long = 0

    while(!(left.isEmpty && right.isEmpty)){
      if (!left.isEmpty && left.head.endRound < curRound) {
        left = left.tail
      }else if (!right.isEmpty && right.head.endRound < curRound){
        right = right.tail
      }else{
        val dominant =
          if (left.isEmpty) {
            nextRound = right.head.endRound + 1
            right.head
          }else if(right.isEmpty) {
            nextRound = left.head.endRound + 1
            left.head
          } else if (curRound < left.head.startRound){ //Right is dominant
            nextRound = min(left.head.startRound, right.head.endRound + 1)
            right.head
          }else if (curRound < right.head.startRound){  //Left is dominant
            nextRound = min(right.head.startRound, left.head.endRound + 1)
            left.head
          }else{
            nextRound = min(left.head.endRound + 1, right.head.endRound + 1)
            if (compareMetadataRound(left.head, right.head) < 0){
              right.head
            }else{
              left.head
            }
          }
        if (curRange == null){
          curRange = dominant.copy()
          curRange.startRound = curRound
          curRound = nextRound
        }else{
          if(compareMetadataRound(curRange,dominant) != 0 ){
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
    MDCCMetadata(firstRound, result)

  }

  def getOwnership(meta : MDCCMetadata)(implicit r: SCADSService) : MDCCMetadata  = {
    getOwnership(meta, meta.currentRound, meta.currentRound, meta.ballots.head.fast)(r)
  }

  private def buildRange(ranges : Seq[MDCCBallotRange], startRound: Long, endRound: Long, r: SCADSService, fast : Boolean) : MDCCBallotRange = {
    val maxVote = ranges.maxBy(_.vote).vote
    assert(maxVote < Long.MaxValue)
    new MDCCBallotRange(startRound, endRound, maxVote + 1, r, fast)
  }


  def curRange(meta : MDCCMetadata) : MDCCBallotRange = {
    assert(meta.ballots.size > 0)
    assert(meta.ballots.head.startRound <= meta.currentRound)
    assert(meta.ballots.head.endRound >= meta.currentRound)
    return meta.ballots.head
  }

  def getRange(meta : MDCCMetadata, startRound: Long, endRound: Long) = {
    meta.ballots.filter(r => (startRound <=  r.endRound && r.startRound <= endRound))
  }

  def isMaster(meta : MDCCMetadata)(implicit r: SCADSService) = {
    getMaster(meta) == r
  }

  def fastRound(meta : MDCCMetadata) : Boolean = {
    assert(validateMeta(meta))
    meta.ballots.head.fast
  }

  def getMaster(meta : MDCCMetadata) : SCADSService = {
    assert(validateMeta(meta))
    meta.ballots.head.server
  }

  def compareMetadataRound(lRange : MDCCBallotRange, rRange : MDCCBallotRange) : Int = {
    if (lRange.fast && !rRange.fast)
      return 1
    else if (!lRange.fast && rRange.fast)
      return -1
    else if(lRange.vote < rRange.vote)
      return -1
    else if (lRange.vote > rRange.vote)
      return 1
    else
      return lRange.server.toString.compare(rRange.server.toString())
  }

  def compareMetadata(metaL : MDCCMetadata, metaR : MDCCMetadata): Int = {
    assert(validateMeta(metaL))
    assert(validateMeta(metaR))
    if (metaL.currentRound < metaR.currentRound)
      return -1
    else if (metaL.currentRound > metaR.currentRound)
      return 1
    else
      compareMetadataRound(metaL.ballots.head, metaR.ballots.head)
  }

}