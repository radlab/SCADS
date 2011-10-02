package edu.berkeley.cs.scads.storage.transactions

import _root_.edu.berkeley.cs.scads.comm.{SCADSService, MDCCBallotRange, MDCCMetadata}

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

  @inline def min(a : Long, b : Long) : Long = if (a < b) a else b
  @inline def max(a : Long, b : Long) : Long = if (a > b) a else b
  @inline def max(a : Int, b : Int) : Int = if (a > b) a else b

  def getOwnership(meta : MDCCMetadata, startRound: Long, endRound: Long, r: SCADSService, fast : Boolean) : MDCCMetadata = {
    assert(!fast || endRound - startRound == 1)  //it is not possible to have more than one fast round assigned
    assert(!(startRound == meta.currentRound) || fast == meta.ballots.head.fast) //you are not allowed to change the current type
    var ballots = meta.ballots
    val newRange = MDCCBallotRange(startRound, endRound, 0, r, fast)

    ballots = replace(ballots, newRange)
    MDCCMetadata(meta.currentRound, ballots)
  }

  def getOwnership(meta : MDCCMetadata)(implicit r: SCADSService) : MDCCMetadata  = {
    getOwnership(meta, meta.currentRound, meta.currentRound, r, meta.ballots.head.fast)
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

  def compareMetadata(metaL : MDCCMetadata, metaR : MDCCMetadata): Int = {
    assert(validateMeta(metaL))
    assert(validateMeta(metaR))
    if (metaL.currentRound < metaR.currentRound)
      return -1
    else if (metaL.currentRound > metaR.currentRound)
      return 1
    else
    {
      val rangeL = metaL.ballots(0)
      val rangeR = metaR.ballots(0)
      if(rangeL.vote < rangeR.vote)
        return -1
      else if (rangeL.vote > rangeR.vote)
        return 1
      else
        rangeL.server.toString.compare(rangeR.server.toString())
    }
  }

}