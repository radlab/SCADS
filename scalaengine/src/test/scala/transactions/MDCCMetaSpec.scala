package edu.berkeley.cs.scads
package storage
package transactions
package test

import MDCCMetaHelper._

import org.scalatest.matchers.ShouldMatchers
import edu.berkeley.cs.scads.comm._
import org.scalatest.{WordSpec, BeforeAndAfterAll, Spec}

class MDCCMetaSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll{
  "A MDCCMetaHelper" should {

    "increase the vote" in {
      val meta = MDCCMetadata(1, MDCCBallotRange(1, 2, 0, null, false) :: MDCCBallotRange(3, 10, 0, null, false) :: Nil)
      var newRound = MDCCMetaHelper.increaseRound(meta)
      meta.currentRound should be (1)
      meta.ballots(0) should be (MDCCBallotRange(1, 2, 0, null, false))
      meta.ballots(1) should be (MDCCBallotRange(3, 10, 0, null, false))
      newRound.currentRound should be (2)
      newRound.ballots.size should be (2)
      newRound.ballots(0) should be (MDCCBallotRange(2, 2, 0, null, false))
      newRound.ballots(1) should be (MDCCBallotRange(3, 10, 0, null, false))
      newRound = MDCCMetaHelper.increaseRound(newRound)
      newRound.currentRound should be (3)
      newRound.ballots.size should be (1)
      newRound.ballots(0) should be (MDCCBallotRange(3, 10, 0, null, false))
    }

    "replace a ballot range" in {
      val metaRange = MDCCBallotRange(5, 10, 1, null, false) :: MDCCBallotRange(11, 20, 5, null, false) :: MDCCBallotRange(21, 25, 3, null, false) :: Nil
      val beginning = MDCCMetaHelper.replace(metaRange, MDCCBallotRange(0, 7, 0, null, false))
      beginning  should equal (MDCCBallotRange(0,7,2,null,false) :: MDCCBallotRange(8,10,1,null,false) :: MDCCBallotRange(11,20,5,null,false) :: MDCCBallotRange(21,25,3,null,false) :: Nil)
      val middle = MDCCMetaHelper.replace(metaRange, MDCCBallotRange(7, 15, 0, null, false))
      middle should equal  (List(MDCCBallotRange(5,6,1,null,false), MDCCBallotRange(7,15,6,null,false), MDCCBallotRange(16,20,5,null,false), MDCCBallotRange(21,25,3,null,false)))
      val rCorner = MDCCMetaHelper.replace(metaRange, MDCCBallotRange(15, 20, 0, null, false))
      rCorner should equal (List(MDCCBallotRange(5,10,1,null,false), MDCCBallotRange(11,14,5,null,false), MDCCBallotRange(15,20,6,null,false), MDCCBallotRange(21,25,3,null,false)))
      val lCorner = MDCCMetaHelper.replace(metaRange, MDCCBallotRange(11, 15, 0, null, false))
      lCorner should equal (List(MDCCBallotRange(5,10,1,null,false), MDCCBallotRange(11,15,6,null,false), MDCCBallotRange(16,20,5,null,false), MDCCBallotRange(21,25,3,null,false)))
      val full = MDCCMetaHelper.replace(metaRange, MDCCBallotRange(11, 20, 0, null, false))
      full should equal  (List(MDCCBallotRange(5,10,1,null,false), MDCCBallotRange(11,20,6,null,false), MDCCBallotRange(21,25,3,null,false)))
    }

    "make the next round fast" in {
      val meta = MDCCMetadata(1,  MDCCBallotRange(1, 10, 3, null, false) :: MDCCBallotRange(11, 20, 0, null, false) :: Nil)
      val newMeta = MDCCMetaHelper.makeNextRoundFast(meta)
      newMeta.currentRound should be (2)
      newMeta.ballots(0) should be (MDCCBallotRange(2,2,4,null,true))

      newMeta.ballots(1) should be (MDCCBallotRange(3,10,3,null,false))
      newMeta.ballots(2) should be (MDCCBallotRange(11, 20, 0, null, false))
    }


    "get ownership" in {
     val foreign = StorageService("foreign", 1000, ServiceName("foreign"))
     val local = StorageService("local", 1000, ServiceName("local"))
     val meta = MDCCMetadata(1,  MDCCBallotRange(1, 10, 3, foreign, false) :: MDCCBallotRange(11, 20, 0, local, false) :: Nil)
     val newMeta = getOwnership(meta)(local)
     newMeta.ballots should equal (List(MDCCBallotRange(1,1,4,local,false), MDCCBallotRange(2,10,3,foreign,false), MDCCBallotRange(11,20,0,local,false)))

    }

    "test fast round " in {
      val meta = MDCCMetadata(1,  MDCCBallotRange(1, 1, 3, null, true) :: MDCCBallotRange(2, 20, 0, null, false) :: Nil)
      fastRound(meta) should be (true)
      fastRound(MDCCMetadata(1,  MDCCBallotRange(1, 1, 3, null, false) :: MDCCBallotRange(2, 20, 0, null, false) :: Nil)) should be (false)
    }

    "combine MDCC" in {
      val s1 = StorageService("s1", 1000, ServiceName("s1"))
      val s2 = StorageService("s2", 1000, ServiceName("s2"))
      val s3 = StorageService("s3", 1000, ServiceName("s3"))
      val meta1 = MDCCMetadata(3,  MDCCBallotRange(1, 10, 5, s1, false) :: MDCCBallotRange(11, 20, 5, s1, false) :: MDCCBallotRange(21, 30, 5, s1, false) :: Nil)
      val meta2 = MDCCMetadata(5,  MDCCBallotRange(1, 10, 5, s2, false) :: MDCCBallotRange(11, 15, 6, s1, false) :: MDCCBallotRange(16, 18, 6, s2, false) :: MDCCBallotRange(19, 20, 6, s2, false) :: MDCCBallotRange(21, 40, 5, s1, false) :: Nil)
      val meta3 = MDCCMetadata(6,  MDCCBallotRange(6, 6, 6, s3, true) :: MDCCBallotRange(7, 10, 5, s2, false) :: MDCCBallotRange(14, 18, 7, s1, false) :: MDCCBallotRange(23, 23, 5, s1, false) :: Nil)
      val metaC1 = combine(meta1, meta2)
      val metaC2 = combine(meta2, meta3)
      println(metaC2)
      meta1.ballots should equal (List(MDCCBallotRange(1, 10, 5, s1, false), MDCCBallotRange(11, 20, 5, s1, false), MDCCBallotRange(21, 30, 5, s1, false) ))
      metaC1.currentRound should equal (5)
      metaC1.ballots should equal (List(MDCCBallotRange(5,10,5,s2,false), MDCCBallotRange(11,15,6,s1,false), MDCCBallotRange(16,20,6,s2,false), MDCCBallotRange(21,40,5,s1,false)))
      metaC2.currentRound should equal  (6)
      metaC2.ballots should equal  (List(MDCCBallotRange(6,6,6,s3,true), MDCCBallotRange(7,10,5,s2,false), MDCCBallotRange(11,13,6,s1,false), MDCCBallotRange(14,18,7,s1,false), MDCCBallotRange(19,20,6,s2,false), MDCCBallotRange(21,40,5,s1,false)))
    }


  }
}