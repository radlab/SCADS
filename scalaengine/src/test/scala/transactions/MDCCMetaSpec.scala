package edu.berkeley.cs.scads
package storage
package transactions
package test


import org.scalatest.matchers.ShouldMatchers
import edu.berkeley.cs.scads.comm._
import org.scalatest.{WordSpec, BeforeAndAfterAll, Spec}
import transactions.MDCCBallotRangeHelper._

class MDCCMetaSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll{
  "A MDCCMetaHelper" should {

    "compare ranges" in {
      val s1 = StorageService(RemoteNode("s1", 100), ServiceName("s1"))
      val ballots : Seq[MDCCBallotRange] = MDCCBallotRange(0,0,0,s1,true) :: Nil
      val newBallots :  Seq[MDCCBallotRange]  = MDCCBallotRange(0,1000,1,s1,false) :: Nil
      val maxRound = 0
      compareRanges(ballots, newBallots, maxRound) should be (-1)

    }

//     "replace ranges" in {
//      val s1 = StorageService(RemoteNode("s1", 100), ServiceName("s1"))
//      val ballots : Seq[MDCCBallotRange] = MDCCBallotRange(0,0,0,s1,true) :: Nil
//      val newRange :  MDCCBallotRange  = MDCCBallotRange(0,1000,0,s1,false)
//      val maxRound = 0
//      replace(ballots, newRange).toString should be ("List(MDCCBallotRange(0,0,1,ServiceName(s1)@s1:100,false),MDCCBallotRange(1,1000,1,ServiceName(s1)@s1:100,false)) ")
//
//    }



  }
}