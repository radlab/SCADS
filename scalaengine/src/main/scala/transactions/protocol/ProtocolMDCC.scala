package edu.berkeley.cs.scads
package storage
package transactions
package mdcc

import comm._
import MDCCMetaHelper._
import scala.math.{floor, ceil}

import net.lag.logging.Logger

import java.lang.Thread
import java.util.Calendar

import collection.mutable.HashSet
import java.util.concurrent.atomic.AtomicInteger

//import tools.nsc.matching.ParallelMatching.MatchMatrix.VariableRule
import actors.Actor




sealed case class MDCCUpdateInfo(   var update: RecordUpdate,
                                    var servers: Seq[PartitionService],
                                    var meta: Option[MDCCMetadata],
                                    var futures: List[MessageFuture[StorageMessage]],
                                    var status: RecordStatus) {
  override def hashCode() = update.key.hashCode()
}

object MDCCHandler extends ProtocolBase {
  def RunProtocol(tx: Tx) = {
    val trxHandler = new MCCCTrxHandler(tx)
    trxHandler.start()
  }


}

case object FINISHED_RECORD
case object MESSAGE

class MCCCTrxHandler(tx: Tx) extends Actor {
  val updates: Seq[MCCCRecordHandler] = transformUpdateList(tx.updateList, tx.readList)
  var status: TxStatus = UNKNOWN
  var Xid = ScadsXid.createUniqueXid()

  protected def transformUpdateList(updateList: UpdateList, readList: ReadList): Seq[MCCCRecordHandler] = {
    updateList.getUpdateList.map(update => {
      update match {
        case ValueUpdateInfo(ns, servers, key, value) => {
          val oldRrecord = readList.getRecord(key)
          val md : MDCCMetadata= oldRrecord
                                  .map(r => MDCCMetadata(r.metadata.currentRound, r.metadata.ballots))
                                  .getOrElse(ns.getDefaultMeta())
          //TODO: Do we really need the MDCCMetadata
          val newBytes = MDCCRecordUtil.toBytes(value, md)
           new MCCCRecordHandler(Xid, ValueUpdate(key, oldRrecord.flatMap(_.value), value.get), servers, md)
        }
        case LogicalUpdateInfo(ns, servers, key, value) => {
          val md = readList.getRecord(key)
                              .map(r => MDCCMetadata(r.metadata.currentRound, r.metadata.ballots))
                              .getOrElse(ns.getDefaultMeta())
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          new MCCCRecordHandler(Xid, LogicalUpdate(key, value.get), servers, md)
        }
      }
    })
  }

  private def determined() : Boolean = {
    if (status != UNKNOWN) {
      return true
    }
    var finished = true
    updates.foreach( u => {
      u.getStatus match {
        case LEARNED_ABORT => {
          status = ABORT
          return true
        }
        case LEARNED_ACCEPT => {

        }
        case _ => finished = false
      }
    })
    if(finished){
      status = COMMIT
      return true
    }else{
      return false
    }
  }

  def act() {
    updates.foreach(_.start())
    val proposeMsg = ProposeTrx()
    updates.foreach(_ ! proposeMsg)
    loop {
      react {
        case FINISHED_RECORD => {
          if (determined())
            exit()
        }
        case _ =>
          throw new RuntimeException("Unknown message")

      }
    }
  }
}

sealed trait RecordStatus {def name: String}
case object READY extends RecordStatus {val name = "READY"}
case object PROPOSE extends RecordStatus {val name = "PROPOSE"}
case object PROPOSED extends RecordStatus {val name = "PROPOSED"}
case object FAST_PROPOSED extends RecordStatus {val name = "FAST_PROPOSED"}
case class PHASE1A(val key: Array[Byte], val startRound: Long, val endRound: Long, val fast : Boolean) extends RecordStatus  {val name = "PHASE1A"}
case object PHASE2A extends RecordStatus {val name = "PHASE2A"}
case object LEARNED_ABORT extends RecordStatus {val name = "LEARNED_ABORT"}
case object LEARNED_ACCEPT extends RecordStatus {val name = "LEARNED_ACCEPT"}

class MCCCRecordHandler (
       var Xid: ScadsXid,
       var update: RecordUpdate,
       var servers: Seq[PartitionService],
       var meta: MDCCMetadata
  ) extends Actor {

  private var status: RecordStatus = READY
  var quorum : Int = 0
  var maxValue : Seq[CStructCommand]  = null

  implicit val localAddress = null

  private var respondFunctions: List[MCCCRecordHandler => Unit] = Nil

  def getStatus = status

  def respond(r: MCCCRecordHandler => Unit): Unit = synchronized {
    respondFunctions ::= r
  }

  def notifyListeners() = {
    respondFunctions.foreach(_(this))
  }

  override def hashCode() = update.key.hashCode()


  def act() {
    loop {
      react {
        case PROPOSE => {
          assert(status == READY)
          SendProposal()
        }
        case request @ BeMaster(key: Array[Byte], startRound: Long, endRound: Long, fast : Boolean) => {
          assert(status == READY)
          startPhase1a(key, startRound, endRound, fast)
        }
        case Phase1b(ballot: MDCCMetadata, value: CStruct) => {   //we do the phase check afterwards to empty out old messages
          status match {
            case a : PHASE1A => processPhase1b(ballot, value)
            case _ => //out of phase message
          }
        }
        case Phase2bClassic(ballot: MDCCBallot, value: CStruct) =>
        case Phase2bFast(ballot: MDCCBallot, value: CStruct) =>
        case Accept(xid: ScadsXid) =>
        case _ => throw new RuntimeException("Not Implemented")
      }
    }
  }


  def selfNotification(body : StorageMessage) = this.!(body)

  def processPhase1b(aMeta: MDCCMetadata, value: CStruct) = {
    assert(status == PHASE1A)
    compareMetadata(aMeta, meta) match {
      case 0 =>
        //The storage node accepted the new meta data
        quorum += 1
      case 1 =>
        //We got an old message, we can ignore this case
      case -1  | -2 => {
        //TODO We need to ensure progress while two nodes try to get the mastership
        //There was a new meta data version out there, so we try it again
        meta = combine(meta, aMeta)
        status match {
          case PHASE1A(key, startRound, endRound, fast)   => startPhase1a(key, startRound, endRound, fast)
          case _ => assert(false)
        }

      }
      case _ => assert(false) //should never happen
    }
    if(quorum >= classicQuorum && update != null) { //only if we need to do an update we go on to Phase2a
      startPhase2a()
    }
  }

  @inline def classicQuorum = floor(servers.size.toDouble / 2.0).toInt + 1
  @inline def fastQuorum = ceil(3.0 * servers.size.toDouble / 4.0).toInt




  def startPhase1a(key: Array[Byte], startRound: Long, endRound: Long, fast : Boolean) : Unit =  {
    status = PHASE1A(key, startRound, endRound, fast)
     //I am already master
    meta = getOwnership(meta, startRound, endRound, fast)
    val phase1aMsg = Phase1a(update.key, meta)
    val futures : Seq[MessageFuture] = servers.map(_ !! phase1aMsg)
    quorum = 0
    maxValue = null
    futures.foreach(_.respond(selfNotification ))
  }

  def SendProposal() = {
    val propose = Propose(Xid, update)
    val futures : List[MessageFuture] =
      if (fastRound(meta)){  //If we have a fast round, we can propose directly
        status = FAST_PROPOSED
        servers.map(_ !! propose)
      }else{
        val master = getMaster(meta)
        if(isMaster(meta)){
          status = PHASE2A
          startPhase2a()
        }else{
          status = PROPOSED
          (master !! propose) :: Nil
        }
      }
    futures.foreach(_.respond(selfNotification ))

  }



  def startPhase2a() : List[MessageFuture] = {
    assert(isMaster(meta))

    //Enabled if maxTried = [None]
    //leader received "1b" for balnom m from every acceptor in quorum
    // v = w add sigma, where sigma is element of Seq(propCmd ),
    // w element of ProvedSafe(Q; m; beta), and beta is any ballot array such that,
    // for every acceptor a in Q, beta_head_a = k and the leader has received a message ("1b"; m; a; p)
    // with beta_a = p

    Nil
  }

  def processAccept() = {

  }


}
