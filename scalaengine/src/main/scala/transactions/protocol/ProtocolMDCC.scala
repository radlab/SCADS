package edu.berkeley.cs.scads.storage.transactions.mdcc

import _root_.edu.berkeley.cs.scads.storage.transactions._
import edu.berkeley.cs.scads.comm._
import MDCCMetaHelper._

import net.lag.logging.Logger

import java.lang.Thread
import java.util.Calendar

import collection.mutable.HashSet

//import tools.nsc.matching.ParallelMatching.MatchMatrix.VariableRule
import actors.Actor

sealed trait RecordStatus {def name: String}
case object FRESH extends RecordStatus {val name = "FRESH"}
case object PROPOSED extends RecordStatus {val name = "PROPOSED"}
case object FAST_PROPOSED extends RecordStatus {val name = "FAST_PROPOSED"}
case object PHASE1A extends RecordStatus {val name = "PHASE1A"}
case object PHASE2A extends RecordStatus {val name = "PHASE2A"}
case object LEARNED_ABORT extends RecordStatus {val name = "LEARNED_ABORT"}
case object LEARNED_ACCEPT extends RecordStatus {val name = "LEARNED_ACCEPT"}


sealed case class MDCCUpdateInfo(
                                    var update: RecordUpdate,
                                    var servers: Seq[PartitionService],
                                    var meta: Option[MDCCMetadata],
                                    var futures: List[MessageFuture],
                                    var status: RecordStatus) {
  override def hashCode() = update.key.hashCode()
}

object MDCCHandler extends ProtocolBase {
  def RunProtocol(tx: Tx) = {
    //TODO make it thread-less
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
        case ValueUpdateInfo(servers, key, value) => {
          val oldRecord = readList.getRecord(key)
          val md = oldRecord.map(r => MDCCMetadata(r.metadata.currentRound, r.metadata.ballots))
          val oldValue : Option[Array[Byte]] = oldRecord.flatMap(_.value)
          assert(value.isDefined) //We still need to change that
          new MCCCRecordHandler(ValueUpdate(key, oldValue, value.get), servers, md, Xid)

        }
        case LogicalUpdateInfo(servers, key, value) => {
          val md = readList.getRecord(key).map(r =>
            MDCCMetadata(r.metadata.currentRound, r.metadata.ballots))
          assert(value.isDefined) //We still need to change that
          new MCCCRecordHandler(LogicalUpdate(key, value.get), servers, md, Xid)
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
      u.status match {
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

object DefaultMetaData {

  //TODO We should store the default in ZooKeeper, otherwise
  //the protocol is incorrect
  def getDefault(servers: Seq[PartitionService]) : MDCCMetadata = {
    val minServer = servers.minBy(_.toString)
    MDCCMetadata(0, MDCCBallotRange(0,0,0,minServer, false) :: Nil)
  }

}

class MCCCRecordHandler (
        val update: RecordUpdate,
        val servers: Seq[PartitionService],
        metaOption: Option[MDCCMetadata],
        val Xid: ScadsXid) extends Actor {
  var futures: List[MessageFuture] = Nil
  var status: RecordStatus = FRESH
  var meta = if(metaOption.isEmpty) DefaultMetaData.getDefault(servers) else metaOption.get

  implicit val localAddress = null

  private var respondFunctions: List[MCCCRecordHandler => Unit] = Nil

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
        case BeMaster(key: Array[Byte], startRound: Long, endRound: Long, fast : Boolean) => startPhase1a(key, startRound, endRound, fast)
        case ProposeTrx() => SendProposal()
        case Phase1b(ballot: MDCCBallot, value: CStruct) =>
        case Phase2bClassic(ballot: MDCCBallot, value: CStruct) =>
        case Phase2bFast(ballot: MDCCBallot, value: CStruct) =>
        case Accept(xid: ScadsXid) =>
        case _ => throw new RuntimeException("Not Implemented")
      }
    }
  }


  def selfNotification(body : MessageBody) = this.!(body)


  def startPhase1a(key: Array[Byte], startRound: Long, endRound: Long, fast : Boolean) : Unit =  {
    if(isMaster(meta)) return  //I am already master
    val newRange = getOwnership(meta, startRound, endRound, fast)
    val phase1aMsg = Phase1a(update.key, newRange)
    val futures : List[MessageFuture] = servers.map(_ !! phase1aMsg)
    futures.foreach(_.respond(selfNotification ))
  }

  def SendProposal() = {
    val propose = Propose(Xid, update)
    val futures : List[MessageFuture] =
      if (metaOption.isEmpty || //Without having the meta data we assume somebody else is responsible
        fastRound(meta)){  //If we have a fast round, we can propose directly
        servers.map(_ !! propose)
      }else{
        val master = getMaster(meta)
        if(isMaster(meta)){
           startPhase2()
        }else{
          (master !! propose) :: Nil
        }
      }
    futures.foreach(_.respond(selfNotification ))
  }



  def startPhase2() : List[MessageFuture] = {
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
