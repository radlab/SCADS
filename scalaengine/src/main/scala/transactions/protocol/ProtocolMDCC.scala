package transactions.protocol

import _root_.edu.berkeley.cs.scads.storage.transactions.ProtocolBase._
import edu.berkeley.cs.scads.comm._

import net.lag.logging.Logger

import java.lang.Thread
import java.util.Calendar

import transactions.conflict.PendingUpdatesController
import collection.mutable.HashSet
import tools.nsc.matching.ParallelMatching.MatchMatrix.VariableRule
import actors.Actor

sealed trait RecordStatus {def name: String}
case object FRESH extends RecordStatus {val name = "FRESH"}
case object PROPOSED extends RecordStatus {val name = "PROPOSED"}
case object FAST_PROPOSED extends RecordStatus {val name = "FAST_PROPOSED"}
case object PHASE1A extends RecordStatus {val name = "PHASE1A"}
case object PHASE2A extends RecordStatus {val name = "PHASE2A"}
case object LEARNED_ABORT extends RecordStatus {val name = "LEARNED_ABORT"}
case object LEARNED_ACCEPT extends RecordStatus {val name = "LEARNED_ACCEPT"}


sealed case class RecordUpdateInfo(
                                    var update: RecordUpdate,
                                    var servers: Seq[PartitionService],
                                    var meta: MDCCMetadata,
                                    var futures: List[MessageFuture],
                                    var status: RecordStatus) {
  override def hashCode() = update.key.hashCode()
}

object ProtocolMDCC extends ProtocolBase {
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

  protected def transformUpdateList(updateList: UpdateList, readList: ReadList): Seq[RecordUpdateInfo] = {
    updateList.getUpdateList.map(update => {
      update match {
        case ValueUpdateInfo(servers, key, value) => {
          val oldRecord = readList.getRecord(key)
          val md = oldRecord.map(r => MDCCMetadata(r.metadata.currentRound, r.metadata.ballots))
          MCCCRecordHandler(ValueUpdate(key, oldRecord.map(r => r.value), newBytes), servers, md, Xid)

        }
        case LogicalUpdateInfo(servers, key, value) => {
          val md = readList.getRecord(key).map(r =>
            MDCCMetadata(r.metadata.currentRound, r.metadata.ballots))
          MCCCRecordHandler(LogicalUpdate(key, newBytes), servers, md, Xid)
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
        case ABORT => {
          status = ABORT
          return true
        }
        case COMMIT => ()
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
    updates.foreach(_.execute())
    loop {
      react {
        case FINISHED_RECORD => {
          determined()
          exit()
        }
        case _ =>
          throw new RuntimeException("Unknown message")

      }
    }
  }
}

object MCCCRecordHandler {

  private def selectMaster() = {
    throw new RuntimeException("No Master")
  }



  /**
   * Returns the master or none if i
   */
  def getMaster(meta : MDCCMetadata) : Option[RemoteActorProxy] = {
    if (meta.ballots.size == 0)
      selectMaster
    val ballot = meta.ballots(0)
    assert(meta.currentRound >= ballot.startRound)
    assert(meta.currentRound <= ballot.endRound)
    if (ballot.fast)
      None
    else
      Some(ballot.server)
  }
}

class MCCCRecordHandler (
        val update: RecordUpdate,
        val servers: Seq[PartitionService],
        var meta: MDCCMetadata,
        val Xid: ScadsXid,
        val serverAddress : RemoteActorProxy) extends Actor {
  var futures: List[MessageFuture] = Nil
  var status: RecordStatus = FRESH

  private var respondFunctions: List[MCCCRecordHandler => Unit] = Nil

  def respond(r: MCCCRecordHandler => Unit): Unit = synchronized {
    respondFunctions ::= r
  }

  def notifyListeners() = {
    respondFunctions.foreach(_(this))
  }

  override def hashCode() = update.key.hashCode()


  def act() {
    SendProposal()
    loop {
      react {
        case BeMaster(key: Array[Byte]) => CreateMastership(key)
        case Phase1b(ballot: MDCCBallot, value: CStruct) =>
        case Phase2bClassic(ballot: MDCCBallot, value: CStruct) =>
        case Phase2bFast(ballot: MDCCBallot, value: CStruct) =>
        case Accept(xid: ScadsXid) =>
        case _ => throw new ProcessingException("Not Implemented", "")
      }
    }
  }



  def selfNotification(body : MessageBody) = this ! body


  def InitMastership(key: Array[Byte]) =  {
    if(meta.)
  }

  def SendProposal() = {
    val master = getMaster(meta)
    val propose = Propose(Xid, update)
    val futures =
      if (master.isEmpty || master.get == serverAddress) {
          servers.map(_ !! propose)
        }else{
          (master.get !! propose) :: Nil
        }
    futures.foreach(_.respond(selfNotification ))
  }

  def Phase1a() = {
    //Enabled if maxTried = [None]
    //sends message ("1b", m, a, bAa) to acceptors
  }

  def Phase2Start() = {
    //Enabled if maxTried = [None]
    //leader received "1b" for balnom m from every acceptor in quorum
    // v = w add sigma, where sigma is element of Seq(propCmd ),
    // w element of ProvedSafe(Q; m; beta), and beta is any ballot array such that,
    // for every acceptor a in Q, beta_head_a = k and the leader has received a message ("1b"; m; a; p)
    // with beta_a = p
  }

  def Accept() = {

  }


}
