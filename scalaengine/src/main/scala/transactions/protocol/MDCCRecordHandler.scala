package edu.berkeley.cs.scads
package storage
package transactions
package mdcc

import comm._
import scala.math.{floor, ceil, min, max}
import MDCCBallotRangeHelper._

import edu.berkeley.cs.scads.util.{Logger, Scheduler}
import org.apache.avro.generic.IndexedRecord
import conflict.ConflictResolver
import actors.{TIMEOUT, Actor}
import collection.mutable.{HashMap, ArrayBuffer}
import storage.SCADSService


sealed trait RecordStatus {def name: String}
case object READY extends RecordStatus {val name = "READY"}
case object FORWARDED extends RecordStatus {val name = "PROPOSED"}    //TODO actually we do not need a Forwarded status --> Future work remove
case object FAST_PROPOSED extends RecordStatus {val name = "FAST_PROPOSED"}
case object PHASE1A  extends RecordStatus {val name = "PHASE1A"}
case object PHASE2A extends RecordStatus {val name = "PHASE2A"}
case object RECOVERY extends RecordStatus {val name = "RECOVERY"}
case object WAITING_FOR_COMMIT extends RecordStatus {val name = "READY"}

object ServerMessageHelper {
  class SMH(s: Seq[PartitionService]) {
    def !(msg : MDCCProtocol)(implicit sender: RemoteServiceProxy[StorageMessage]) = s.foreach(_ ! msg)
  }
  implicit def smh(i: Seq[PartitionService]) = new SMH(i)
}


class MDCCRecordHandler (
       var key : Array[Byte],
       var value : CStruct,
       var version: MDCCBallot, //The version of the value
       var ballots : Seq[MDCCBallotRange], //Majority Ballot
       var confirmedBallot : Boolean, //Is the ballot confirmed by a majority?
       var servers: Seq[PartitionService],
       var resolver : ConflictResolver,
       var master : SCADSService //If we are master, this is outer not garbage collected remote handler
  ) {

  import ServerMessageHelper._

  //TODO we should always
  private var provedSafe : CStruct = value //Because of readCommitted property, that value is fine
  private var unsafeCommands : Seq[SinglePropose] = Nil

  private val logger = Logger(classOf[MDCCRecordHandler])

  private val mailbox = new PlainMailbox[StorageMessage]()

  private var responses =  new HashMap[ServiceType, MDCCProtocol]()

  implicit val remoteHandle = StorageService(StorageRegistry.registerFastMailboxFunc(processMailbox, mailbox))

  private var request : Envelope[StorageMessage] = null

  private var masterRecordHandler : SCADSService = null //HACK Needed to get the commit message through


  private var status: RecordStatus = READY

  type ServiceType =  RemoteServiceProxy[StorageMessage]

  @inline def debug(msg : String, items : scala.Any*) = logger.debug("id:" + remoteHandle.id + " key:" + (new ByteArrayWrapper(key)).hashCode() + ":" + status + " " + msg, items:_*)

  implicit def toRemoteService(src : ServiceType) : RemoteService[StorageMessage] = src.asInstanceOf[RemoteService[StorageMessage]]

  implicit def extractSource(src : Option[RemoteService[IndexedRecord]]) = src.get

  //TODO Define mastership with one single actor


  override def hashCode() = key.hashCode()

  implicit def toStorageService(service : RemoteServiceProxy[StorageMessage]) : StorageService = StorageService(service)

  @inline def areWeMaster(service : SCADSService) : Boolean = {
    debug("Checking for mastership current: %s - master: %s == %s", service, master, service == master)
    service == master
  }


  override def equals(that: Any): Boolean = that match {
     case other: MDCCRecordHandler => key == other.key
     case _ => false
  }

  @inline def currentBallot() = MDCCBallotRangeHelper.topBallot(ballots)

  def nextBallot() : Option[MDCCBallot] = {
    validate(version, ballots)
    if(confirmedBallot)
      Some(MDCCBallotRangeHelper.topBallot(ballots))
    else
      None
  }

  @inline private def seq(propose : Propose) :Seq[SinglePropose]= {
    propose match {
      case s : SinglePropose => s :: Nil
      case MultiPropose(seq) => seq
    }
  }

  def getStatus = status

  def commit(msg : MDCCProtocol, xid: ScadsXid, trxStatus : Boolean) = {
    debug("Received commit/abort: " + trxStatus)
    value.commands.find(_.xid == xid).map(cmd => {
      cmd.pending = false
      cmd.commit = trxStatus
    })
    if(trxStatus)
      servers ! Commit(xid)
    else
      servers ! Abort(xid)
    if(masterRecordHandler != null){
      debug("Forwarding the commit/abort message to " + remoteHandle)
      masterRecordHandler ! msg
    }
    if(status == WAITING_FOR_COMMIT)
      status = READY
  }

  //TODO We should create a proper priority queue
  def processMailbox(mailbox : Mailbox[StorageMessage]) {
      mailbox{
        case StorageEnvelope(src, msg: Commit) => {
          commit(msg, msg.xid, true)
        }
        case StorageEnvelope(src, msg: Abort) => {
          commit(msg, msg.xid, false)
        }
        case StorageEnvelope(src, msg: Exit)  if status == READY => {
          debug("EXIT request")
          StorageRegistry.unregisterService(remoteHandle)
        }
        case env@StorageEnvelope(src, msg: BeMaster) if status == READY => {
          request = env
          debug("Received BeMaster message", env)
          startPhase1a(msg.startRound, msg.endRound, msg.fast)
        }
        case env@StorageEnvelope(src, msg: Phase1b) => {
          //we do the phase check afterwards to empty out old messages
          debug("Phase1b message" , env)
          status match {
            case PHASE1A => processPhase1b(src, msg)
            case _ =>
          }
        }
        case env@StorageEnvelope(src, msg: Phase2bMasterFailure) => {
          debug("Phase2bMasterFailure message" , env)
          status match {
            case PHASE2A => processPhase2bMasterFailure(src, msg)
            case FAST_PROPOSED => processPhase2bMasterFailure(src, msg)
            case _ =>
          }
        }
        case env@StorageEnvelope(src, msg: Phase2b) => {
          debug("Phase2b message" , env)
          status match {
            case PHASE2A => processPhase2b(src, msg)
            case FAST_PROPOSED => processPhase2b(src, msg)
            case _ =>
          }
        }
        case env@StorageEnvelope(src, msg : Recovered) => {
          debug("Recovered message", env)
          if (status == RECOVERY) {
            this.value = msg.value
            this.version = msg.meta.currentVersion
            this.ballots = msg.meta.ballots
            confirmedBallot = true
            status = READY
            if(src != remoteHandle)
              remoteHandle ! msg
          }
        }
        case env@StorageEnvelope(src, msg : Learned)  => {
          debug("Learned message", env)
          //TODO update my value and version
          status match {
            case FORWARDED => {
              request match {
                case StorageEnvelope(origRequester, x: Propose) => {
                  debug("Master RecordHandler: "  + src + " Forward request. We inform the original requester:" + origRequester)
                  masterRecordHandler = src
                  origRequester ! msg
                  clear()
                }
                case _ =>
              }
            }
            case _ =>
          }
        }
        case env@StorageEnvelope(src,  GotMastership(newBallot)) if status == READY => {
          //TODO Block in the case of remote BeMaster
          debug("GotMastership message", env)
          val maxRound = max(ballots.head.startRound, newBallot.head.startRound)
          compareRanges(ballots, newBallot, maxRound) match {
            case -1 => {
              this.ballots = newBallot
              confirmedBallot = true
            }
            case 0 => {
              confirmedBallot = true
            }
            case -2 => {
              ballots = combine(ballots, newBallot, maxRound)
              confirmedBallot = false
            }
            case _ => { }
          }
        }
        case env@StorageEnvelope(src, msg:ResolveConflict) if status == READY => {
          debug("ResolveConflict request", env)
          request = env
          resolveConflict(src, msg)
        }
        case env@StorageEnvelope(src, msg: Propose) if status == READY => {
          debug("Propose request", env)
          request = env
          processProposal(src, msg)
        }
        case msg@_ => {
          logger.debug("Ignoring message in mailbox: %s %s", msg, status )
          mailbox.keepMsgInMailbox = true
        }
      }
  }

  def resolveConflict(src : ServiceType, msg : ResolveConflict){
    if(!areWeMaster(currentBallot.server)){
      //We are not responsible for doing the conflict resolution
      //So we go in Recovery mode
      currentBallot.server ! msg
      status = RECOVERY
      return

    }
    if( confirmedBallot && seq(msg.propose).forall(prop => value.commands.find(_.xid == prop.xid).isDefined)){
      //The resolve conflict is a duplicate and was already resolved
      src ! Recovered(key, value, MDCCMetadata(version, ballots, true, confirmedBallot))
      return
    }
    if(seq(msg.propose).forall(prop => unsafeCommands.find(_.xid == prop.xid).isDefined)){
      //We already have it on our todo list, so we just store the propose to be on the safe side
      forwardRequest(src, msg.propose)
      return
    }

    //First we need a new ballot
    val maxRound = max(msg.ballots.head.startRound, ballots.head.startRound)
    confirmedBallot = false
    ballots = compareRanges(ballots, msg.ballots, maxRound) match {
      case 0 =>
        ballots
      case 1 =>
        ballots
      case -1  | -2 => {
        combine(msg.ballots, ballots, maxRound)
      }
      case _ => throw new RuntimeException("Unvalid compare type")
    }
    startPhase1a(getOwnership(ballots, maxRound, maxRound, ballots.head.fast, master))
  }



  def processProposal(src: ServiceType, propose : Propose) : Unit = {
    val cBallot = currentBallot
    debug("Processing proposal source: %s, propose: %s", src, propose)
    if(confirmedBallot){
      if(cBallot.fast){
        status = FAST_PROPOSED
        debug("Sending fast propose from " + remoteHandle + " to " + servers.mkString(":"))
        servers.foreach(_ ! propose)
      }else{
        debug("We do have a confirmed ballot number")
        if(stableRound){
          val nBallot = getBallot(ballots, cBallot.round + 1).getOrElse(null)
          if(nBallot == null){
            debug("React to CSTABLE_NEXT_UNDEFINED")
            forwardRequest(src, propose)
            //Fast rounds are default
            requestNextFastRound()
            return
          }
          if(areWeMaster(nBallot.server)){
            debug("We start the next round with Phase2a")
            //We are the master, so lets do a propose
            startPhase2a(src, propose)
            return
          }else{
            //We are not the master, so we let the master handle it
            debug("We start the next round with forwarding the request we: %s ballot: %s", master, nBallot)
            status = FORWARDED
            nBallot.server ! propose
            return
          }
        }
        debug("The current round might not be decided version: %s ballot: %s", version, cBallot)
        if(areWeMaster(cBallot.server)){
          debug("We are the  master and we start a Phase2a")
          //We are the master, so lets do a propose
          startPhase2a(src, propose)
          return
        }else{
          //We are not the master, so we let the master handle it
          debug("We are not the master forward the request we: %s ballot: %s", master, currentBallot)
          status = FORWARDED
          currentBallot.server ! propose
          return
        }
      }
    }else{
      debug("We do NOT have a confirmed ballot number. Se we request the mastership first")
      forwardRequest(src, propose)//and we try it later again
      //If we have no idea about the ballot we just try to get a fast round accepted
      forwardRequest(remoteHandle, BeMaster(key,  cBallot.round, cBallot.round, true)) //the last one will be the first one
      return
    }
  }

  def startPhase1a(startRound: Long, endRound: Long, fast : Boolean)  : Unit =
    startPhase1a ( getOwnership(ballots, startRound, endRound, fast, master)   )

  def startPhase1a(ballots : Seq[MDCCBallotRange]) : Unit  = {
    debug("Starting Phase1a - ballots: %s", ballots)
    status = PHASE1A
    confirmedBallot = false
    this.ballots = ballots
    val phase1aMsg = Phase1a(key, ballots)
    responses.clear()
    servers ! phase1aMsg
  }

  //TODO: At the moment we agree on the full meta data, we could also just agree on the new value
  def processPhase1b(src : ServiceType, msg : Phase1b)  = {
    debug("Processing Phase1b: src:%s msg:%s", src, msg)
    assert(status == PHASE1A)
    //We take the max for comparing/combining ranges, as it only can mean, we are totally outdated
    val maxRound = max(msg.meta.ballots.head.startRound, ballots.head.startRound)
    compareRanges(ballots, msg.meta.ballots, maxRound) match {
      case 0 => { //The storage node accepted the new meta data
        responses += src -> msg
      }
      case 1 =>
        //We got an old message, we can ignore this case
      case -1  | -2 => {
        //TODO We need to ensure progress while two nodes try to get the mastership
        //There was a new meta data version out there, so we try it again
        startPhase1a(combine(msg.meta.ballots, ballots, maxRound))
      }
      case _ => assert(false) //should never happen
    }

    //We check what we need. If the current round is classic, we might need the next one
    val quorum = if(ballots.head.fast || ballots.tail.headOption.map(_.fast).getOrElse(false)) fastQuorum else classicQuorum

    if(responses.size >= quorum) {
      debug("We got the quorum and the mastership")
      confirmedBallot = true
      calculateCStructs()
      request match {
        case StorageEnvelope(src, x: BeMaster) => {
          src ! GotMastership(ballots)
          clear() //We are done
        }
        case StorageEnvelope(src, ResolveConflict(_,_, propose,requester)) => {
          clear() //We are done
          startPhase2a(requester, propose)
        }
        case _ => throw new RuntimeException("A Phase1a should always be triggered through BeMaster")
      }
    }
  }

  @inline def stableRound() : Boolean = {
    debug("Test round for stability: fast:%s version.round:%s currentBallot.round:%s value.commands.size:%s provedSafe.commands.size:%s",
      currentBallot.fast, version.round,
      currentBallot.round,
      value.commands.size,
      provedSafe.commands.size)
    !currentBallot.fast &&
      version.round ==  currentBallot.round &&
      value.commands.size > 0 &&
      value.commands.size == provedSafe.commands.size //if the size is the same, the commands have to be the same
  }


  private def calculateCStructs() = {
    var kBallot = MDCCBallot(-1, -1,null,true)
    var values : List[CStruct] =  Nil
    responses.foreach( v => {
      val msg : Phase1b  = v._2.asInstanceOf[Phase1b]
      kBallot.compare(msg.meta.currentVersion) match {
        case -1 => {
          values = msg.value :: Nil
          kBallot = msg.meta.currentVersion
        }
        case 0 => {
          values =  msg.value :: values
        }
        case 1 =>
      }
    })
    val kQuorum =  if(kBallot.fast) fastQuorum else classicQuorum
    val mQuorum =  if(ballots.head.fast) fastQuorum else classicQuorum
    var tmp = resolver.provedSafe(values, kQuorum, mQuorum, servers.size)
    provedSafe = tmp._1
    unsafeCommands = tmp._2
    if(values.size >= kQuorum){
      value = learn(values, kQuorum)
      version = kBallot
    }
  }

  private def learn(values : List[CStruct], quorum : Int) : CStruct =  {
    assert(values.size >= quorum)
    resolver.provedSafe(values, quorum, servers.size, servers.size)._1
  }

  @inline private def startOver() = {
    forwardRequest(request)
    clear()
  }

  @inline private def clear() = {
    request = null
    status = READY
  }

  @inline def classicQuorum = MDCCRecordHandler.classicQuorumSize(servers.size)
  @inline def fastQuorum = MDCCRecordHandler.fastQuorumSize(servers.size)

  def requestNextFastRound() = {
    forwardRequest(remoteHandle, BeMaster(key, ballots.head.startRound + 1, ballots.head.startRound + 10, true)) //Ok lets get a fast round
  }

//  private def !(msg : StorageMessage)(implicit sender: RemoteServiceProxy[StorageMessage]) = {
//    val remoteService = sender match {
//
//    }
//    mailbox.addFirst(Envelope()
//
//  } remoteHandle.!(msg)(sender)

  def kill = remoteHandle ! Exit()



  @inline def forwardRequest(src : ServiceType, msg : StorageMessage) : Unit = {
    msg match {
      case MultiPropose(proposes) =>  {
        proposes.size match {
          case 0 => //Its an empty propose so no need to send it
          case 1 => forwardRequest(Envelope(Some(src), proposes.head))
          case _ => forwardRequest(Envelope(Some(src), msg))
          }
        }
      case _ =>  forwardRequest(Envelope(Some(src), msg))
    }
  }

  @inline def forwardRequest(env : Envelope[StorageMessage]) : Unit = {
    mailbox.addFirst(env)
  }

  /**
   * Forwards a request and unsafe proposes
   */
  @inline def forwardRequest(src : ServiceType, propose : Propose,  unsafeCommands : Seq[SinglePropose] ) : Unit = {
    forwardRequest(src, propose)
    forwardRequest(src, MultiPropose(unsafeCommands))
  }

  def moveToNextRound() = {
    ballots = adjustRound(ballots, ballots.head.startRound + 1)
  }

  def startPhase2a(src : ServiceType, propose : Propose) : Unit = {
    responses.clear()
    debug("Starting Phase2a")
    status = PHASE2A
    val cBallot = topBallot(ballots)
    assert(areWeMaster(cBallot.server))

    if(cBallot.fast){
      debug("we are in a fast ballot")
      //We are just opening a fast next round as part of conflict resolution
      servers ! Phase2a(key, cBallot, value, Nil, Nil, unsafeCommands ++ seq(propose))
    }else{
      if(stableRound) {
        debug("We have a stable round")
        //OK we have a stable round, so we can just open the next round
        val nBallot = getBallot(ballots, cBallot.round + 1).getOrElse(null)
        //The round is clear and committed, time to move on
        debug("Rebasing")
        val (rebase, commitXids, abortXids) = resolver.compressCStruct(value)
        debug("Rebase done")
        if(nBallot.fast){
          debug("Next ballot is fast, we are opening a fast round ballot" + nBallot)
          moveToNextRound()
          servers ! Phase2a(key, nBallot, rebase, commitXids, abortXids, unsafeCommands ++ seq(propose))
        }else{
          //We are in the classic mode
          debug("Testing for pending updates")
          if(value.commands.exists(_.pending)){
            debug("We  have still pending update, so we postpone")
            forwardRequest(remoteHandle, propose, unsafeCommands)

            //We found a pending update, so we wait with moving on to the next round
            //Scheduler.schedule(() => {}, MDCCRecordHandler.WAIT_TIME )
            clear()
            status = WAITING_FOR_COMMIT
          }else{
            debug("No pending updates. We are ready to go")
            //The round is clear and committed, time to move on
            if(unsafeCommands.isEmpty){
              debug("Classic rounds: No pending updates and no unsafe commands, so we propose the next")
              val props = seq(propose)
              moveToNextRound()
              servers ! Phase2a(key, nBallot, rebase, commitXids, abortXids, props.head :: Nil)
              forwardRequest(src, MultiPropose(props.tail))
            }else{
              debug("Classic rounds: No pending updates but unsafe commands -> we resolve the unsafe commands first")
              moveToNextRound()
              servers ! Phase2a(key, nBallot, rebase, commitXids, abortXids, unsafeCommands.head :: Nil)
              forwardRequest(src, propose, unsafeCommands.tail)
            }
          }
        }
      }else{
        //The current round is instable or free
        if(value.commands.size < provedSafe.commands.size){
          debug("The current round is unstable, so we propose provedSafe again and postpone the rest")
          //The current round is still not stable, lets make it stable
          servers ! Phase2a(key, cBallot, provedSafe)
          forwardRequest(src, propose)
        }else if(value.commands.size == 0){
          //The round is still free, so lets use it.
          val nBallot = getBallot(ballots, cBallot.round + 1).getOrElse(null)
          val (rebase, commitXids, abortXids) = resolver.compressCStruct(value)
          if(nBallot == null || !nBallot.fast){
            //Next is classic, so one command at a time
            if(unsafeCommands.isEmpty){
              debug("Current classic round is still empty, and there are no unsafe commands. Perfect, we take the round")
              val props = seq(propose)
              servers ! Phase2a(key, cBallot, rebase, commitXids, abortXids, props.head :: Nil)
              forwardRequest(src, MultiPropose(props.tail))
            }else{
              debug("Current classic round is still empty, but we have unsafe commands")
              servers ! Phase2a(key, cBallot, rebase, commitXids, abortXids, unsafeCommands.head :: Nil)
              forwardRequest(src, propose, unsafeCommands.tail)
            }
          }else{
            debug("Current round is still unstable, but the next is fast, so better accept everything")
            //Next ballot is fast, so better try to accept everything
            servers ! Phase2a(key, cBallot, rebase, commitXids, abortXids, unsafeCommands ++ seq(propose))
          }
        }
      }
    }
  }

  def processPhase2bMasterFailure(src : ServiceType, msg : Phase2bMasterFailure) : Unit = {
     debug("Got master failure message, we abort %s %s", msg, src)
    //OK, a master failure can only mean our ballot number is not valid anymore
    val maxRound = max(ballots.head.startRound, msg.ballots.head.startRound)
    ballots = combine(ballots, msg.ballots, maxRound)
    confirmedBallot = msg.confirmed //it is only possible to change from true->false, it is impossible to change it otherwise
    startOver()
  }

  def processPhase2b(src : ServiceType, msg : Phase2b) : Unit = {
    debug("Received 2b message %s %s", msg, src)
    currentBallot.compare(msg.ballot) match {
      case 1 => {
        debug("We got an old message. We ignore it")
        return
      }
      case 0 => responses += src -> msg
      case -1 => throw new RuntimeException("Should never happen as the storage node should send a Phase2bMasterFailure message: current:" + currentBallot + " received:" + msg.ballot)
    }

    val quorum = if(currentBallot.fast) fastQuorum else classicQuorum
    if(responses.size >= quorum){
      debug("We got a quorum")
      val values = responses.map(_._2.asInstanceOf[Phase2b].value).toSeq
      val tmp = resolver.provedSafe(values, quorum, servers.size, servers.size)
      debug("ProvedSafe CStruct: %s", tmp._1)
      debug("Unsafe commands:s %s", tmp._2)
      value = tmp._1
      provedSafe = tmp._1
      unsafeCommands = tmp._2
      request match {
        case msg@StorageEnvelope(src, propose: SinglePropose)  =>  {
          val cmd = value.commands.find(_.xid == propose.xid)
          if(cmd.isDefined){
            debug("We learned the value, lets inform the requester. Informing src" + src)
            src ! Learned(propose.xid, key, cmd.get.commit)
          }else{
            debug("We did not learn our transaction.")
            if(ballots.head.fast) {
              debug("We are in a fast round. So it can only mean, that we have a conflict (or got too old messages)\nBetter to start conflict resolution")
              remoteHandle ! ResolveConflict(key, ballots, propose, src) //Using the original source to ensure that the requester gets notified
            }else{
              debug("We are in a classic round, so we just try it again")
              //TODO Implement back of to guarantee progress
              forwardRequest(msg)
            }
          }
        }
        case StorageEnvelope(src, req: MultiPropose)  =>  {
          debug("We got the sequence accepted")
          val missing = informLearners(src, req)
          if(missing.size == req.proposes.size){
            debug("We did not learn a single value, we should trigger conflict resolution")
            remoteHandle ! ResolveConflict(key, ballots, req, src)
          }else if(!missing.isEmpty){
            forwardRequest(src, MultiPropose(missing))
          }
        }
        case StorageEnvelope(src, ResolveConflict(key, ballots, propose, proposer)) => {
          debug("We recovered successfully")
          val missing = informLearners(src, propose)
           src ! Recovered(key, value, MDCCMetadata(version, ballots, true, confirmedBallot))
          if(!missing.isEmpty){
            debug("The recovery was successful, but we did not learn the propose")
            forwardRequest(src, MultiPropose(missing))
          }
        }
        case _ => throw new RuntimeException("Should never happen")
      }
      responses.clear()
      clear()
    }
  }

  def informLearners(src : ServiceType, proposes : Propose) : Seq[SinglePropose] = {
    debug("We are informing the learners")
    var missing = new ArrayBuffer[SinglePropose](1)
    seq(proposes).foreach(propose => {
      val cmd = value.commands.find(_.xid == propose.xid)
      if(cmd.isDefined){
        src ! Learned(propose.xid, key, cmd.get.commit)
      }else{
        debug("We got a missing proposal")
        //TODO: Do we need to trigger recovery?
        missing += propose
      }
      })
    missing
  }

}


object MDCCRecordHandler {
  final val WAIT_TIME = 20

  @inline def classicQuorumSize(servers: Int) = floor(servers.toDouble / 2.0).toInt + 1
  @inline def fastQuorumSize(servers: Int) = ceil(3.0 * servers.toDouble / 4.0).toInt
}
