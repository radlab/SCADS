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
import _root_.org.fusesource.hawtdispatch._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit._
import scala.util.Random


sealed trait RecordStatus {def name: String}
case object READY extends RecordStatus {val name = "READY"}
case object FORWARDED extends RecordStatus {val name = "PROPOSED"}    //TODO actually we do not need a Forwarded status --> Future work remove
case object FAST_PROPOSED extends RecordStatus {val name = "FAST_PROPOSED"}
case object PHASE1A  extends RecordStatus {val name = "PHASE1A"}
case object PHASE2A extends RecordStatus {val name = "PHASE2A"}
case object RECOVERY extends RecordStatus {val name = "RECOVERY"}
case object WAITING_FOR_COMMIT extends RecordStatus {val name = "WAITING_FOR_COMMIT"}
case object WAITING_FOR_NEW_FAST_ROUND extends RecordStatus {val name = "WAITING_FOR_NEW_ROUND"}
case object SCAN_FOR_PROPOSE extends RecordStatus {val name = "SCAN_FOR_PROPOSE"}

object ServerMessageHelper {
  class SMH(s: Seq[PartitionService]) {
    def !(msg : MDCCProtocol)(implicit sender: RemoteServiceProxy[StorageMessage]) = s.foreach(_ ! msg)
  }
  implicit def smh(i: Seq[PartitionService]) = new SMH(i)
}

object RoundStats{
    var fast = new AtomicInteger(0)
    var classic = new AtomicInteger(0)
    var forward = new AtomicInteger(0)
    var recovery = new AtomicInteger(0)

    monitor_hawtdispatch()

    def monitor_hawtdispatch() :Unit = {
        getGlobalQueue().after(30, SECONDS) {
        println("Round Stats -> Fast: " + fast + " Classic:" + classic + " Forward:" + forward +" Recovery:" + recovery)
        monitor_hawtdispatch
      }
    }
}


class MDCCRecordHandler (
       var key : Array[Byte],
       var value : CStruct,
       var version: MDCCBallot, //The version of the value
       var ballots : Seq[MDCCBallotRange], //Majority Ballot
       var confirmedBallot : Boolean, //Is the ballot confirmed by a majority?
       var servers: Seq[PartitionService],
       var resolver : ConflictResolver,
       var thisService : SCADSService //If we are master, this is outer not garbage collected remote handler
  ) {

  import ServerMessageHelper._

  //TODO we should always
  private var provedSafe : CStruct = value //Because of readCommitted property, that value is fine
  private var unsafeCommands : Seq[SinglePropose] = Nil

  private val logger = Logger(classOf[MDCCRecordHandler])

  private var responses =  new HashMap[ServiceType, MDCCProtocol]()

  val mailbox = new PlainMailbox[StorageMessage]("" + this.hashCode)

  implicit val remoteHandle = StorageService(StorageRegistry.registerFastMailboxFunc(processMailbox, mailbox))


  private var request : Envelope[StorageMessage] = null

  // HACK Needed to get the commit message through.
  // masterRecordHandler is the master, from which this service received a
  // Learned message.
  // masterRecordHandlerSent is the master, to which this service sent a
  // Propose message.
  var masterRecordHandler : Option[SCADSService] = None
  var masterRecordHandlerSent : Option[SCADSService] = None

  // Returns what might be the master, either the last sent propose, or what
  // metadata thinks.
  def possibleMasterRecordHandler = {
    if (masterRecordHandlerSent.isDefined) {
      masterRecordHandlerSent.get
    } else {
      currentBallot.server
    }
  }


  private var status: RecordStatus = READY

  type ServiceType =  RemoteServiceProxy[StorageMessage]

  @inline def debug(msg : String, items : scala.Any*) = logger.debug("id:" + remoteHandle.id + " key:" + (new ByteArrayWrapper(key)).hashCode() + ":" + status + " " + msg, items:_*)
  @inline def error(msg : String, items : scala.Any*) = logger.error("id:" + remoteHandle.id + " key:" + (new ByteArrayWrapper(key)).hashCode() + ":" + status + " " + msg, items:_*)
  @inline def fullDebug(msg : String, items : scala.Any*) = debug(
    "\n Current Meta-Data: " + ballots
    + "\n confirmedBallot:" + confirmedBallot
    + "\n Current Ballat:" + currentBallot
    + "\n Version:" + version
    + "\n Value:" + value
    + "\n ProvedSafe:" + provedSafe
    + "\n UnsafeCommands:" + unsafeCommands
    + "\n ThisService:" + thisService
    + "\n Mailbox: " +mailbox
    + msg, items:_*)


  override def toString = "[id:" + remoteHandle.id + " key:" + (new ByteArrayWrapper(key)).hashCode() + ":" + status + "]"

  debug("Created RecordHandler. Hash %s, Mailbox: %s", this.hashCode, mailbox.hashCode())

  implicit def toRemoteService(src : ServiceType) : RemoteService[StorageMessage] = src.asInstanceOf[RemoteService[StorageMessage]]

  implicit def extractSource(src : Option[RemoteService[IndexedRecord]]) = src.get

  //TODO Define mastership with one single actor


  override def hashCode() = key.hashCode()

  implicit def toStorageService(service : RemoteServiceProxy[StorageMessage]) : StorageService = StorageService(service)

  @inline def areWeMaster(master : SCADSService) : Boolean = {
    debug("Checking for mastership thisService: %s - master: %s == %s", thisService, master, master == thisService)
    val r = master == thisService
    r
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

  def commit(msg : MDCCProtocol, xid: ScadsXid, trxStatus : Boolean) : Boolean = {
    debug("Received xid %s status %s ", xid, trxStatus)
    val cmd = value.commands.find(_.xid == xid)
    if(cmd.isDefined){
      cmd.get.pending = false
      cmd.get.commit = trxStatus
      if(status == WAITING_FOR_COMMIT)
        status = READY
      debug("Commit was found and deleted: %s", xid)
      return false
    }else{
      debug("Commit is kept in mailbox: %s", xid)
      return true
    }
  }

  //TODO We should create a proper priority queue
  def processMailbox(mailbox : Mailbox[StorageMessage]) {
      debug("Mailbox %s %s", this.hashCode(), mailbox)
      if(mailbox.size() > 10) debug("################   PROBLEM #################################### %s", mailbox.size())
      mailbox{
        case env@StorageEnvelope(src, msg: Commit) => {
          debug("Processing Commit msg : %s", env)
          mailbox.keepMsgInMailbox= commit(msg, msg.xid, true)
        }
        case env@StorageEnvelope(src, msg: RecordCommit) => {
          debug("Processing RecordCommit msg : %s", env)
          mailbox.keepMsgInMailbox= commit(msg, msg.xid, true)
        }
        case env@StorageEnvelope(src, msg: Abort) => {
          debug("Processing Abort msg: %s", env)
          mailbox.keepMsgInMailbox = commit(msg, msg.xid, false)
        }
        case env@StorageEnvelope(src, msg: RecordAbort) => {
          debug("Processing RecordAbort msg: %s", env)
          mailbox.keepMsgInMailbox = commit(msg, msg.xid, false)
        }
        case env@StorageEnvelope(src, msg: Exit)  if status == READY => {
          debug("Processing EXIT request: %s", env)
          if(mailbox.size() == 1){
            debug("No pending request, we are ready to let go", env)
            StorageRegistry.unregisterService(remoteHandle)
          }else{
            debug("The queue is still not empty", env)
            mailbox.keepMsgInMailbox = true
          }
        }
        case env@StorageEnvelope(src, msg: BeMaster) if status == READY => {
          request = env
          debug("Processing BeMaster message", env)
          startBeMaster(src, msg)
        }
        case env@StorageEnvelope(src, msg: Phase1b) => {
          //we do the phase check afterwards to empty out old messages
          debug("Processing Phase1b message" , env)
          status match {
            case PHASE1A => processPhase1b(src, msg)
            case _ =>
          }
        }
        case env@StorageEnvelope(src, msg: Phase2bMasterFailure) => {
          debug("Processing Phase2bMasterFailure message" , env)
          status match {
            case PHASE2A => processPhase2bMasterFailure(src, msg)
            case FAST_PROPOSED => processPhase2bMasterFailure(src, msg)
            case _ =>
          }
        }
        case env@StorageEnvelope(src, msg: Phase2b) => {
          debug("Processing Phase2b message" , env)
          status match {
            case PHASE2A => processPhase2b(src, msg)
            case FAST_PROPOSED => processPhase2b(src, msg)
            case _ =>
          }
        }
        case env@StorageEnvelope(src, msg : Recovered) => {
          debug("Processing Recovered message", env)
          if (status == RECOVERY) {
            this.value = msg.value
            this.version = msg.meta.currentVersion
            this.ballots = msg.meta.ballots
            confirmedBallot = true
            status = READY
            debug("We recovered and updated the meta data value:%s version:%s ballots:%s message:%s", value, version, ballots, msg)

            request match {
              case r@StorageEnvelope(src2, ResolveConflict(_, _, propose: SinglePropose, requester)) => {
                // Assume key is the same as this.key.
                val cmd = this.value.commands.find(_.xid == propose.xid)
                if(cmd.isDefined) {
                  debug("processed recovered message: %s src: %s remoteHandle: %s inform: orig request: %s cmd: %s requester: %s", msg, src, remoteHandle, request, cmd, requester)
                  requester ! Learned(propose.xid, key, cmd.get.commit)
                } else {
                  debug("recovered cmd is none: %s orig request: %s", this.value.commands, request)
                  // Can this happen?  The point of the recovery was to decide
                  // what happened to this xid.
                }
              }
              case _ => // Do we need to handle other types of requests here?
                debug("unexpected orig request: %s", request)
            }

            // TODO: Is this correct and/or required?  The remoteHandle is
            //       the handle for this record handler, so why send this
            //       message back to this handler again?  This means every
            //       Recovered() message happens twice, where the second one
            //       is ignored.  Is the second one required?
//            if(src != remoteHandle) {
//              remoteHandle ! msg
//            }
          }
        }
        case env@StorageEnvelope(src, msg : Learned)  => {
          debug("Processing Learned message %s", env)
          //TODO update my value and version
          request match {
            case StorageEnvelope(origRequester, x: SinglePropose) => {
              status match {
                case FORWARDED | FAST_PROPOSED => {
                  val xidMatch = msg.xid == x.xid
                  debug("Got Learned. Master RecordHandler: "  + src + " We inform the original requester:" + origRequester + " propose: " + x + " origRequest: " + request + " xidMatch: " + xidMatch)
                  if (xidMatch) {
                    masterRecordHandler = Some(src)
                    origRequester ! msg
                    clear()
                  } else {
                    error("Ignoring Learned message with wrong xid. src: %s msg: %s currentRequest: %s", src, msg, request)
                  }
                }
                case _ => {
                  error("We got a learned message without forwarding or fast propose. src: %s msg: %s currentRequest: %s", src, msg, request)
                }
              }
            }
            case _ => {
              error("We got a learned message, but not for a SinglePropose request. What should we do. src: %s msg: %s currentRequest: %s", src, msg, request)
            }
          }
        }
        case env@StorageEnvelope(src,  GotMastership(newBallot)) if status == READY || status == WAITING_FOR_NEW_FAST_ROUND => {
          //TODO Block in the case of remote BeMaster
          debug("Processing GotMastership message", env)
          val maxRound = max(ballots.head.startRound, newBallot.head.startRound)
          compareRanges(ballots, newBallot, maxRound) match {
            case -1 => {
              debug("Our current range is smaller: old Ballat:%s new Ballot:%s max Round: %s", ballots, newBallot, maxRound)
              this.ballots = newBallot
              confirmedBallot = true
            }
            case 0 => {
              debug("Its the same ballot: old Ballat:%s new Ballot:%s max Round: %s", ballots, newBallot, maxRound)
              confirmedBallot = true
            }
            case -2 => {
              debug("The ballots are not compatible:%s new Ballot:%s max Round: %s", ballots, newBallot, maxRound)
              ballots = combine(ballots, newBallot, maxRound)
              confirmedBallot = false
            }
            case _ => {
              debug("The new ballot is older than ours. our ballot:%s new Ballot:%s max Round: %s", ballots, newBallot, maxRound)
            }
          }
          status = READY
        }
        case env@StorageEnvelope(src, msg:ResolveConflict) if status == READY => {
          debug("Processing ResolveConflict request", env)
          request = env
          resolveConflict(src, msg)
        }
        case env@StorageEnvelope(src, msg: Propose) if status == READY => {
          debug("Processing Propose request", env)
          request = env
          processProposal(src, msg)
        }
        case env@StorageEnvelope(src, msg: SinglePropose) if status == SCAN_FOR_PROPOSE => {
          val sfp = request.msg.asInstanceOf[ScanForPropose]
          if (sfp.learned.xid == msg.xid) {
            debug("Notifying propose sender of old learned message src: %s, propose: %s, request: %s", src, env, request)
            src ! request.msg.asInstanceOf[ScanForPropose].learned
          } else {
            // Skip over this message, since it does not have the same xid.
            mailbox.keepMsgInMailbox = true
          }
        }
        case env@StorageEnvelope(src, msg: ScanForPropose) if status == READY => {
          debug("Processing ScanForPropose %s", env)
          request = env
          status = SCAN_FOR_PROPOSE
        }
        case env@StorageEnvelope(src, msg: DoneScanForPropose) if status == SCAN_FOR_PROPOSE => {
          debug("Done ScanForPropose %s", env)
          request = null
          status = READY
        }
        case msg@_ => {
          debug("Mailbox-Hash:%s, Ignoring message in mailbox: msg:%s status: %s current request:%s", mailbox.hashCode(), msg, status, request)
          if(status == WAITING_FOR_COMMIT){
            debug("Value: %s, Version: %s, Ballots: %s, confirmed: %s", value, version, ballots, confirmedBallot)
          }
          mailbox.keepMsgInMailbox = true
        }
      }
  }



  def resolveConflict(src : ServiceType, msg : ResolveConflict){
    if(!areWeMaster(currentBallot.server)){
      //We are not responsible for doing the conflict resolution
      //So we go in Recovery mode
      debug("sending resolve conflict: %s to %s", msg, currentBallot.server)
      currentBallot.server ! msg
      RoundStats.recovery.incrementAndGet()
      status = RECOVERY
      return

    }
    if( confirmedBallot && seq(msg.propose).forall(prop => value.commands.find(_.xid == prop.xid).isDefined)){
      //The resolve conflict is a duplicate and was already resolved
      val seqXids = seq(msg.propose).map(_.xid).mkString(",")
      debug("already recovered xids: [%s], sending recovered back to %s", seqXids, src)
      src ! Recovered(key, value, MDCCMetadata(version, ballots, true, confirmedBallot))
      return
    }
    if(seq(msg.propose).forall(prop => unsafeCommands.find(_.xid == prop.xid).isDefined)){
      //We already have it on our todo list, so we just store the propose to be on the safe side
      debug("recovering, forward request")
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
    debug("recovering, starting phase1a, request: %s ballot: %s", request, ballots)
    var nBallots = getOwnership(ballots, maxRound, maxRound, false, thisService)
    nBallots = getOwnership(ballots, maxRound + 1, maxRound +1 , true, thisService)
    startPhase1a(nBallots)
  }



  def fastPropose(propose : Propose){
    //val rnd = Random
    status = FAST_PROPOSED
    debug("Sending fast propose from " + remoteHandle + " to [" + servers.mkString(", ") + "]")
    servers.foreach(server => {
      //Thread.sleep(rnd.nextInt(1000))
      server ! propose
    })
    RoundStats.fast.incrementAndGet()
  }

  def forwardPropose(masterServer: SCADSService, propose : Propose){
    status = FORWARDED
    masterRecordHandlerSent = Some(masterServer)
    masterServer ! propose
    RoundStats.forward.incrementAndGet()
  }

  def createBeMasterRequest( startRound: Long, endRound: Long, fast : Boolean) : BeMaster = {
    val overlappingBallots =  ballots.filter(range => range.endRound >= startRound && range.startRound <= endRound)
    val maxVote :Long = if(overlappingBallots.isEmpty) 0 else  overlappingBallots.maxBy(_.vote).vote
    val beMasterMsg = BeMaster(key, startRound, endRound, maxVote, fast)
    debug("Created BeMaster messages: %s", beMasterMsg)
    beMasterMsg
  }

  def processProposal(src: ServiceType, propose : Propose) : Unit = {
    //TODO Gene: Filter out duplicates
    responses.clear()
    val cBallot = currentBallot
    debug("Processing proposal source: %s, propose: %s", src, propose)
    if(confirmedBallot){
      if(cBallot.fast){
        fastPropose(propose)
      }else{
        debug("We do have a confirmed ballot number")
        if(stableRound){
          val nBallot = getBallot(ballots, cBallot.round + 1).getOrElse(null)
          if(nBallot == null) {
            debug("React to CSTABLE_NEXT_UNDEFINED")
            forwardRequest(src, propose)
            //Fast rounds are default
            val beMasterRequest =  createBeMasterRequest(ballots.head.startRound + 1, ballots.head.startRound + 1, true)
            if(areWeMaster(cBallot.server)) {
              debug("We are supposed to be the master, so we send ourself a message")
              forwardRequest(remoteHandle, beMasterRequest)
            } else {
              debug("We are not the master and currently we do not do master changes. So we send the master request to %s", currentBallot.server)
              currentBallot.server ! beMasterRequest
              status = WAITING_FOR_NEW_FAST_ROUND
            }
            return
          }
          if(areWeMaster(nBallot.server)){
            debug("We start the next round with Phase2a")
            //We are the master, so lets do a propose
            startPhase2a(src, propose)
            return
          }else{
            //We are not the master, so we let the master handle it
            debug("We start the next round with forwarding the request we: %s ballot: %s", thisService, nBallot)
            forwardPropose(nBallot.server, propose)
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
          debug("We are not the master forward the request we: %s ballot: %s", thisService, currentBallot)
          forwardPropose(currentBallot.server, propose)
          return
        }
      }
    }else{
      //TODO Our handling of outdated metadata can cause a cycle of forwards. We need to fix this
      if(cBallot.fast){
        if(areWeMaster(cBallot.server)){
          error("We are the master and we do not have a confirmed ballot number. This is super wierd. We try to get the master ship")
          forwardRequest(remoteHandle, createBeMasterRequest(cBallot.round, cBallot.round, true))
        }else{
          debug("We have a not confirmed meta data but most likely we are in a fast round anyway. So we simply try it")
          fastPropose(propose)
        }
      }else{
        if(areWeMaster(cBallot.server)){
          debug("We do NOT have a confirmed ballot number. Se we request the mastership first")
          forwardRequest(src, propose)
          forwardRequest(remoteHandle, createBeMasterRequest(cBallot.round, cBallot.round, false))
        }else{
          debug("We do NOT have a confirmed ballot number and we are not supposed to be the master. So we forward everything to %s", currentBallot.server)
          forwardPropose(currentBallot.server, propose)
        }
      }
      return
    }
  }

  def startBeMaster(src : ServiceType, msg : BeMaster) = {
    val cBallot = currentBallot
    if(confirmedBallot &&
      areWeMaster(cBallot.server) &&
      ballots.filter(range => range.endRound >= msg.startRound && range.startRound <= msg.endRound).forall(_.vote <= msg.maxVote) &&
      ballots.last.endRound >= msg.endRound &&
      cBallot.fast == msg.fast ){
      debug("Got BeMaster message, but we already are the master")
      src ! GotMastership(ballots)
      clear()
    } else if (msg.startRound < cBallot.round) {
      debug("Got BeMaster message, but it is for an old round. Just reply to src: %s, cBallot: %s, ballots: %s, msg: %s", src, cBallot, ballots, msg)
      src ! GotMastership(ballots)
      clear()
    } else {
      debug("BeMaster starting phase1a. cBallot: %s, ballots: %s, msg: %s, src: %s", cBallot, ballots, msg, src)
      startPhase1a(msg.startRound, msg.endRound, msg.fast)
    }
  }

  def startPhase1a(startRound: Long, endRound: Long, fast : Boolean) : Unit =
    startPhase1a(getOwnership(ballots, startRound, endRound, fast, thisService))

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
    //TODO make sure we use the right startRound  --> Abort and restart
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
          debug("finished BeMaster. send GotMastership to %s.  ballots: %s", src, ballots)
          src ! GotMastership(ballots)
          clear() //We are done
        }
        case StorageEnvelope(src, ResolveConflict(_,_, propose,requester)) => {
          startPhase2a(src, propose)
        }
        case _ => throw new RuntimeException("A Phase1a should always be triggered through BeMaster")
      }
    }
  }

  @inline def stableRound() : Boolean = {
    val r = !currentBallot.fast &&
      version.round ==  currentBallot.round &&
      value.commands.size > 0 &&
      value.commands.size == provedSafe.commands.size //if the size is the same, the commands have to be the same

    debug("Test round for stability: fast:%s version.round:%s currentBallot.round:%s value.commands.size:%s provedSafe.commands.size:%s ==> %s",
      currentBallot.fast, version.round,
      currentBallot.round,
      value.commands.size,
      provedSafe.commands.size,
      r)
    r
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
    debug("calculateCStructs: kQuorum: %s mQuorum: %s server-size: %s values: %s value: %s provedSafe: %s", kQuorum, mQuorum, servers.size, values, value, tmp._1)
    provedSafe = tmp._1
    unsafeCommands = tmp._2
    if(values.size >= kQuorum){
      value = learn(values, kQuorum)
      debug("learned: value %s version %s", value, version)
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
    responses.clear()
    request = null
    status = READY
  }

  @inline def classicQuorum = MDCCRecordHandler.classicQuorumSize(servers.size)
  @inline def fastQuorum = MDCCRecordHandler.fastQuorumSize(servers.size)


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
      RoundStats.classic.incrementAndGet()
    }else{

      if(stableRound) {
        debug("We have a stable round")
        //OK we have a stable round, so we can just open the next round
        val nBallot = getBallot(ballots, cBallot.round + 1).getOrElse(null)
        //The round is clear and committed, time to move on
        debug("Rebasing value before: %s", value)
        val (rebase, commitXids, abortXids) = resolver.compressCStruct(value)
        debug("Rebase done. value: %s", rebase)
        if (nBallot == null) {
          debug("nullBallot1 ballots: %s, cBallot: %s, nround: %d", ballots, cBallot, cBallot.round + 1)
        } else {
          debug("nullBallot2 ballots: %s, cBallot: %s, nround: %d", ballots, cBallot, cBallot.round + 1)
        }
        if(nBallot.fast) {
          debug("Next ballot is fast, we are opening a fast round ballot. nBallot: %s cBallot: %s ballots: %s", nBallot, cBallot, ballots)
          moveToNextRound()
          servers ! Phase2a(key, nBallot, rebase, commitXids, abortXids, unsafeCommands ++ seq(propose))
          RoundStats.classic.incrementAndGet()
        } else {
          //We are in the classic mode
          debug("Testing for pending updates")
          if(false && value.commands.exists(_.pending)) {
            //We have to add the optional waiting
            debug("We  have still pending update, so we postpone: Value: %s", value)
            clear()
            forwardRequest(src, propose, unsafeCommands)
            //We found a pending update, so we wait with moving on to the next round
            //Scheduler.schedule(() => {}, MDCCRecordHandler.WAIT_TIME )

            status = WAITING_FOR_COMMIT

          } else {
            debug("No pending updates. We are ready to go")
            //The round is clear and committed, time to move on
            if(unsafeCommands.isEmpty) {

              val props = seq(propose)
              moveToNextRound()
              val msg = Phase2a(key, nBallot, rebase, commitXids, abortXids, props.head :: Nil)
              debug("Classic rounds: No pending updates and no unsafe commands, so we propose the next. msg: %s tail: %s current value: %s, rebased value: %s", msg, props.tail, value, rebase)
              servers ! msg
              RoundStats.classic.incrementAndGet()
              forwardRequest(src, MultiPropose(props.tail))
            } else {
              error("Classic rounds: No pending updates but unsafe commands -> we resolve the unsafe commands first")
              fullDebug("Unsafe commands")
              moveToNextRound()
              servers ! Phase2a(key, nBallot, rebase, commitXids, abortXids, unsafeCommands.head :: Nil)
              RoundStats.classic.incrementAndGet()
              forwardRequest(src, propose, unsafeCommands.tail)
            }
          }
        }
      } else {
        //The current round is instable or free
        if(value.commands.size < provedSafe.commands.size) {
          debug("The current round is unstable, so we propose provedSafe again and postpone the rest")
          //The current round is still not stable, lets make it stable
          servers ! Phase2a(key, cBallot, provedSafe)
          forwardRequest(src, propose)
        } else if(value.commands.size == 0) {
          //The round is still free, so lets use it.
          val nBallot = getBallot(ballots, cBallot.round + 1).getOrElse(null)
          val (rebase, commitXids, abortXids) = resolver.compressCStruct(value)
          if(nBallot == null || !nBallot.fast) {
            //Next is classic, so one command at a time
            if(unsafeCommands.isEmpty) {
              debug("Current classic round is still empty, and there are no unsafe commands. Perfect, we take the round")
              val props = seq(propose)
              servers ! Phase2a(key, cBallot, rebase, commitXids, abortXids, props.head :: Nil)
              RoundStats.classic.incrementAndGet()
              forwardRequest(src, MultiPropose(props.tail))
            } else {
              fullDebug("Current classic round is still empty, but we have unsafe commands")
              servers ! Phase2a(key, cBallot, rebase, commitXids, abortXids, unsafeCommands.head :: Nil)
              forwardRequest(src, propose, unsafeCommands.tail)
            }
          } else {
            debug("Current round is still unstable, but the next is fast, so better accept everything")
            //Next ballot is fast, so better try to accept everything
            servers ! Phase2a(key, cBallot, rebase, commitXids, abortXids, unsafeCommands ++ seq(propose))
            RoundStats.classic.incrementAndGet()
          }
        } else {
          error("We have a value bigger than the provedSafe size. That should never happen. ")
          error("Current Meta-Data: %s, confirmedBallot: %s, Current Ballat: %s,  Version: %s, Value: %s ProvedSafe: %s, UnsafeCommands: %s, thisService: %s mailbox: %s",
            ballots,
            confirmedBallot,
            currentBallot,
            version,
            value,
            provedSafe,
            unsafeCommands,
            thisService,
            mailbox)
          assert(false)
        }
        // This does not have to return anything.
        // stableRound
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
    val cmp = currentBallot.compare(msg.ballot)
    if (cmp > 0) {
      debug("We got an old message. We ignore it")
      return
    } else {
      if (!currentBallot.fast) {
        assert(cmp == 0, "Should never happen: somebody stole our mastership and we did not a Receive a Phase2bMaster Failure message. current ballot:" + currentBallot + " Phase2b msg:" + msg )
        responses += src -> msg
      } else {
        request match {
          case StorageEnvelope(_, propose: SinglePropose) => {
            if (msg.value.commands.exists(_.xid == propose.xid)) {
              if(cmp < 0){
                if(responses.size > 0){
                  debug("We already got responses with a different ballot number. We therefore abort and start over msg:%s, ballots: %s", msg, ballots)
                  startOver()
                  return
                }
                val newMeta = combine(msg.ballot, ballots)
                debug("We did a fast propose but have outdated meta data. So we brought it up to date. msg: %s, current: %s, new: %s", msg, ballots, newMeta)
                ballots = newMeta
              }
              debug("We got a valid Phase2b message for a fast round")
              responses += src -> msg
            } else {
              debug("We got an old Phase2b Fast message. We ignore it")
              return
            }
          }
          case _  => {
            error("We see multi-proposes. Not sure if this code is actually correct.")
            responses += src -> msg
          }
        }
      }
    }

    val quorum = if(currentBallot.fast) fastQuorum else classicQuorum
    debug("Current quorum size size: %s quorum: %s", responses.size ,quorum)
    if(responses.size >= quorum){
      debug("We got a quorum")
      val values = responses.map(_._2.asInstanceOf[Phase2b].value).toSeq
      val tmp = resolver.provedSafe(values, quorum, servers.size, servers.size)
      debug("Old value: %s", value)
      value = tmp._1
      provedSafe = tmp._1
      unsafeCommands = tmp._2
      version = msg.ballot
      debug("ProvedSafe CStruct: %s, Unsafe commands:s %s, Value: %s request: %s", provedSafe, unsafeCommands, value, request)
      if(!unsafeCommands.isEmpty){
        fullDebug("We have unsafe commands \n Responses: %s \n Values: %s", responses, value)
      }
      request match {
        case msg@StorageEnvelope(src, propose: SinglePropose)  =>  {
          val cmd = value.commands.find(_.xid == propose.xid)
          val oldCommit = responses.toList.map(_._2.asInstanceOf[Phase2b].oldCommits).reduce((a, b) => a ++ b).find(_.xid == propose.xid)
          if(cmd.isDefined) {
            debug("We learned the value")
            if(currentBallot.fast && !cmd.get.commit && cmd.get.command.isInstanceOf[LogicalUpdate]){
              debug("We learned an abort in a fast classic round with logical updates. This can only happend when we violate the limit. So we switch to classic. send BeMaster to %s", currentBallot.server)
              currentBallot.server ! createBeMasterRequest( currentBallot.round, currentBallot.round + 1000, false)

            }
            debug("We inform the requester about the learned value. cmd: %s src: %s, propose: %s", cmd, src, propose)
            val l = Learned(propose.xid, key, cmd.get.commit)
            src ! l
            addScanForProposeMsg(l)
          } else if (!oldCommit.isEmpty) {
            debug("Propose was already committed in the past. We inform the requester about the learned value. cmd: %s src: %s, propose: %s", cmd, src, propose)
            val l = Learned(propose.xid, key, oldCommit.get.commit)
            src ! l
            addScanForProposeMsg(l)
          } else {
            debug("We did not learn our transaction. TxId: %s quorum %s server-size %s - values %s - provedSafe %s - unsafeCommands %s - full responses: %s", propose.xid, quorum, servers.size, values, provedSafe, unsafeCommands, responses)
            if(ballots.head.fast) {
              debug("We are in a fast round. So it can only mean, that we have a conflict (or got too old messages) Better to start conflict resolution")
              remoteHandle ! ResolveConflict(key, ballots, propose, src) //Using the original source to ensure that the requester gets notified
            } else {
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
        case StorageEnvelope(src, ResolveConflict(key, oldBallots, propose, proposer)) => {
          debug("We recovered successfully")
          val missing = informLearners(src, propose)
           src ! Recovered(key, value, MDCCMetadata(version, ballots, true, confirmedBallot))
          if(!missing.isEmpty){
            debug("The recovery was successful, but we did not learn the propose")
            forwardRequest(src, MultiPropose(missing))
          }
        }
        case _ => throw new RuntimeException("Should never happen. request: " + request)
      }
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

  def addScanForProposeMsg(l: Learned) = {
    // remoteHandle is only used as a dummy value.
    mailbox.addFirst(Envelope(remoteHandle, ScanForPropose(l)))
    mailbox.addFirst(Envelope(remoteHandle, DoneScanForPropose(l)))
  }
}


object MDCCRecordHandler {
  final val WAIT_TIME = 20

  @inline def classicQuorumSize(servers: Int) = floor(servers.toDouble / 2.0).toInt + 1
  @inline def fastQuorumSize(servers: Int) = ceil(3.0 * servers.toDouble / 4.0).toInt
}
