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


sealed trait RecordStatus {def name: String}
case object EXIT
case object READY extends RecordStatus {val name = "READY"}
case object FORWARDED extends RecordStatus {val name = "PROPOSED"}    //TODO actually we do not need a Forwarded status --> Future work remove
case object FAST_PROPOSED extends RecordStatus {val name = "FAST_PROPOSED"}
case object PHASE1A  extends RecordStatus {val name = "PHASE1A"}
case object PHASE2A extends RecordStatus {val name = "PHASE2A"}
case object LEARNED_ABORT extends RecordStatus {val name = "LEARNED_ABORT"}
case object LEARNED_ACCEPT extends RecordStatus {val name = "LEARNED_ACCEPT"}
case object LEARNING extends RecordStatus {val name = "LEARNING"}
case object RECOVERY extends RecordStatus {val name = "RECOVERY"}

sealed trait BallotStatus {def name : String}
case object FAST_BALLOT extends BallotStatus {val name = "FAST_BALLOT"}

case object VOTED_SWITCH_TO_CLASSIC extends BallotStatus {val name = "VOTED_SWITCH_TO_CLASSIC"}
case object UNVOTED_SWITCH_TO_CLASSIC extends BallotStatus {val name = "UNVOTED_SWITCH_TO_CLASSIC"}

case object CSTABLE_VOTED_NEXT_CLASSIC extends BallotStatus {val name = "CSTABLE_VOTED_NEXT_CLASSIC"}
case object CSTABLE_UNVOTED_NEXT_CLASSIC extends BallotStatus {val name = "CSTABLE_UNVOTED_NEXT_CLASSIC"}
case object CSTABLE_UNVOTED_NEXT_FAST extends BallotStatus {val name = "CSTABLE_UNVOTED_NEXT_FAST"}
case object CSTABLE_NEXT_UNDEFINED extends BallotStatus {val name = "CSTABLE_NEXT_UNDEFINED"}
case object CUNSTABLE extends BallotStatus {val name = "CUNSTABLE"}

object ServerMessageHelper {
  class SMH(s: Seq[PartitionService]) {
    def !(msg : MDCCProtocol)(implicit sender: RemoteServiceProxy[StorageMessage]) = s.foreach(_ ! msg)
  }
  implicit def smh(i: Seq[PartitionService]) = new SMH(i)
}



class MCCCRecordHandler (
       var key : Array[Byte],
       var value : CStruct,
       var servers: Seq[PartitionService],
       var version: MDCCBallot, //The version of the value
       var ballots : Seq[MDCCBallotRange], //Majority Ballot
       var confirmedBallot : Boolean, //Is the ballot confirmed by a majority?
       var confirmedVersion : Boolean, //Is the version confirmed by a majority?
       var resolver : ConflictResolver
  ) {
  protected val logger = Logger(classOf[MCCCRecordHandler])
  @inline def debug(msg : String, items : scala.Any*) = logger.debug("" + key + ":" + status + " " + msg, items:_*)

  import ServerMessageHelper._

  private var unsafeCommands : Seq[SinglePropose] = Nil

  type ServiceType =  RemoteServiceProxy[StorageMessage]

  implicit def toRemoteService(src : ServiceType) : RemoteService[StorageMessage] = src.asInstanceOf[RemoteService[StorageMessage]]

  implicit def extractSource(src : Option[RemoteService[IndexedRecord]]) = src.get

  //TODO: is this really a storageservice?
  val mailbox = new PlainMailbox[StorageMessage]()
  implicit val remoteHandle = StorageService(StorageRegistry.registerFastMailboxFunc(processMailbox, mailbox))

  private var status: RecordStatus = READY
  private var responses =  new HashMap[ServiceType, MDCCProtocol]()

  private var request : Envelope[StorageMessage] = null

  override def hashCode() = key.hashCode()

  implicit def toStorageService(service : RemoteServiceProxy[StorageMessage]) : StorageService = StorageService(service)


  override def equals(that: Any): Boolean = that match {
     case other: MCCCRecordHandler => key == other.key
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

  /**
   * Returns (revalidationRequired, nextBallot)
   */
  def ballotStatus : BallotStatus = {
    validate(version, ballots)
    if(ballots.head.fast){
      return FAST_BALLOT   //if ballots are fast, our round has to be fast and ongoing
    } else {
      if (version.fast) {
        //The current round is fast, but not the next one
        if(confirmedBallot){
          return VOTED_SWITCH_TO_CLASSIC
        }else{
          return UNVOTED_SWITCH_TO_CLASSIC
        }
      }else{
        //Current round is classic
        if(confirmedVersion){
          //Current round is stable, lets check how the next one is
          val nextRange = getRange(ballots, version.round + 1)
          if (nextRange.isDefined) {
            if(nextRange.get.fast){
              if(confirmedBallot){
                //Mhhh, that should actually never happen.
                //Anyway, we can handle it easily
                return FAST_BALLOT
              }else{
                return CSTABLE_UNVOTED_NEXT_FAST
              }
            }else{
              //OK, so we have a next classic round
              if(confirmedBallot){
                return CSTABLE_VOTED_NEXT_CLASSIC
              }else{
                return CSTABLE_UNVOTED_NEXT_CLASSIC
              }
            }
          }else{
            return CSTABLE_NEXT_UNDEFINED
          }
        }else{
          return CUNSTABLE
        }
      }
    }
  }

  @inline private def seq(propose : Propose) :Seq[SinglePropose]= {
    propose match {
      case s : SinglePropose => s :: Nil
      case MultiPropose(seq) => seq
    }
  }

  def getStatus = status

  //TODO We should create a proper priority queue
  def processMailbox(mailbox : Mailbox[StorageMessage]) {
      mailbox {
        case Envelope(src, msg: Commit) => {
          servers ! msg
        }
        case Envelope(src, msg: Abort) => {
          servers ! msg
        }
        case Envelope(src, msg: Exit)  if status == READY => {
          debug("EXIT request")
          StorageRegistry.unregisterService(remoteHandle)
        }
        //Highest priority
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
            confirmedVersion = true
            status = READY
            if(src != remoteHandle)
              remoteHandle ! msg
          }
        }
        case env@StorageEnvelope(src, msg : Learned)  => {
          debug("Learned message", env)
          status match {
            case FORWARDED => {
              request match {
                case StorageEnvelope(src, x: Propose) => {
                  src ! msg
                  clear()
                }
                case _ =>
              }
            }
            case _ =>
          }
        }
        case env@StorageEnvelope(src,  GotMastership(newBallot)) if status == READY => {
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
        case env@StorageEnvelope(src, msg: Propose) if status == READY=> {
          debug("Propose request", env)
          request = env
          processProposal(src, msg)
        }
        case msg@_ => {
          logger.debug("Ignoring message in mailbox: %s", msg)
          mailbox.keepMsgInMailbox = true
        }
      }
  }

  def resolveConflict(src : ServiceType, msg : ResolveConflict){
    if(currentBallot.server != remoteHandle){
      //We are not responsible for doing the conflict resolution
      //So we go in Recovery mode
      currentBallot.server ! msg
      status = RECOVERY
      return

    }
    if(confirmedVersion && this.confirmedBallot && seq(msg.propose).forall(prop => value.commands.find(_.xid == prop.xid).isDefined)){
      //The resolve conflict is a duplicate and was already resolved
      src ! Recovered(key, value, MDCCMetadata(version, ballots))
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
    startPhase1a(getOwnership(ballots, maxRound, maxRound, ballots.head.fast))
  }


  def processProposal(src: ServiceType, propose : Propose) : Unit = {
    debug("Processing proposal", src, propose)
    ballotStatus match {
     case FAST_BALLOT => {
        status = FAST_PROPOSED
        debug("Sending fast propose from " + remoteHandle + " to " + servers.mkString(":"))
        servers.foreach(_ ! propose)
      }
      case VOTED_SWITCH_TO_CLASSIC => {
        //We are in a classic round
        if(currentBallot.server == this.remoteHandle){
          startPhase2a(src, propose)
        }else{
          status = FORWARDED
          currentBallot.server ! propose
        }
      }
      case UNVOTED_SWITCH_TO_CLASSIC => {
        //TODO store ResolveConflict status
       remoteHandle ! ResolveConflict(key, ballots, propose, StorageService(src))
       debug("Request ResolveConflict " + version)
      }
     case CSTABLE_VOTED_NEXT_CLASSIC | CSTABLE_UNVOTED_NEXT_CLASSIC => {
       val ballot = getBallot(ballots, version.round + 1).get
        if(ballot.server == this.remoteHandle){
            //We are the master for the next round, so lets go
            if(confirmedBallot) {
              //The ballot is valid, lets move to phase2
              startPhase2a(src, propose)
            }else{
              //we might have an invalid ballot, lets renew it
              debug("Request BeMaster " +  version.round + 1)
              ballot.server ! BeMaster(key,  version.round + 1, version.round + 1, false)
              forwardRequest(src, propose)//and we try it later again
            }
          }else{
            //We are not the master, but we might know who is
            status = FORWARDED
            ballot.server ! propose //TODO we need to avoid cycles
        }
     }
     case CSTABLE_NEXT_UNDEFINED => {
       debug("React to CSTABLE_NEXT_UNDEFINED")
       remoteHandle ! BeMaster(key, version.round + 1, version.round + 1, true) //Ok lets get a fast round
       forwardRequest(src, propose)
     }
     case CUNSTABLE => {
      debug("React to CUNSTABLE")
      remoteHandle ! ResolveConflict(key, ballots, propose, StorageService(src))
     }
  }
  }

  def startPhase1a(startRound: Long, endRound: Long, fast : Boolean)  : Unit =
    startPhase1a ( getOwnership(ballots, startRound, endRound, fast)   )

  def startPhase1a(ballots : Seq[MDCCBallotRange]) : Unit  = {
    debug("Starting Phase1a", ballots)
    status == PHASE1A
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
    val maxRound = max(msg.meta.currentVersion.round, version.round)
    compareRanges(ballots, msg.meta.ballots, maxRound) match {
      case 0 =>
        //The storage node accepted the new meta data
        responses += src -> msg
      case 1 =>
        //We got an old message, we can ignore this case
      case -1  | -2 => {
        //TODO We need to ensure progress while two nodes try to get the mastership
        //There was a new meta data version out there, so we try it again
        startPhase1a(combine(msg.meta.ballots, ballots, maxRound))
      }
      case _ => assert(false) //should never happen
    }
    //Do we need a fast or classic quorum
    val quorum : Int = if(ballots.head.fast) fastQuorum else classicQuorum
    if(responses.size >= quorum) {
      confirmedBallot = true
      version = MDCCBallot(-1, -1,null,true)
      var values : List[CStruct] =  Nil
      responses.foreach( v => {
        val msg : Phase1b  = v._2.asInstanceOf[Phase1b]
        version.compare(msg.meta.currentVersion) match {
          case -1 => {
            values = msg.value :: Nil
            version = msg.meta.currentVersion
          }
          case 0 => {
            values :+ msg.value
          }
          case 1 =>
        }
      })
      //We only need to consider the quorum of the version and the current round.
      //The next round will only be started, if the current one is stable
      val tmp = resolver.provedSafe(values, if(version.fast) fastQuorum else classicQuorum, quorum, servers.size)
      value = tmp._1
      unsafeCommands = tmp._2
      //TODO: We should already learn the value here
      confirmedVersion = false
      request match {
        case StorageEnvelope(src, x: BeMaster) => {
          src ! GotMastership(ballots)
          clear() //We are done
        }
        case StorageEnvelope(src, ResolveConflict(_,_, propose,requester)) => {
          startPhase2a(requester, propose)
        }
        case _ => throw new RuntimeException("A Phase1a should always be triggered through BeMaster")
      }
    }
  }

  private def isMaster(src : ServiceType) : Boolean = {
    src == this.remoteHandle
  }

  @inline private def startOver() = {
    forwardRequest(request)
    clear()
  }

  @inline private def clear() = {
    request = null
    status = READY
    responses.clear
  }

  @inline def classicQuorum = floor(servers.size.toDouble / 2.0).toInt + 1
  @inline def fastQuorum = ceil(3.0 * servers.size.toDouble / 4.0).toInt


  def createNextBallot() = {
    servers ! BeMaster(key, version.round + 1, version.round + 10, true)
  }

  def !(msg : StorageMessage)(implicit sender: RemoteServiceProxy[StorageMessage]) = remoteHandle.!(msg)(sender)

  def kill = remoteHandle.notify(Exit())



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

  def startPhase2a(src : ServiceType, propose : Propose) : Unit = {
    status = PHASE2A
    assert(confirmedBallot)
    assert(value != null) //if we do a phase 2, we have at least an empty CStruct
    val cBallot = topBallot(ballots)
    assert(cBallot.server == remoteHandle)
    assert(version.compare(cBallot) < 0 )

    if(cBallot.fast){
      //We are just doing conflict resolution and open the next round
      servers ! Phase2a(key, cBallot, value, unsafeCommands)
    }else{
      val nextBallot : MDCCBallot =  getBallot(ballots, version.round + 1)  match {
        case Some(ballot) => ballot
        case None => {
          debug("The next round is not defined. We abort")
          //The next round is not defined, so we do not know how we can handle this one
          //Lets first create a new round and then we try it later again
          createNextBallot()
          forwardRequest(src, propose, unsafeCommands)
          //We abort
          clear()
          return
        }
      }
      if(nextBallot.fast){
        if(value.commands.size == 0){
          //The round is still free, so lets use it
          servers ! Phase2a(key, cBallot, value, unsafeCommands ++ seq(propose))
        }else{
          if(confirmedVersion){
            //The round is not free, but we already learned the value -> so we can move on to the next round
            //In addition, we use the chance to compress the cstruct
            val rebase = resolver.compressCStruct(value)
            servers ! Phase2a(key, nextBallot, rebase, unsafeCommands ++ seq(propose))
          }else{
            //The version is still not stable, so we need to make it stable first
            servers ! Phase2a(key, cBallot, value, unsafeCommands ++ seq(propose))
          }
        }
      }else{
        //OK, we are in a classic round and the next one is a classic round
        //Thus, we handle it more pessimistic (normally 1 update per round)
        if(value.commands.size == 0){
          //The round is still free, so lets use it
          if(unsafeCommands.isEmpty){
            //But we do one value at a time
            servers ! Phase2a(key, cBallot, value, seq(propose).head :: Nil)
          }else{
            //Lets do one update at a time
            servers ! Phase2a(key, cBallot, value, unsafeCommands.head :: Nil )
            forwardRequest(src, propose, unsafeCommands.tail)
          }
        }else{
          //The round is already used
          if(confirmedVersion){
            //The value is stable, so we can move on to the next round if all updates are committed
            if(value.commands.exists(_.pending)){
              //We found a pending update, so we wait with moving on to the next round
              Scheduler.schedule(() => {forwardRequest(remoteHandle, propose, unsafeCommands)}, MCCCRecordHandler.WAIT_TIME )
            }else{
              //The round is clear and committed, time to move on
              val rebase = resolver.compressCStruct(value)
              if(unsafeCommands.isEmpty){
                servers ! Phase2a(key, nextBallot, rebase, seq(propose))
              }else{
                servers ! Phase2a(key, nextBallot, rebase, unsafeCommands.head :: Nil)
                forwardRequest(src, propose, unsafeCommands.tail)
              }
            }
          }else{
            //The value is not stable, so we need to stabilize it first
            servers ! Phase2a(key, nextBallot, value)
            forwardRequest(src, propose, unsafeCommands)
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
      case -1 => throw new RuntimeException("Should never happen as the storage node should send a Phase2bMasterFailure message")
    }

    val quorum = if(currentBallot.fast) fastQuorum else classicQuorum
    if(responses.size >= quorum){
      debug("We got a quorum")
      val values = responses.map(_._2.asInstanceOf[Phase2b].value).toSeq
      val tmp = resolver.provedSafe(values, quorum, quorum, servers.size)
      debug("ProvedSafe CStruct: %s", tmp._1)
      debug("Unsafe commands:s %s", tmp._2)
      value = tmp._1
      unsafeCommands = tmp._2
      confirmedVersion = true
      request match {
        case msg@StorageEnvelope(src, propose: SinglePropose)  =>  {
          val cmd = value.commands.find(_.xid == propose.xid)
          if(cmd.isDefined){
            debug("We learned the value, lets inform the requester")
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
           src ! Recovered(key, value, MDCCMetadata(version, ballots))
          if(!missing.isEmpty){
            debug("The recovery was successful, but we did not learn the propose")
            forwardRequest(src, MultiPropose(missing))
          }
        }
        case _ => throw new RuntimeException("Should never happen")
      }
      clear()
    }
  }

  def informLearners(src : ServiceType, proposes : Propose) : Seq[SinglePropose] = {
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



object MCCCRecordHandler {
 final val WAIT_TIME = 20
}


