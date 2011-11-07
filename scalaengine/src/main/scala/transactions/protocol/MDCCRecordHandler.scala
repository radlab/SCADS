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

  private var unsafeCommands : Seq[Propose] = Nil

  type ServiceType =  RemoteServiceProxy[StorageMessage]

  implicit def extractSource(src : Option[RemoteService[IndexedRecord]]) = src.get

  //TODO: is this really a storageservice?
  implicit val remoteHandle = StorageService(StorageRegistry.registerFastMailboxFunc(messagePriorities, processMailbox))

  private var status: RecordStatus = READY
  private var responses =  new HashMap[ServiceType, MDCCProtocol]()

  private var request : Envelope[StorageMessage] = null

  override def hashCode() = key.hashCode()


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



  def messagePriorities(msg : StorageMessage) : Int = {
    msg match {
      case x : Exit => 0
      case x : Commit => 5
      case x : Abort => 5
      case x : Phase1b => 10
      case x : Phase2b => 10
      case x : Recovered => 20
      case x : Learned => 20
      case x : GotMastership => 20
      case x : BeMaster => 30
      case x : ResolveConflict => 40
      case x : Propose => 50
      case _ => 100
    }
  }

  def getStatus = status

  //TODO We should create a proper priority queue
  def processMailbox(mailbox : Mailbox[StorageMessage]) {
      mailbox {
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
        case env@StorageEnvelope(src, msg: Phase2b) => {
          debug("Phase2b message" , env)
          status match {
            case PHASE2A => processPhase2b(src, msg)
            case FAST_PROPOSED => processPhase2b(src, msg)
            case _ =>
          }
        }
        case env@StorageEnvelope(src, Recovered(key, value, metaData))  => {
          debug("Recovered message", env)
          if (status == RECOVERY) {
            this.value = value
            this.version = metaData.currentVersion
            this.ballots = metaData.ballots
            confirmedBallot = true
            confirmedVersion = true
            status = READY
          }
        }
        case env@StorageEnvelope(src, msg : Learned)  => {
          debug("Learned message", env)
          status match {
            case FORWARDED => {
              request match {
                case StorageEnvelope(src, x: Propose) => src ! msg
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
        case Envelope(src, msg: Exit)  if status == READY => {
          debug("EXIT request")
          StorageRegistry.unregisterService(remoteHandle)
        }
        case Envelope(src, msg: Commit) => {
          servers ! msg
        }
        case Envelope(src, msg: Abort) => {
          servers ! msg
        }
        case env@StorageEnvelope(src, ResolveConflict(key, ballot)) if status == READY => {
          debug("ResolveConflict request", env)
              //TODO
        }
        case env@StorageEnvelope(src, x: Propose) if status == READY=> {
          debug("Propose request", env)
          request = env
          SendProposal(src, x)
        }
        case _ => mailbox.keepMsgInMailbox = true
      }
  }

  def resolveConflict(){

  }


  def SendProposal(src: ServiceType, propose : Propose) : Unit = {
    debug("Processing proposal", src, propose)
    ballotStatus match {
     case FAST_BALLOT => {
        status = FAST_PROPOSED
        debug("Sending fast propose from " + remoteHandle + " to " + servers.mkString(":"))
        servers.foreach(_ ! propose)
      }
      case VOTED_SWITCH_TO_CLASSIC | UNVOTED_SWITCH_TO_CLASSIC => {
       remoteHandle ! ResolveConflict(key, version)
       debug("Request ResolveConflict " + version)
       forwardRequest(src, propose)
      }
     case CSTABLE_VOTED_NEXT_CLASSIC | CSTABLE_UNVOTED_NEXT_CLASSIC => {
       val ballot = getBallot(ballots, version.round + 1).get
        if(ballot.server == this.remoteHandle){
            //We are the master for the next round, so lets go
            if(confirmedBallot) {
              //The ballot is valid, lets move to phase2
              debug("Request ResolveConflict " + version)
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
       remoteHandle ! ResolveConflict(key, version) //A new fast round always starts with a conflict resolution
       remoteHandle ! propose
     }
     case CUNSTABLE => {
      debug("React to CUNSTABLE")
      remoteHandle ! ResolveConflict(key, version)
      remoteHandle ! propose
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
    compareRanges(msg.meta.ballots, ballots, maxRound) match {
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
        case StorageEnvelope(src, x: BeMaster) => src ! GotMastership(ballots)
        case _ => throw new RuntimeException("A Phase1a should always be triggered through BeMaster")
      }
      clear()
    }
  }

  private def isMaster(src : ServiceType) : Boolean = {
    src == this.remoteHandle
  }

  private def clear() = {
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

  @inline def forwardRequest(src : ServiceType, msg : StorageMessage) = remoteHandle.!(msg)(src)

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
          //The next round is not defined, so we do not know how we can handle this one
          //Lets first create a new round and then we try it later again
          createNextBallot()
          forwardRequest(src, ProposeSeq(unsafeCommands :+ propose))
          //We abort
          clear()
          return
        }
      }
      if(nextBallot.fast){
        if(value.commands.size == 0){
          //The round is still free, so lets use it
          servers ! Phase2a(key, cBallot, value, unsafeCommands :+ propose)
        }else{
          if(confirmedVersion){
            //The round is not free, but we already learned the value -> so we can move on to the next round
            //In addition, we use the chance to compress the cstruct
            val rebase = resolver.compressCStruct(value)
            servers ! Phase2a(key, nextBallot, rebase, unsafeCommands :+ propose)
          }else{
            //The version is still not stable, so we need to make it stable first
            servers ! Phase2a(key, cBallot, value, unsafeCommands :+ propose)
          }
        }
      }else{
        //OK, we are in a classic round and the next one is a classic round
        //Thus, we handle it more pessimistic (normally 1 update per round)
        if(value.commands.size == 0){
          //The round is still free, so lets use it
          if(unsafeCommands.isEmpty){
            servers ! Phase2a(key, cBallot, value, propose)
          }else{
            //Lets do one update at a time
            servers ! Phase2a(key, cBallot, value, unsafeCommands.head )
            forwardRequest(src, ProposeSeq(unsafeCommands.tail :+ propose))//we need to ensure the right owner
          }
        }else{
          //The round is already used
          if(confirmedVersion){
            //The value is stable, so we can move on to the next round if all updates are committed
            if(value.commands.exists(_.pending)){
              //We found a pending update, so we wait with moving on to the next round
              Scheduler.schedule(() => {forwardRequest(remoteHandle, ProposeSeq(unsafeCommands :+ propose))}, MCCCRecordHandler.WAIT_TIME )
            }else{
              //The round is clear and committed, time to move on
              val rebase = resolver.compressCStruct(value)
              if(unsafeCommands.isEmpty){
                servers ! Phase2a(key, nextBallot, rebase, propose)
              }else{
                servers ! Phase2a(key, nextBallot, rebase, unsafeCommands.head)
                forwardRequest(src, ProposeSeq(unsafeCommands.tail :+ propose))
              }
            }
          }else{
            //The value is not stable, so we need to stabilize it first
            servers ! Phase2a(key, nextBallot, value)
            forwardRequest(src, ProposeSeq(unsafeCommands :+ propose))
          }
        }
      }
    }
  }


  def processPhase2b(src : ServiceType, msg : Phase2b) : Unit = {
    debug("Received 2b message %s %s", msg, src)
    currentBallot.compare(msg.ballot) match {
      case 0 => {
        responses += src -> msg
      }
      case -1 => {
        //we got a newer ballot number
        responses.clear()
        responses += src -> msg
        ballots = combine(msg.ballot, ballots)
        //We do not change comfirmedBallot. Parts of it might still
        //be not confirmed if it is a fast rpopose
      }
      case 1 => //Old message we do nothing
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
        case msg@StorageEnvelope(src, propose: Propose)  =>  {
          val cmd = value.commands.find(_.xid == propose.xid)
          if(cmd.isDefined){
            debug("We learned the value, lets inform the requester")
            src ! Learned(propose.xid, key, cmd.get.commit)
          }else{
            debug("We did not learn our transaction, so we trying it again")
            forwardRequest(src, propose) //We should try it again
          }
        }
        case StorageEnvelope(src, req: ProposeSeq)  =>  {
          var missing = new ArrayBuffer[Propose](1)
          req.proposes.foreach(propose => {
           val cmd = value.commands.find(_.xid == propose.xid)
            if(cmd.isDefined){
              src ! Learned(propose.xid, key, cmd.get.commit)
            }else{
              missing += propose
            }
          })
          if(!missing.isEmpty){
            forwardRequest(src, ProposeSeq(missing))
          }
        }
        case _ => throw new RuntimeException("Should never happen")
      }
      clear()
    }
  }
}

object MCCCRecordHandler {
 final val WAIT_TIME = 20
}


