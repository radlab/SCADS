package edu.berkeley.cs.scads
package storage
package transactions
package mdcc

import _root_.edu.berkeley.cs.avro.marker.AvroRecord
import comm._
import MDCCMetaHelper._
import scala.math.{floor, ceil}

import net.lag.logging.Logger

import java.lang.Thread
import java.util.Calendar

import java.util.concurrent.atomic.AtomicInteger
import util.{LRUMap}
import org.apache.avro.generic.IndexedRecord
import conflict.ConflictResolver
import collection.mutable.{SynchronizedSet, HashSet, HashMap}

//import tools.nsc.matching.ParallelMatching.MatchMatrix.VariableRule
import actors.Actor


object MDCCHandler extends ProtocolBase {
  def RunProtocol(tx: Tx) = {
    val trxHandler = new MCCCTrxHandler(tx)
    trxHandler.start()
  }
}

class MCCCTrxHandler(tx: Tx) extends Actor {
  var status: TxStatus = UNKNOWN
  var Xid = ScadsXid.createUniqueXid()
  var count = 0
  var size = 0
  var participants = HashSet[MCCCRecordHandler]()

  var callbacks : Seq[(Boolean) => Unit] = Nil

  implicit val remoteHandle = StorageRegistry.registerActor(this).asInstanceOf[RemoteService[StorageMessage]]

  protected def startTrx(updateList: UpdateList, readList: ReadList) = {
    updateList.getUpdateList.foreach(update => {
      size += 1
      update match {
        case ValueUpdateInfo(ns, servers, key, value) => {
          val oldRrecord = readList.getRecord(key)
          val md : MDCCMetadata = oldRrecord.map(r => MDCCMetadata(r.metadata.currentRound, r.metadata.ballots)).getOrElse(ns.getDefaultMeta())
          //TODO: Do we really need the MDCCMetadata
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          val propose = Propose(Xid, ValueUpdate(key, oldRrecord.flatMap(_.value), value.get))
          val rHandler = ns.recordCache.getOrCreate(key, servers, md, ns.getConflictResolver)
          participants += rHandler
          rHandler.remoteHandle ! propose
        }
        case LogicalUpdateInfo(ns, servers, key, value) => {
          val md = readList.getRecord(key).map(r => MDCCMetadata(r.metadata.currentRound, r.metadata.ballots)).getOrElse(ns.getDefaultMeta())
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          val propose = Propose(Xid, LogicalUpdate(key, value.get))
          val rHandler = ns.recordCache.getOrCreate(key, servers, md, ns.getConflictResolver)
          participants += rHandler
          rHandler.remoteHandle ! propose
        }
      }
    })
  }

  def notifyAcceptors() = {
    if (status == COMMITTED)
      participants.foreach(_.remoteHandle !  edu.berkeley.cs.scads.storage.Commit(Xid))
    else if(status == ABORTED)
      participants.foreach(_.remoteHandle !  edu.berkeley.cs.scads.storage.Abort(Xid))
    else assert(false)
  }

  def act() {
    startTrx(tx.updateList, tx.readList)
    loop {
      react {
        case Learned(_, _, false) => {
          assert(status != COMMITTED)
          if(status == UNKNOWN) {
            status = ABORTED
            this ! EXIT
          }
        }
        case Learned(_, _, true) => {
          count += 1
          if(count == size && status == UNKNOWN){
            status = COMMITTED
            this ! EXIT
          }
        }
        case EXIT => {
          callbacks.foreach(_(status == COMMITTED))
          notifyAcceptors
          StorageRegistry.unregisterService(remoteHandle)
          exit()
        }
        case _ =>
          throw new RuntimeException("Unknown message")

      }
    }
  }
}


class MDCCRecordCache() {

  val CACHE_SIZE = 500



  //TODO: If we wanna use the cache for reads, we should use a lock-free structure
  lazy val cache = new LRUMap[Array[Byte], MCCCRecordHandler](CACHE_SIZE, None, killHandler){
      protected override def canExpire(k: Array[Byte], v: MCCCRecordHandler): Boolean = v.getStatus == READY
    }

  def killHandler (key : Array[Byte], handler :  MCCCRecordHandler) = handler ! EXIT

  def get(key : Array[Byte]) : Option[MCCCRecordHandler] = {
    cache.synchronized{
      cache.get(key)
    }
  }

  def getOrCreate(key : Array[Byte], servers: Seq[PartitionService], mt: MDCCMetadata, conflictResolver : ConflictResolver) : MCCCRecordHandler = {
    cache.synchronized{
      cache.get(key) match {
        case None => {
          var handler = new MCCCRecordHandler(key, servers, mt, conflictResolver)
          handler.start()
          cache.update(key, handler)
          handler
        }
        case Some(v) => v
      }
    }
  }

}


sealed trait RecordStatus {def name: String}
case object EXIT
case object READY extends RecordStatus {val name = "READY"}
case object FORWARDED extends RecordStatus {val name = "PROPOSED"}
case object FAST_PROPOSED extends RecordStatus {val name = "FAST_PROPOSED"}
case object PHASE1A  extends RecordStatus {val name = "PHASE1A"}
case object PHASE2A extends RecordStatus {val name = "PHASE2A"}
case object LEARNED_ABORT extends RecordStatus {val name = "LEARNED_ABORT"}
case object LEARNED_ACCEPT extends RecordStatus {val name = "LEARNED_ACCEPT"}
case object LEARNING extends RecordStatus {val name = "LEARNED_ACCEPT"}

class MCCCRecordHandler (
       var key : Array[Byte],
       var servers: Seq[PartitionService],
       var meta: MDCCMetadata,
       var resolver : ConflictResolver
  ) extends Actor {

  type ServiceType =  RemoteService[IndexedRecord]

  implicit def extractSource(src : Option[RemoteService[IndexedRecord]]) = src.get

  implicit val remoteHandle = StorageRegistry.registerActor(this)

  private var initRequest : Envelope[IndexedRecord] = null

  private var learnedValue : CStruct  = null
  private var status: RecordStatus = READY
  private var quorum =  new HashMap[ServiceType, MDCCProtocol]()

  implicit val localAddress = null

  override def hashCode() = key.hashCode()

  override def equals(that: Any): Boolean = that match {
     case other: MCCCRecordHandler => key == other.key
     case _ => false
  }

  def currentBallot() = {assert(false); MDCCBallot(meta.currentRound, meta.ballots.head.vote, meta.ballots.head.server, meta.ballots.head.fast)}
  def getStatus = status
  def getValue = learnedValue

  def act() {
    loop {
      react {
        case EXIT =>{
          StorageRegistry.unregisterService(remoteHandle)
          exit()
        }
        case msg @ Envelope(src, ResolveConflict(key, ballot)) if status == READY  => {
          initRequest = msg
          startPhase1a()
        }
        case msg @ Envelope(src, x : BeMaster) if status == READY => {
          initRequest = msg
          startPhase1a()
        }
        case msg @ Envelope(src, x : Propose) if status == READY => {
          initRequest = msg
          SendProposal(x)
        }
        case Envelope(src, msg : Phase1b ) =>   {   //we do the phase check afterwards to empty out old messages
          status match {
            case PHASE1A => processPhase1b(src, msg)
            case _ => //out of phase message
          }
        }
        case Envelope(src, msg : Phase2b) => {
          status match {
            case PHASE2A => processPhase2b(src, msg)
            case FAST_PROPOSED => processPhase2b(src, msg)
            case _ => //out of phase message
          }
        }
        case Envelope(src, msg : Commit) => servers.map(_ ! msg)
        case Envelope(src, msg : Abort) =>  servers.map(_ ! msg)
        case _ =>  throw new RuntimeException("Not supported request")
      }
    }
  }

  //TODO: At the moment we agree on the full meta data, we could also just agree on the new value
  def processPhase1b(src : ServiceType, msg : Phase1b) = {
    assert(status == PHASE1A)
    compareMetadata(msg.ballots, meta) match {
      case 0 =>
        //The storage node accepted the new meta data
        quorum += src -> msg
      case 1 =>
        //We got an old message, we can ignore this case
      case -1  | -2 => {
        //TODO We need to ensure progress while two nodes try to get the mastership
        //There was a new meta data version out there, so we try it again
        meta = combine(msg.ballots, meta)
        startPhase1a  //we start over
      }
      case _ => assert(false) //should never happen
    }
    if(quorum.size >= classicQuorum) { //only if we need to do an update we go on to Phase2a
      initRequest match {                //why are we doing this
        case Envelope(src, x : BeMaster) => {
          src ! GotMastership(meta)
          clear()
        }
        case Envelope(src, x : Propose) =>  startPhase2a
        case _ => throw new RuntimeException("Unallowed request")
      }
    }
  }

  private def clear() = {
    status = READY
    initRequest = null
    quorum.clear
  }

  @inline def classicQuorum = floor(servers.size.toDouble / 2.0).toInt + 1
  @inline def fastQuorum = ceil(3.0 * servers.size.toDouble / 4.0).toInt


  def startPhase1a() : Unit =  {
    status = PHASE1A
    initRequest match {                //why are we doing this
      case Envelope(src, BeMaster(key, startRound, endRound, fast )) => {
        meta = getOwnership(meta, startRound, endRound, fast)
      }
      case _ =>
        meta = getOwnership(meta, meta.currentRound, meta.currentRound, false)
    }
    val phase1aMsg = Phase1a(key, meta.ballots)
    quorum.clear()
    servers.map(_ ! phase1aMsg)
  }

  def SendProposal(propose : Propose) = {
    if (fastRound(meta)){  //If we have a fast round, we can propose directly
      status = FAST_PROPOSED
      servers.foreach(_ ! propose)
    }else{
      if(isMaster(meta)){
        status = PHASE2A
        startPhase2a()
      }else{
        val master = getMaster(meta)
        status = FORWARDED
        master ! propose
      }
    }
  }

  def startPhase2aClassic()= {
    assert(isMaster(meta))
    assert(quorum.size >= classicQuorum)
  }

  def startPhase2a() = {
    assert(isMaster(meta))
    assert(quorum.size >= classicQuorum)

    //TODO I need a new conflict value
    val (safeValue, leftover) = resolver.provedSafe(quorum.map(v => v._2.asInstanceOf[Phase1b].value), fastQuorum, classicQuorum, servers.size)
    val msg = initRequest match {
      case Envelope(src, Propose(xid, update)) => Phase2a(key, currentBallot, safeValue, update :: Nil)
      case _ => throw new RuntimeException("So far we only do classic rounds")
    }
    status = PHASE2A
    quorum.clear
    servers.map(_ ! msg)
  }

  def processPhase2b(src : ServiceType, msg : Phase2b) = {
    currentBallot.compare(msg.ballot) match {
      case 0 => {
        quorum += src -> msg
      }
      case 1 => {
        status = LEARNING
        quorum.clear()
        meta = combine(msg.ballot, meta)
      }
      case -1 => //Old message we do nothing
    }

    val success =
      if(isFast(meta) && quorum.size >= fastQuorum) {
        learnedValue = resolver.provedSafe(quorum.map(v => v._2.asInstanceOf[Phase2b].value), fastQuorum)
        true
      }else if(!isFast(meta) && quorum.size >= classicQuorum){
        learnedValue = resolver.provedSafe(quorum.map(v => v._2.asInstanceOf[Phase2b].value), classicQuorum)
        true
      }else{
        false
      }
    if(success) {
      initRequest match {
        case Envelope(src, Propose(xid, update)) => checkAndNotify(src, xid)
        case _ => //nobody to inform
      }
    }
  }

  def checkAndNotify(src : Option[RemoteService[IndexedRecord]], xid : ScadsXid ) = {
    val cmd = learnedValue.commands.find(_.xid == xid)
    if(cmd.isDefined){
      assert(src.isDefined)
      src.get ! Learned(xid, key, cmd.get.commit)
      clear()
    }else{
      throw new RuntimeException("What should we do? Restart?")
    }
  }


}
