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

import collection.mutable.HashMap
import java.util.concurrent.atomic.AtomicInteger
import util.{LRUMap}
import org.apache.avro.generic.IndexedRecord
import conflict.ConflictResolver

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

class MCCCTrxHandler(tx: Tx) extends Actor {
  var status: TxStatus = UNKNOWN
  var Xid = ScadsXid.createUniqueXid()
  var count = 0
  var size = 0

  implicit val remoteHandle = StorageRegistry.registerActor(this).asInstanceOf[RemoteService[StorageMessage]]

  protected def startTrx(updateList: UpdateList, readList: ReadList) = {
    updateList.getUpdateList.foreach(update => {
      size += 1
      update match {
        case ValueUpdateInfo(ns, servers, key, value) => {
          val oldRrecord = readList.getRecord(key)
          val md : Option[MDCCMetadata] = oldRrecord.map(r => MDCCMetadata(r.metadata.currentRound, r.metadata.ballots))
          //TODO: Do we really need the MDCCMetadata
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          val propose = Envelope(Some(remoteHandle), Propose(Xid, ValueUpdate(key, oldRrecord.flatMap(_.value), value.get)))
          MDCCRecordCache.getOrCreate(ns, key, servers, md) ! propose
        }
        case LogicalUpdateInfo(ns, servers, key, value) => {
          val md = readList.getRecord(key).map(r => MDCCMetadata(r.metadata.currentRound, r.metadata.ballots))
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          val propose = Envelope(Some(remoteHandle), Propose(Xid, LogicalUpdate(key, value.get)))
          MDCCRecordCache.getOrCreate(ns, key, servers, md) ! propose
        }
      }
    })
  }

  def act() {
    startTrx(tx.updateList, tx.readList)
    loop {
      react {
        case LEARNED_ABORT => {
          assert(status != COMMIT)
          if(status == UNKNOWN) {
            status = ABORT
            this ! EXIT
          }
        }
        case LEARNED_ACCEPT => {
          count += 1
          if(count == size && status == UNKNOWN){
            status = COMMIT
            this ! EXIT
          }
        }
        case EXIT => {
          StorageRegistry.unregisterService(remoteHandle)
          exit()
        }
        case _ =>
          throw new RuntimeException("Unknown message")

      }
    }
  }
}


object MDCCRecordCache {

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

  def getOrCreate(ns: TransactionI, key : Array[Byte], servers: Seq[PartitionService], mt: Option[MDCCMetadata]) : MCCCRecordHandler = {
    cache.synchronized{
      cache.get(key) match {
        case None => {
          var handler = new MCCCRecordHandler(key, servers, mt.getOrElse(ns.getDefaultMeta()), ns.getConflictResolver)
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
case object PROPOSE extends RecordStatus {val name = "PROPOSE"}
case object PROPOSED extends RecordStatus {val name = "PROPOSED"}
case object FAST_PROPOSED extends RecordStatus {val name = "FAST_PROPOSED"}
case object PHASE1A  extends RecordStatus {val name = "PHASE1A"}
case object PHASE2A extends RecordStatus {val name = "PHASE2A"}
case object LEARNED_ABORT extends RecordStatus {val name = "LEARNED_ABORT"}
case object LEARNED_ACCEPT extends RecordStatus {val name = "LEARNED_ACCEPT"}

class MCCCRecordHandler (
       var key : Array[Byte],
       var servers: Seq[PartitionService],
       var meta: MDCCMetadata,
       var resolver : ConflictResolver
  ) extends Actor {

  type ServiceType =  RemoteService[IndexedRecord]

  implicit def extractSource(src : Option[RemoteService[IndexedRecord]]) = src.get

  implicit val remoteHandle = StorageRegistry.registerActor(this)

  private var safeValue : CStruct  = null
  private var status: RecordStatus = READY
  private var currentRequest : MDCCProtocol = null
  private var quorum =  new HashMap[ServiceType, MDCCProtocol]()
  private var Xid: ScadsXid = null
  private var update: RecordUpdate = null
  private var source : RemoteService[IndexedRecord] = null
  private var maxTried = null

  implicit val localAddress = null

  override def hashCode() = key.hashCode()

  override def equals(that: Any): Boolean = that match {
     case other: MCCCRecordHandler => key == other.key
     case _ => false
  }


  def getStatus = status
  def getValue = safeValue

  def act() {
    loop {
      react {
        case EXIT =>{
          StorageRegistry.unregisterService(remoteHandle)
          exit()
        }
        case Envelope(src, msg @ BeMaster(key, startRound, endRound, fast )) if status == READY => {
          source = src.get
          currentRequest = msg
          startPhase1a()
        }
        case Envelope(src, msg @ Propose(xid, update)) if status == READY => {
          source = src.get
          currentRequest = msg
          this.update = update
          this.Xid = xid
          SendProposal()
        }
        case Envelope(src, msg : Phase1b ) =>   {   //we do the phase check afterwards to empty out old messages
          status match {
            case PHASE1A => processPhase1b(src, msg)
            case _ => //out of phase message
          }
        }
        case Envelope(src, Phase2b(ballot, value)) =>
        case Envelope(src, Accept(xid)) =>
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
        startPhase1a()  //we start over
      }
      case _ => assert(false) //should never happen
    }
    if(quorum.size >= classicQuorum) { //only if we need to do an update we go on to Phase2a
      currentRequest match {                //why are we doing this
        case x : BeMaster => {
          source ! GotMastership(meta)
          clear()
        }
        case x : Propose =>  startPhase2a
        case _ => throw new RuntimeException("Unallowed request")
      }
    }
  }

  private def clear() = {
    status = READY
    update = null
    currentRequest = null
    quorum.clear
    Xid = null
    update = null
    source  = null
    maxTried = null
  }

  @inline def classicQuorum = floor(servers.size.toDouble / 2.0).toInt + 1
  @inline def fastQuorum = ceil(3.0 * servers.size.toDouble / 4.0).toInt


  def startPhase1a() : Unit =  {
    status = PHASE1A
    currentRequest match {                //why are we doing this
      case BeMaster(key, startRound, endRound, fast ) => {
        meta = getOwnership(meta, startRound, endRound, fast)
      }
      case x : Propose =>
        meta = getOwnership(meta, meta.currentRound, meta.currentRound, false)
      case _ => throw new RuntimeException("Unallowed request")
    }
    val phase1aMsg = Phase1a(key, meta)
    quorum.clear()
    servers.map(_ ! phase1aMsg)
  }

  def SendProposal() = {
    val propose = Propose(Xid, update)
    if (fastRound(meta)){  //If we have a fast round, we can propose directly
      status = FAST_PROPOSED
      servers.foreach(_ ! propose)
    }else{
      val master = getMaster(meta)
      if(isMaster(meta)){
        status = PHASE2A
        startPhase2a()
      }else{
        status = PROPOSED
        master ! propose
      }
    }
  }



  def startPhase2a() : List[MessageFuture[StorageMessage]] = {
    assert(isMaster(meta))
    assert(quorum.size >= classicQuorum)

    safeValue = resolver.provedSafe(quorum.map(v => v._2.asInstanceOf[Phase1b].value), fastQuorum, classicQuorum, servers.size)

    val msg = Phase2a(key, currentBallot(meta), safeValue)

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
