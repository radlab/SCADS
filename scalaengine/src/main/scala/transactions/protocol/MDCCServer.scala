package edu.berkeley.cs.scads
package storage
package transactions
package mdcc

import _root_.transactions.protocol.MDCCRoutingTable
import comm._
import conflict._
import org.apache.avro.Schema
import java.util.concurrent.ConcurrentHashMap
import collection.mutable.HashMap
import edu.berkeley.cs.scads.storage.transactions.MDCCBallotRangeHelper._
import scala.math.max


class MDCCServer(val namespace : String,
                 val db: TxDB[Array[Byte], Array[Byte]],
                 implicit val partition : PartitionService,
                 val pendingUpdates: PendingUpdates,
                 val default : MDCCMetaDefault,
                 val recordCache : MDCCRecordCache,
                 val routingTable : MDCCRoutingTable
                 ) extends TrxManager {


  def startTrx() : TransactionData= {
    db.txStart()
  }

  def commitTrx(implicit txn : TransactionData) : Boolean = {
    try{
      db.txCommit(txn)
    } catch {
      case e: Exception => {
          db.txAbort(txn)
          return false
        }
    }
    return true
  }

  @inline def getRecord(key : Array[Byte])(implicit txn : TransactionData) : Option[MDCCRecord]  = {
     db.get(txn, key).map(MDCCRecordUtil.fromBytes(_))

  }

  @inline def putRecord(key : Array[Byte], record : MDCCRecord)(implicit txn : TransactionData) = {
    db.put(txn, key, MDCCRecordUtil.toBytes(record))
  }

  @inline def getMeta(key : Array[Byte])(implicit txn : TransactionData) : MDCCMetadata = {
    extractMeta(getRecord(key))
  }

  @inline def extractMeta(record : Option[MDCCRecord])(implicit txn : TransactionData) : MDCCMetadata = {
     if(record.isEmpty)
      return default.defaultMetaData
     else
      return record.get.metadata
  }

  @inline def putMeta(key : Array[Byte], value : Option[Array[Byte]], meta : MDCCMetadata)(implicit txn : TransactionData) : Boolean  = {
    db.put(txn, key, MDCCRecordUtil.toBytes(value, meta))
  }

  protected  def processPropose(src: RemoteServiceProxy[StorageMessage], msg : Propose) : Unit = {
    implicit val trx = startTrx()
    val meta = getMeta(msg.update.key)
    val ballot = meta.ballots.head.ballot
    meta.validate() //just to make sure
    if(ballot.fast){
      val cstruct = pendingUpdates.acceptOption(msg.xid, msg.update)
      src ! Phase2b(ballot, cstruct._3)
    }else{
      val recordHandler = recordCache.getOrCreate(
        msg.update.key,
        pendingUpdates.getCStruct(msg.update.key),
        routingTable.serversForKey(msg.update.key),
        meta,
        pendingUpdates.getConflictResolver)
      recordHandler ! new Envelope[StorageMessage](Some(src.asInstanceOf[RemoteService[StorageMessage]]), msg)
    }
    commitTrx(trx)
  }

  protected  def processPropose(src: RemoteServiceProxy[StorageMessage], msg : ProposeSeq) : Unit = {
    //TODO we should do everything in a single transaction
   msg.proposes.foreach(processPropose(src, _))
  }


  protected def processPhase1a(src: RemoteServiceProxy[StorageMessage], key: Array[Byte], newMeta: Seq[MDCCBallotRange]) = {
    implicit val trx = startTrx()
    val record = getRecord(key)
    val meta = extractMeta(record)
    val maxRound = max(meta.ballots.head.startRound, newMeta.head.startRound)
    compareRanges(meta.ballots, newMeta, maxRound) match {
      case -1 => meta.ballots = newMeta
      case 2 => meta.ballots = combine(meta.ballots, newMeta, max(meta.ballots.head.startRound, newMeta.head.startRound))
      case _ => //The meta data is old or the same, so we do need to do nothing
    }
    val r = record.getOrElse(new MDCCRecord(None, null))
    r.metadata = meta
    putRecord(key, r)
    src ! Phase1a(key, meta.ballots)
    commitTrx(trx)
  }

  protected def processPhase2a(src: RemoteServiceProxy[StorageMessage], key: Array[Byte], reqBallot: MDCCBallot, value: CStruct, newUpdates : Seq[Propose] ) = {
    implicit val trx = startTrx()
    var record = getRecord(key)
    val meta = extractMeta(record)
    val myBallot = getBallot(meta.ballots, reqBallot.round)
    if(myBallot.isEmpty || myBallot.get.compare(reqBallot) != 0){
      src ! Phase2bMasterFailure(meta.ballots)
    }else{
      pendingUpdates.overwrite(key, value, newUpdates)
      val r = getRecord(key).get
      r.metadata.currentVersion = myBallot.get
      putRecord(key, r)
      src ! Phase2b(myBallot.get, pendingUpdates.getCStruct(key)) //TODO Ask Gene if this is correct
    }
    commitTrx(trx)
  }

  protected def processAccept(src: RemoteServiceProxy[StorageMessage], xid: ScadsXid, commit : Boolean) = {
    implicit val trx = startTrx()
    if(commit)
      pendingUpdates.commit(xid)
    else
      pendingUpdates.abort(xid)
    commitTrx(trx)
  }




  def process(src: RemoteServiceProxy[StorageMessage], msg: TrxMessage) = {
    msg match {
      case msg : Propose =>  processPropose(src, msg)
      case msg : ProposeSeq =>  processPropose(src, msg)
      case Phase1a(key: Array[Byte], ballot: MDCCBallotRange) => processPhase1a(src, key, ballot)
      case Phase2a(key, ballot, safeValue, proposes ) => processPhase2a(src, key, ballot, safeValue, proposes)
      case Commit(xid: ScadsXid) => processAccept(src, xid, true)
      case Abort(xid: ScadsXid) =>  processAccept(src, xid, false)
      case _ => src ! ProcessingException("Trx Message Not Implemented", "")
    }
  }
}

object ProtocolMDCCServer {

  protected lazy val PUControllers = new HashMap[String, PendingUpdates]


  def createMDCCProtocol(namespace : String,
                 nsRoot : ZooKeeperProxy#ZooKeeperNode,
                 db: TxDB[Array[Byte], Array[Byte]],
                 factory: TxDBFactory,
                 partition : PartitionService,
                 keySchema: Schema,
                 valueSchema: Schema) : MDCCServer = {
    var puController : PendingUpdates = null
    PUControllers.synchronized{
      PUControllers.get(namespace) match {
        case None => {
          puController = new PendingUpdatesController(db, factory, keySchema, valueSchema)
          PUControllers.put(namespace, puController)
        }
        case Some(controller) => puController = controller
      }
    }
    val defaultMeta = MDCCMetaDefault.getOrCreateDefault(nsRoot, partition)

    new MDCCServer(namespace, db, partition, puController, defaultMeta, new MDCCRecordCache, new MDCCRoutingTable(nsRoot, keySchema))
  }
}