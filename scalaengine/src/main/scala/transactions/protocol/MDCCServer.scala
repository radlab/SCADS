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
import edu.berkeley.cs.scads.util.Logger


class MDCCServer(val namespace : String,
                 val db: TxDB[Array[Byte], Array[Byte]],
                 implicit val partition : PartitionService,
                 val pendingUpdates: PendingUpdates,
                 val default : MDCCMetaDefault,
                 val recordCache : MDCCRecordCache,
                 val routingTable : MDCCRoutingTable
                 ) extends TrxManager {
  protected val logger = Logger(classOf[MDCCServer])
  @inline def debug(key : Array[Byte], msg : String, items : scala.Any*) = logger.debug(msg + " -> " + namespace + ":" + key, items:_*)


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
    debug(msg.update.key, "Process propose %s %s %s", src, msg, pendingUpdates)
    implicit val trx = startTrx()
    val meta = getMeta(msg.update.key)
    val ballot = meta.ballots.head.ballot
    meta.validate() //just to make sure
    if(ballot.fast){
      val cstruct = pendingUpdates.acceptOption(msg.xid, msg.update)
      debug(msg.update.key, "Replying with 2b to fast ballot source:%s cstruct:cstruct", src, cstruct)
      src ! Phase2b(ballot, cstruct._3)
    }else{
      debug(msg.update.key, "Classic ballot: We start our own MDCCRecordHandler")
      val recordHandler = recordCache.getOrCreate(
        msg.update.key,
        pendingUpdates.getCStruct(msg.update.key),
        routingTable.serversForKey(msg.update.key),
        meta,
        pendingUpdates.getConflictResolver)
      recordHandler.forwardRequest(src, msg)
    }
    commitTrx(trx)
  }

  protected  def processPropose(src: RemoteServiceProxy[StorageMessage], msg : ProposeSeq) : Unit = {
    //TODO we should do everything in a single transaction
   msg.proposes.foreach(processPropose(src, _))
  }


  protected def processPhase1a(src: RemoteServiceProxy[StorageMessage], key: Array[Byte], newMeta: Seq[MDCCBallotRange]) = {
    debug(key, "Process Phase1a", src, newMeta)
    implicit val trx = startTrx()
    val record = getRecord(key)
    val meta = extractMeta(record)
    val maxRound = max(meta.ballots.head.startRound, newMeta.head.startRound)
    compareRanges(meta.ballots, newMeta, maxRound) match {
      case -1 => {
        debug(key, "Setting new meta", meta.ballots, newMeta, maxRound)
        meta.ballots = newMeta
      }
      case 2 => meta.ballots = {
        debug(key, "Combining meta", meta.ballots, newMeta, maxRound)
        combine(meta.ballots, newMeta, max(meta.ballots.head.startRound, newMeta.head.startRound))
      }
      case _ => debug(key, "Ignoring message")//The meta data is old or the same, so we do need to do nothing
    }
    val r = record.getOrElse(new MDCCRecord(None, null))
    r.metadata = meta
    putRecord(key, r)
    src ! Phase1a(key, meta.ballots)
    commitTrx(trx)
  }

  protected def processPhase2a(src: RemoteServiceProxy[StorageMessage], key: Array[Byte], reqBallot: MDCCBallot, value: CStruct, newUpdates : Seq[Propose] ) = {
    debug(key, "Process Phase2a", src, value, newUpdates)
    implicit val trx = startTrx()
    var record = getRecord(key)
    val meta = extractMeta(record)
    val myBallot = getBallot(meta.ballots, reqBallot.round)
    if(myBallot.isEmpty || myBallot.get.compare(reqBallot) != 0){
      debug(key, "Sending Master Failure")
      src ! Phase2bMasterFailure(meta.ballots)
    }else{
      debug(key, "Writing new value")
      pendingUpdates.overwrite(key, value, newUpdates)
      val r = getRecord(key).get
      r.metadata.currentVersion = myBallot.get
      putRecord(key, r)
      src ! Phase2b(myBallot.get, pendingUpdates.getCStruct(key)) //TODO Ask Gene if this is correct
    }
    commitTrx(trx)
  }

  protected def processAccept(src: RemoteServiceProxy[StorageMessage], xid: ScadsXid, commit : Boolean) = {
    logger.debug("Trx-Id: %s Process commit message. Status: %s", xid, commit )
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

  def createMDCCProtocol(namespace : String,
                         nsRoot : ZooKeeperProxy#ZooKeeperNode,
                         db: TxDB[Array[Byte], Array[Byte]],
                         partition : PartitionService,
                         keySchema: Schema,
                         puController: PendingUpdates) : MDCCServer = {
    val defaultMeta = MDCCMetaDefault.getOrCreateDefault(nsRoot, partition)

    new MDCCServer(namespace, db, partition, puController, defaultMeta, new MDCCRecordCache, new MDCCRoutingTable(nsRoot, keySchema))
  }
}
