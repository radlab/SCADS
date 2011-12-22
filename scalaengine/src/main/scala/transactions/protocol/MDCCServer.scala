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
    val proposes = msg match {
      case s : SinglePropose => s :: Nil
      case MultiPropose(seq) => seq
    }
    val key = proposes.head.update.key
    assert(!proposes.isEmpty, "The propose has to contain at least one update")
    assert(proposes.map(_.update.key).distinct.size == 1, "Currenty we only support multi proposes for the same key")
    debug(key, "Process propose %s %s %s", src, msg, pendingUpdates)
    implicit val trx = startTrx()
    val meta = getMeta(proposes.head.update.key)
    val ballot = meta.ballots.head.ballot
    meta.validate() //just to make sure
    if(ballot.fast){
      //TODO can we optimize the accept?
      val cstruct = proposes.map(prop => pendingUpdates.acceptOption(prop.xid, prop.update, true)).last._3
      debug(key, "Replying with 2b to fast ballot source:%s cstruct:cstruct", src, cstruct)
      src ! Phase2b(ballot, cstruct)
    }else{
      debug(key, "Classic ballot: We start our own MDCCRecordHandler")
      val recordHandler = recordCache.getOrCreate(
        key,
        pendingUpdates.getCStruct(key),
        meta,
        routingTable.serversForKey(key),
        pendingUpdates.getConflictResolver,
        partition)
      recordHandler.remoteHandle.!(msg)(src)
    }
    commitTrx(trx)
  }

  protected def processPhase1a(src: RemoteServiceProxy[StorageMessage], key: Array[Byte], newMeta: Seq[MDCCBallotRange]) = {
    debug(key, "Process Phase1a", src, newMeta)
    implicit val trx = startTrx()
    val record = getRecord(key)
    val meta : MDCCMetadata = extractMeta(record)
    val maxRound = max(meta.ballots.head.startRound, newMeta.head.startRound)
    compareRanges(meta.ballots, newMeta, maxRound) match {
      case -1 => {
        debug(key, "Setting new meta", meta.ballots, newMeta, maxRound)
        meta.ballots = newMeta
        meta.confirmedBallot = false
      }
      case -2 => {
        debug(key, "Combining meta", meta.ballots, newMeta, maxRound)
        meta.confirmedBallot = false
        meta.ballots = combine(meta.ballots, newMeta, max(meta.ballots.head.startRound, newMeta.head.startRound))
      }
      case _ => debug(key, "Ignoring Phase1a message local-ballots:" + meta.ballots + " proposed" + newMeta)//The meta data is old or the same, so we do need to do nothing
    }
    val r = record.getOrElse(new MDCCRecord(None, null))
    r.metadata = meta
    putRecord(key, r)
    src ! Phase1b(meta, pendingUpdates.getCStruct(key))
    commitTrx(trx)
  }

  protected def processPhase2a(src: RemoteServiceProxy[StorageMessage], key: Array[Byte], reqBallot: MDCCBallot, value: CStruct, newUpdates : Seq[SinglePropose] ) = {
    debug(key, "Process Phase2a", src, value, newUpdates)
    implicit val trx = startTrx()
    var record = getRecord(key)
    val meta = extractMeta(record)
    val myBallot = getBallot(meta.ballots, reqBallot.round)
    if(myBallot.isEmpty || myBallot.get.compare(reqBallot) != 0){
      debug(key, "Sending Master Failure local: %s - request: %s", myBallot, reqBallot)
      src ! Phase2bMasterFailure(meta.ballots, false)
    }else{
      debug(key, "Writing new value value: %s updates: %s", value, newUpdates)
      if (value.value.isEmpty && value.commands.isEmpty) {
        newUpdates.foreach(c => {
          // TODO(kraska): how to find out if this is a fast or classic round?
          pendingUpdates.acceptOption(c.xid, c.update, false)
        })
      } else {
        // TODO(kraska): how to find out if this is a fast or classic round?
        pendingUpdates.overwrite(key, value, newUpdates, false)
      }
      val r = getRecord(key)
      if(r.isDefined){
        val record = r.get
        record.metadata.currentVersion = myBallot.get
        //TODO shorten meta data
        putRecord(key, record)
      }else{
        //What shoudl we do when it is empty
      }
      val msg = Phase2b(myBallot.get, pendingUpdates.getCStruct(key)) //TODO Ask Gene if this is correct
      debug(key, "Sending Phase2b back %s %s", src, msg)
      src ! msg //TODO Ask Gene if this is correct
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
      case Phase1a(key, ballot) => processPhase1a(src, key, ballot)
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
                         puController: PendingUpdates,
                         forceNewMeta : Boolean = false) : MDCCServer = {
    val defaultMeta = MDCCMetaDefault.getOrCreateDefault(nsRoot, partition, forceNewMeta)

    new MDCCServer(namespace, db, partition, puController, defaultMeta, new MDCCRecordCache, puController.routingTable)
  }
}
