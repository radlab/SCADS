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
import edu.berkeley.cs.scads.storage.transactions.MDCCMetaHelper._


class MDCCServer(namespace : String,
                 db: TxDB[Array[Byte], Array[Byte]],
                 partition : PartitionService,
                 pendingUpdates: PendingUpdates,
                 default : MDCCMetaDefault,
                 recordCache : MDCCRecordCache,
                 routingTable : MDCCRoutingTable
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

  def getMeta(key : Array[Byte])(implicit txn : TransactionData) : MDCCMetadata  = {
     val storedMDCCRec: Option[MDCCRecord] = db.get(txn, key).map(MDCCRecordUtil.fromBytes(_))
     if(storedMDCCRec.isEmpty)
      return default.defaultMetaData
     else
      return storedMDCCRec.get.metadata
  }


  protected  def processPropose(src: Option[RemoteServiceProxy[StorageMessage]], xid: ScadsXid, update: RecordUpdate)(implicit sender: RemoteServiceProxy[StorageMessage])  = {
    implicit val trx = startTrx()
    val meta = getMeta(update.key)
    if(isFast(meta)){
      val cstruct = pendingUpdates.acceptOption(xid, update)
    }else{
      val recordHandler = recordCache.getOrCreate(update.key, routingTable.serversForKey(update.key), meta, pendingUpdates.getConflictResolver)
      recordHandler ! new Envelope[StorageMessage](src.asInstanceOf[Option[RemoteService[StorageMessage]]], Propose(xid, update))
    }
    commitTrx(trx)
  }

  protected def processPhase1a(src: Option[RemoteServiceProxy[StorageMessage]], key: Array[Byte], newMeta: MDCCBallotRange) = {
    implicit val trx = startTrx()
    val meta = getMeta(key)
    commitTrx(trx)
    //pendingUpdates.setMeta(combine(oldMeta, newMeta))
  }

  protected def processPhase2a(src: Option[RemoteServiceProxy[StorageMessage]], key: Array[Byte], ballot: MDCCBallot, value: CStruct, newUpdate : Seq[RecordUpdate] ) = {

  }

  protected def processAccept(src: Option[RemoteServiceProxy[StorageMessage]], xid: ScadsXid) = {

  }




  def process(src: Option[RemoteServiceProxy[StorageMessage]], msg: TrxMessage)(implicit sender: RemoteServiceProxy[StorageMessage]) = {
    msg match {
      case Propose(xid, update) =>  processPropose(src, xid, update)
      case Phase1a(key: Array[Byte], ballot: MDCCBallotRange) => processPhase1a(src, key, ballot)
      case Phase2a(key, ballot, safeValue, newUpdate ) => processPhase2a(src, key, ballot, safeValue, newUpdate)
      case Commit(xid: ScadsXid) =>
      case Abort(xid: ScadsXid) =>
      case _ => src.map(_ ! ProcessingException("Trx Message Not Implemented", ""))
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