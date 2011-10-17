package edu.berkeley.cs.scads
package storage
package transactions
package mdcc

import comm._
import conflict._
import org.apache.avro.Schema
import java.util.concurrent.ConcurrentHashMap
import collection.mutable.HashMap


class MDCCServer(namespace : String,
                 db: TxDB[Array[Byte], Array[Byte]],
                 partition : PartitionService,
                 pendingUpdates: PendingUpdates,
                 default : MDCCMetaDefault) extends TrxManager {


  def startTrx() : TransactionData= {
    db.txStart()
  }

  def commitTrx(txn : TransactionData, failFunction : () => Unit) = {
    try{
      db.txCommit(txn)
    } catch {
      case e: Exception => {
          db.txAbort(txn)
          failFunction()
        }
    }
  }

  def getMeta(txn : TransactionData, key : Array[Byte]) : MDCCMetadata  = {
     val storedMDCCRec: Option[MDCCRecord] = db.get(txn, key).map(MDCCRecordUtil.fromBytes(_))
     if(storedMDCCRec.isEmpty)
      return default.defaultMetaData
     else
      return storedMDCCRec.get.metadata
  }


  def processPropose(src: Option[RemoteServiceProxy[StorageMessage]], xid: ScadsXid, update: RecordUpdate)(implicit sender: RemoteServiceProxy[StorageMessage])  = {
    val trx = startTrx()
    val meta = getMeta(trx, update.key)
    //val master = getMaster(meta)
    val result = pendingUpdates.accept(xid, update)
    commitTrx(trx, null)
  }

  def processPhase1a(src: Option[RemoteServiceProxy[StorageMessage]], key: Array[Byte], newMeta: MDCCMetadata) = {
    val trx = startTrx()
    val oldMeta = getMeta(trx, key)
    commitTrx(trx, null)
    //pendingUpdates.setMeta(combine(oldMeta, newMeta))
  }

  def processPhase2a(src: Option[RemoteServiceProxy[StorageMessage]], key: Array[Byte], ballot: MDCCBallot, value: CStruct, newUpdate : Seq[RecordUpdate] ) = {

  }

  def processAccept(src: Option[RemoteServiceProxy[StorageMessage]], xid: ScadsXid) = {

  }


  def process(src: Option[RemoteServiceProxy[StorageMessage]], msg: TrxMessage)(implicit sender: RemoteServiceProxy[StorageMessage]) = {
    msg match {
      case Propose(xid: ScadsXid, update: RecordUpdate) => processPropose(src, xid, update)
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
    new MDCCServer(namespace, db, partition, puController, defaultMeta)
  }
}