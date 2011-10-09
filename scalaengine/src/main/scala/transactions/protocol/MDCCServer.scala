package edu.berkeley.cs.scads.storage.transactions.mdcc

import _root_.edu.berkeley.cs.scads.comm._
import _root_.edu.berkeley.cs.scads.storage.transactions._
import _root_.edu.berkeley.cs.scads.storage.{PartitionHandler, StorageManager}
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

  def getMeta: MDCCMetadata  = null

  def processPropose(src: Option[RemoteActorProxy], xid: ScadsXid, update: RecordUpdate)(implicit sender: RemoteActorProxy)  = {
    val trx = startTrx()
    val meta = getMeta(trx, update.key)
    //val master = getMaster(meta)
    val result = pendingUpdates.accept(xid, update)
    commitTrx(trx, null)
  }

  def processPhase1a(src: Option[RemoteActorProxy], key: Array[Byte], newMeta: MDCCMetadata) = {
    val trx = startTrx()
    val oldMeta = getMeta(trx, key)
    commitTrx(trx, null)
    //pendingUpdates.setMeta(combine(oldMeta, newMeta))
  }

  def processPhase2a(src: Option[RemoteActorProxy], key: Array[Byte], ballot: MDCCBallot, value: CStruct) = {

  }

  def processAccept(src: Option[RemoteActorProxy], xid: ScadsXid) = {

  }


  def process(src: Option[RemoteActorProxy], msg: TrxMessage)(implicit sender: RemoteActorProxy) = {
    msg match {
      case Propose(xid: ScadsXid, update: RecordUpdate) => processPropose(src, xid, update)
      case Phase1a(key: Array[Byte], ballot: MDCCBallotRange) => processPhase1a(src, key, ballot)
      case Phase2a(key: Array[Byte], ballot: MDCCBallot, value: CStruct) => processPhase2a(src, key, ballot, value)
      case Accept(xid: ScadsXid) =>
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