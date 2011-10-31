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

  def getMeta(key : Array[Byte])(implicit txn : TransactionData) : MDCCMetadata  = {
     val storedMDCCRec: Option[MDCCRecord] = db.get(txn, key).map(MDCCRecordUtil.fromBytes(_))
     if(storedMDCCRec.isEmpty)
      return default.defaultMetaData
     else
      return storedMDCCRec.get.metadata
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
   msg.proposes.foreach(processPropose(src, _))
  }


  protected def processPhase1a(src: RemoteServiceProxy[StorageMessage], key: Array[Byte], newMeta: MDCCBallotRange) = {
    implicit val trx = startTrx()
    val meta = getMeta(key)
    commitTrx(trx)
    //pendingUpdates.setMeta(combine(oldMeta, newMeta))
  }

  protected def processPhase2a(src: RemoteServiceProxy[StorageMessage], key: Array[Byte], ballot: MDCCBallot, value: CStruct, newUpdate : Seq[Propose] ) = {

  }

  protected def processAccept(src: RemoteServiceProxy[StorageMessage], xid: ScadsXid) = {

  }




  def process(src: RemoteServiceProxy[StorageMessage], msg: TrxMessage) = {
    msg match {
      case msg : Propose =>  processPropose(src, msg)
      case msg : ProposeSeq =>  processPropose(src, msg)
      case Phase1a(key: Array[Byte], ballot: MDCCBallotRange) => processPhase1a(src, key, ballot)
      case Phase2a(key, ballot, safeValue, proposes ) => processPhase2a(src, key, ballot, safeValue, proposes)
      case Commit(xid: ScadsXid) =>
      case Abort(xid: ScadsXid) =>
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