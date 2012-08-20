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
  @inline def debug(key : Array[Byte], msg : String, items : scala.Any*) = logger.debug(" Id:" + this.hashCode() + " " + partition.id + " key:" + (new ByteArrayWrapper(key)).hashCode() + " - " + msg, items:_*)


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
    val startT = System.nanoTime / 1000000
    val rec = getRecord(key)
    val endT = System.nanoTime / 1000000
    if (endT - startT > 35) {
      logger.info("slow get meta get record rec: %s, %s", rec, (endT - startT))
    }
    extractMeta(key, rec)
  }

  @inline def extractMeta(key : Array[Byte], record : Option[MDCCRecord])(implicit txn : TransactionData) : MDCCMetadata = {
     if(record.isEmpty) {
       val startT = System.nanoTime / 1000000
       val ret = default.defaultMetaData(key)
       val endT = System.nanoTime / 1000000
       if (endT - startT > 35) {
         logger.info("slow extract meta: %s", (endT - startT))
       }
       return ret
     } else
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
    val previousCommits = proposes.head.previousCommits
    val startT = System.nanoTime / 1000000
    debug(key, "Processing Propose. %s src: %s, msg: %s previousCommits: %s", Thread.currentThread.getName, src, msg, previousCommits)
    assert(!proposes.isEmpty, "The propose has to contain at least one update")
    assert(proposes.map(_.update.key).distinct.size == 1, "Currenty we only support multi proposes for the same key")
    previousCommits.foreach(pc => pendingUpdates.addEarlyCommit(pc.xid, pc.status))
    val endT2 = System.nanoTime / 1000000
    implicit val trx = startTrx()
    val meta = getMeta(proposes.head.update.key)
    val ballot = meta.ballots.head.ballot
    meta.validate() //just to make sure
    // TODO(kraska): there is a potential for an infinite loop, if the default
    //               metadata was fast, but then the metadata was switched to
    //               classic by the master.
    //               The main problem is that the code is making a decision
    //               based on the default metadata, which is really old, and
    //               potentially wrong.

    val endT3 = System.nanoTime / 1000000
    if(ballot.fast) {
      //TODO can we optimize the accept?
      debug(key, "Fast ballot: update local db. %s", Thread.currentThread.getName)
      val results = proposes.map(prop => (prop, pendingUpdates.acceptOption(prop.xid, prop.update, true)))
      val endT4 = System.nanoTime / 1000000
      commitTrx(trx)

      val endT5 = System.nanoTime / 1000000

      val cstruct = results.last._2._3
      debug(key, "Replying with 2b to fast ballot source:%s cstruct:%s", src, cstruct)
      // Get possibly old decisions on the new updates.
      val oldCommits = proposes.map(prop => {
        (prop.xid, pendingUpdates.getDecision(prop.xid, prop.update.key))
      }).filter(!_._2.isEmpty).map(i => OldCommit(i._1, i._2.get))

      src ! Phase2b(ballot, cstruct, oldCommits)

      val endT6 = System.nanoTime / 1000000

      // Must be outside of db lock.
      results.foreach(r => {
        pendingUpdates.txStatusAccept(r._1.xid, List(r._1.update), r._2._1)
      })

      val endT7 = System.nanoTime / 1000000

      val endT = System.nanoTime / 1000000
      if (endT - startT >= 50) {
        logger.info("Processing Propose DONE. key:%s %s msg: %s [%s, %s, %s, %s, %s, %s] proposeTime: %s", (new ByteArrayWrapper(key)).hashCode(), Thread.currentThread.getName, msg, (endT2 - startT), (endT3 - endT2), (endT4 - endT3), (endT5 - endT4), (endT6 - endT5), (endT7 - endT6), (endT - startT))
      }
    }else{
      commitTrx(trx)
      debug(key, "Classic ballot: We get or start our own MDCCRecordHandler")
      val recordHandler = recordCache.getOrCreate(
        key,
        pendingUpdates.getCStruct(key),
        meta,
        routingTable.serversForKey(key),
        pendingUpdates.getConflictResolver,
        partition)
      debug(key, "Classic ballot: forwarding src: %s, msg: %s handler: %s", src, msg, recordHandler.remoteHandle)
      recordHandler.remoteHandle.forward(msg, src)
    }
    val endT = System.nanoTime / 1000000
    debug(key, "Processing Propose DONE. %s msg: %s proposeTime: %s", Thread.currentThread.getName, msg, (endT - startT))

  }

  protected  def processRecordHandlerMsg(src: RemoteServiceProxy[StorageMessage], key: Array[Byte], msg : MDCCProtocol) : Unit = {
    implicit val trx = startTrx()
    val meta = getMeta(key)
    val getRec = getRecord(key)
    commitTrx(trx)
    debug(key, "We got a recordhandler request. msg: %s meta: %s getRecord: %s", msg, meta, getRec)
    val recordHandler = recordCache.getOrCreate(
        key,
        pendingUpdates.getCStruct(key),
        meta,
        routingTable.serversForKey(key),
        pendingUpdates.getConflictResolver,
        partition)
    recordHandler.remoteHandle.forward(msg, src)
  }

  protected def processPhase1a(src: RemoteServiceProxy[StorageMessage], key: Array[Byte], newMeta: Seq[MDCCBallotRange]) = {
    debug(key, "Process Phase1a src: %s, newMeta: %s", src, newMeta)
    implicit val trx = startTrx()
    val record = getRecord(key)
    val meta : MDCCMetadata = extractMeta(key, record)
    val maxRound = max(meta.ballots.head.startRound, newMeta.head.startRound)
    compareRanges(meta.ballots, newMeta, maxRound) match {
      case -1 => {
        debug(key, "Setting new meta balots:%s, new:%s, maxround:%s", meta.ballots, newMeta, maxRound)
        meta.ballots = newMeta
        meta.confirmedBallot = false
      }
      case -2 => {
        debug(key, "Combining meta balots:%s, new:%s, maxround:%s", meta.ballots, newMeta, maxRound)
        meta.confirmedBallot = false
        meta.ballots = combine(meta.ballots, newMeta, max(meta.ballots.head.startRound, newMeta.head.startRound))
      }
      case _ => debug(key, "Ignoring Phase1a message local-ballots %s, proposed: %s compareRanges: %d", meta.ballots, newMeta, compareRanges(meta.ballots, newMeta, maxRound))//The meta data is old or the same, so we do need to do nothing
    }
    val r = record.getOrElse(new MDCCRecord(None, null))
    r.metadata = meta
    putRecord(key, r)
    commitTrx(trx)
    src ! Phase1b(meta, pendingUpdates.getCStruct(key))
  }



  protected def processPhase2a(src: RemoteServiceProxy[StorageMessage], key: Array[Byte], reqBallot: MDCCBallot, value: CStruct, committedXids: Seq[ScadsXid], abortedXids: Seq[ScadsXid], newUpdates : Seq[SinglePropose], forceNonPending: Boolean) = {
    debug(key, "Process Phase2a src: %s, value: %s, reqBallot: %s newUpdates: %s forceNonPending: %s", src, value, reqBallot, newUpdates, forceNonPending)
    // A seq of all pending commands in the cstruct.
    val safePendingOptions = value.commands.filter(_.pending).map(c => {
      (c.xid, c.command, c.commit)
    })
    implicit val trx = startTrx()
    var record = getRecord(key)
    val meta = extractMeta(key, record)
    val myBallot = getBallot(meta.ballots, reqBallot.round)
    if (myBallot.isEmpty || myBallot.get.compare(reqBallot) != 0) {
      commitTrx(trx)
      debug(key, "Sending Master Failure local: %s - request: %s record: %s meta: %s", myBallot, reqBallot, record, meta)
      src ! Phase2bMasterFailure(meta.ballots, false)
    } else {
      // Update metadata.
      meta.ballots = adjustRound(meta.ballots, reqBallot.round)
      meta.currentVersion = myBallot.get
      meta.confirmedBallot = true
      debug(key, "phase2a new meta: %s", meta)
      // TODO shorten meta data

      debug(key, "Writing new value value: %s updates: %s", value, newUpdates)
      if (!(value.value.isEmpty && value.commands.isEmpty)) {
        val oR = pendingUpdates.overwrite(key, value, Some(meta), committedXids, abortedXids, myBallot.get.fast)
        debug(key, "Overwrite value: %s \n - committedXids %s \n - abortedXids %s, \n - newUpdates %s, \n - myBallot.get.fast %s - status \n %s", value, committedXids, abortedXids, newUpdates, myBallot.get.fast, oR)
        if (!oR) debug(key, "Not accepted overwrite")
      } else {
        // Write the new metadata to db record.
        if (record.isDefined) {
          val r = record.get
          r.metadata = meta
          // This record is still correct since overwrite() was not called.
          putRecord(key, r)
        } else {
          // What should we do when it is empty.  Can this ever happen?
        }
      }
      val pendingOptions = newUpdates.map(c => {
        (c.xid, c.update, pendingUpdates.acceptOption(c.xid, c.update, myBallot.get.fast, forceNonPending)._1)
      })
      commitTrx(trx)

      // Must be outside of db lock.
      (safePendingOptions ++ pendingOptions).foreach(r => {
        pendingUpdates.txStatusAccept(r._1, List(r._2), r._3)
      })

      // Get possibly old decisions on the new updates.
      val oldCommits = newUpdates.map(c => {
        (c.xid, pendingUpdates.getDecision(c.xid, key))
      }).filter(!_._2.isEmpty).map(i => OldCommit(i._1, i._2.get))

      // TODO: More efficient way to get the cstruct.
      val msg = Phase2b(myBallot.get, pendingUpdates.getCStruct(key), oldCommits) //TODO Ask Gene if this is correct
      debug(key, "Sending Phase2b back %s %s", src, msg)
      src ! msg //TODO Ask Gene if this is correct
    }

    // TODO: Can also update status for committedXids and abortedXids, but it
    //       not necessary for correctness.
  }

  protected def processAccept(src: RemoteServiceProxy[StorageMessage], xid: ScadsXid, commit : Boolean) = {
    logger.debug("Id: %s %s %s Trx-Id: %s Process commit message. Status: %s", this.hashCode(), partition.id, Thread.currentThread.getName, xid, commit )
    if(commit)
      pendingUpdates.commit(xid)
    else
      pendingUpdates.abort(xid)
    logger.debug("Id: %s %s %s Trx-Id: %s Process commit message DONE.", this.hashCode(), partition.id, Thread.currentThread.getName, xid)
  }

  protected def processRecordAccept(src: RemoteServiceProxy[StorageMessage], key: Array[Byte], msg: TrxMessage) = {
    val recordHandler = recordCache.get(key)
    logger.debug("Id: %s %s key:%s Process record commit message. exists: %s msg: %s", this.hashCode(), partition.id, (new ByteArrayWrapper(key)).hashCode(), recordHandler.isDefined, msg)
    if (recordHandler.isDefined) {
      recordHandler.get.remoteHandle.forward(msg, src)
    }
    logger.debug("Id: %s %s key:%s Process record commit message DONE. msg: %s", this.hashCode(), partition.id, (new ByteArrayWrapper(key)).hashCode(), msg)
  }

  def process(src: RemoteServiceProxy[StorageMessage], msg: TrxMessage) = {
    msg match {
      case msg : Propose =>  processPropose(src, msg)
      case Phase1a(key, ballot) => processPhase1a(src, key, ballot)
      case Phase2a(key, ballot, safeValue, committedXids, abortedXids, proposes, forceNonPending) => processPhase2a(src, key, ballot, safeValue, committedXids, abortedXids, proposes, forceNonPending)
      case Commit(xid: ScadsXid) => processAccept(src, xid, true)
      case Abort(xid: ScadsXid) => processAccept(src, xid, false)
      case RecordCommit(key: Array[Byte], xid: ScadsXid) => processRecordAccept(src, key, msg)
      case RecordAbort(key: Array[Byte], xid: ScadsXid) => processRecordAccept(src, key, msg)
      case msg : BeMaster => processRecordHandlerMsg(src, msg.key, msg)
      case msg : ResolveConflict => processRecordHandlerMsg(src, msg.key, msg)
      case _ => src ! ProcessingException("Trx Message Not Implemented msg: " + msg, "")
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
