package edu.berkeley.cs.scads
package storage
package transactions
package mdcc


import edu.berkeley.cs.scads.storage.transactions.{Tx, ProtocolBase}
import actors.Actor
import util.Logger._
import comm.RemoteService
import actors.Actor._
import collection.mutable.HashSet
import util.{Scheduler, Logger}
import actors.TIMEOUT
import java.util.concurrent.Semaphore
import collection.mutable.HashSet

import scala.concurrent.Lock

import scala.actors.Future
import scala.actors.Futures._

case object TRX_EXIT
case object TRX_TIMEOUT

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit, ThreadPoolExecutor}

//import tools.nsc.matching.ParallelMatching.MatchMatrix.VariableRule
object MDCCProtocol extends ProtocolBase {
  protected val logger = Logger(classOf[MDCCProtocol])

  // for Megastore
  private val lock = new Lock()
  private var msgQueue = new ArrayBlockingQueue[Runnable](1024)
  protected val executor = new ThreadPoolExecutor(1, 1, 100, TimeUnit.SECONDS, msgQueue)

  class MSRequest(handler: MDCCTrxHandler) extends Runnable {
    var status: TxStatus = ABORTED
    val sema = new Semaphore(0, false)
    def run() {
      println("start serial tx: " + handler.Xid)
      status = handler.execute()
      println("finish serial tx: " + handler.Xid)
      println("")
      sema.release()
    }
  }

  def RunProtocol(tx: Tx): TxStatus = {
//    logger.info("START1 %s", Thread.currentThread.getName)
    val trxHandler = new MDCCTrxHandler(tx, Thread.currentThread.getName)

    if (false) {  // Megatore
      val req = new MSRequest(trxHandler)
      executor.execute(req)
      req.sema.acquire()
      req.status
    } else {
      val status = trxHandler.execute()
      status
    }
  }
}

class MDCCTrxHandler(tx: Tx, threadName: String) extends Actor {
  @volatile var status: TxStatus = UNKNOWN
  var Xid = ScadsXid.createUniqueXid()
  var count = 0
  var participants = collection.mutable.HashSet[MDCCRecordHandler]()

  //Semaphore is used for the programming model timeout SLO 300ms
  private val sema = new Semaphore(0, false)

  private var notifiedAcceptors = false

  protected val logger = Logger(classOf[MDCCTrxHandler])

  implicit val remoteHandle = StorageRegistry.registerActor(this).asInstanceOf[RemoteService[StorageMessage]]

  @inline def debug(msg : String, items : scala.Any*) = logger.debug("" + remoteHandle.id + ": Xid:" + Xid + " ->" + msg, items:_*)
  @inline def info(msg : String, items : scala.Any*) = logger.info("" + remoteHandle.id +   ": Xid:" + Xid + " ->" + msg, items:_*)

  def execute(): TxStatus = {
//    logger.info("START2 %s %s", threadName, Xid)
//    logger.info("START3 %s %s", threadName, Xid)
    val startT = System.nanoTime / 1000000

    startTrx(tx.updateList, tx.readList)

    val endT = System.nanoTime / 1000000
//    logger.info("END4 %s %s", threadName, Xid)

    this.start()
    debug("Waiting for status")
    sema.acquire()
    debug("We got a status: %s", status)
    status match {
      case UNKNOWN => tx.unknownFn()
      case COMMITTED=> tx.commitFn(COMMITTED)
      case ABORTED => tx.commitFn(ABORTED)
    }
    status
  }


  protected def startTrx(updateList: UpdateList, readList: ReadList) = {
    val startT = System.nanoTime / 1000000

    // TODO: filter out duplicates?
    updateList.getUpdateList.foreach(update => {
      update match {
        case ValueUpdateInfo(ns, servers, key, value) => {
          val endT1 = System.nanoTime / 1000000
          var action = ""
          val (md, oldBytes) = readList.getRecord(key) match {
            case None => {
              action = "N"
              (ns.getDefaultMeta(key), None)
            }
            case Some((r, b)) => {
              action = "S"
              (r.metadata, Some(b))
            }
          }
          val endT2 = System.nanoTime / 1000000
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          //TODO: Do we really need the MDCCMetadata
          val endT3 = System.nanoTime / 1000000
          val propose = SinglePropose(Xid, ValueUpdate(key, oldBytes, newBytes))  //TODO: We need a read-strategy

          val endT4 = System.nanoTime / 1000000
          val rHandler = ns.recordCache.getOrCreate(key, CStruct(value, Nil), md, servers, ns.getConflictResolver)

          participants += rHandler
          val endT5 = System.nanoTime / 1000000
          debug("" + Xid + ": Sending physical update propose to MCCCRecordHandler", propose)
          debug("Record handler " + rHandler.hashCode )
          rHandler.remoteHandle ! propose
          val endT6 = System.nanoTime / 1000000
//          if (endT6 - startT > 5) {
//            logger.error("slow %s %s [%s, %s%s, %s, %s, %s, %s] startTrx: %s", Thread.currentThread.getName, Xid, (endT1 - startT), (endT2 - endT1), action, (endT3 - endT2), (endT4 - endT3), (endT5 - endT4), (endT6 - endT5), (endT6 - startT))
//          }
        }
        case LogicalUpdateInfo(ns, servers, key, value) => {
          val md = readList.getRecord(key) match {
            case None => ns.getDefaultMeta(key)
            case Some((r, _)) => r.metadata
          }
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          val propose = SinglePropose(Xid, LogicalUpdate(key, newBytes))
          val rHandler = ns.recordCache.getOrCreate(key, CStruct(value, Nil), md, servers, ns.getConflictResolver)  //TODO: Gene is the CStruct correct?
          participants += rHandler
          debug("" + Xid + ": Sending logical update propose to MCCCRecordHandler", propose)
          debug("Record handler " + rHandler.hashCode )
          rHandler.remoteHandle ! propose
        }
      }
    })
  }

  def notifyAcceptors() = {
    assert(status == COMMITTED || status == ABORTED )
    if(!notifiedAcceptors) {
      val msg = if (status == COMMITTED) {
        edu.berkeley.cs.scads.storage.Commit(Xid)
      } else {
        edu.berkeley.cs.scads.storage.Abort(Xid)
      }
      val servers = new HashSet[PartitionService]()
      participants.foreach(s => {
        val recordMsg = if (status == COMMITTED) {
          edu.berkeley.cs.scads.storage.RecordCommit(s.key, Xid)
        } else {
          edu.berkeley.cs.scads.storage.RecordAbort(s.key, Xid)
        }
        s.servers.foreach(servers += _)
        debug("Notify recordhandler xid: %s local:%s master:%s possibleMaster:%s remoteHandle:%s", Xid, s, s.masterRecordHandler, s.possibleMasterRecordHandler, s.remoteHandle)
        if(s.masterRecordHandler.isDefined)
          s.masterRecordHandler.get ! msg
        else {
          s.possibleMasterRecordHandler ! recordMsg
          s.remoteHandle ! recordMsg
//          s.remoteHandle ! msg
        }
      })
//      // Only send one commit to each machine.
//      // (this doesn't work because there could be multiple services on
//      //  same machine. commented out.)
//      val uniqServers = servers.map(s => {
//        (s.host, s)
//      }).toMap.values
//      debug("Notify servers: %s", uniqServers)
//      uniqServers.foreach( _ ! msg)

      debug("Notify servers: %s", servers)
      servers.foreach( _ ! msg)
    }
    notifiedAcceptors = true


  }

  def act() {
    debug("" + this.hashCode() + "Starting to wait for messages. Setting timeout:" + tx.timeout)
    Scheduler.schedule(() => {
      this ! TRX_TIMEOUT}, tx.timeout)
//      this ! TRX_TIMEOUT}, 10000)  // Megastore

    var timedOut = false

    loop {
      react {
        case StorageEnvelope(src, msg@Learned(_, _, success)) => {
          if(success) {
            //TODO Take care of duplicate messages
            count += 1
            debug("Received record commit: %s src: %s", msg, src)
            debug("Receive record commit %s status: %s committed: %s of %s. Participants: %s", Xid, status, count, participants.size, participants  )
            if(count == participants.size && status == UNKNOWN){
              debug("Transaction " + Xid + " committed")
              status = COMMITTED
              this ! TRX_EXIT
            }
          }else{
            if(status == UNKNOWN) {
              debug("" + Xid + ": Receive record abort")
              debug("Transaction " + Xid + " aborted")
              status = ABORTED
              this ! TRX_EXIT
            }
          }
        }
        case TRX_EXIT => {
          sema.release()
          debug("" + Xid + ": TRX_EXIT requested. timedOut: " + timedOut)
          notifyAcceptors
          StorageRegistry.unregisterService(remoteHandle)
          //TODO Add finally remote
          exit()
        }
        case TRX_TIMEOUT => {
          debug("" + Xid + "Time out")
          timedOut = true
          sema.release()
        }
        case msg@_ =>
          throw new RuntimeException("Unknown message: " + msg)

      }
    }
  }
}
