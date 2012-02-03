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

case object TRX_EXIT
case object TRX_TIMEOUT

//import tools.nsc.matching.ParallelMatching.MatchMatrix.VariableRule
object MDCCProtocol extends ProtocolBase {
  def RunProtocol(tx: Tx): TxStatus = {
    val trxHandler = new MDCCTrxHandler(tx)
    trxHandler.execute()
  }
}

class MDCCTrxHandler(tx: Tx) extends Actor {
  @volatile var status: TxStatus = UNKNOWN
  var Xid = ScadsXid.createUniqueXid()
  var count = 0
  var participants = collection.mutable.HashSet[MDCCRecordHandler]()

  //Semaphore is used for the programming model timeout SLO 300ms
  private val sema = new Semaphore(0, false)

  protected val logger = Logger(classOf[MDCCTrxHandler])

  implicit val remoteHandle = StorageRegistry.registerActor(this).asInstanceOf[RemoteService[StorageMessage]]

  @inline def debug(msg : String, items : scala.Any*) = logger.debug("" + remoteHandle.id + ": Xid:" + Xid + " ->" + msg, items:_*)
  @inline def info(msg : String, items : scala.Any*) = logger.info("" + remoteHandle.id +   ": Xid:" + Xid + " ->" + msg, items:_*)

  def execute(): TxStatus = {
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
    updateList.getUpdateList.foreach(update => {
      update match {
        case ValueUpdateInfo(ns, servers, key, value) => {
          val (md, oldBytes) = readList.getRecord(key) match {
            case None => (ns.getDefaultMeta(), None)
            case Some(r) => (r.metadata, Some(MDCCRecordUtil.toBytes(r)))
          }
          val newBytes = MDCCRecordUtil.toBytes(value, md)
          //TODO: Do we really need the MDCCMetadata
          val propose = SinglePropose(Xid, ValueUpdate(key, oldBytes, newBytes))  //TODO: We need a read-strategy
          val rHandler = ns.recordCache.getOrCreate(key, CStruct(value, Nil), md, servers, ns.getConflictResolver)
          participants += rHandler
          info("" + Xid + ": Sending physical update propose to MCCCRecordHandler", propose)
          debug("Record handler " + rHandler.hashCode )
          rHandler.remoteHandle ! propose
        }
        case LogicalUpdateInfo(ns, servers, key, value) => {
          val md = readList.getRecord(key) match {
            case None => ns.getDefaultMeta()
            case Some(r) => r.metadata
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
    val msg = if (status == COMMITTED)   edu.berkeley.cs.scads.storage.Commit(Xid)
              else edu.berkeley.cs.scads.storage.Abort(Xid)
    participants.foreach(s => {
      debug("Notify recordhandler local:%s master:%s", s, s.masterRecordHandler)
      s.masterRecordHandler.map( _ ! msg)
      s.servers.foreach(_ ! msg)
      s.remoteHandle ! msg
    })
  }

  def act() {
    debug("" + this.hashCode() + "Starting to wait for messages. Setting timeout:" + tx.timeout)
    Scheduler.schedule(() => {
      this ! TRX_TIMEOUT}, tx.timeout)
    startTrx(tx.updateList, tx.readList)
    loop {
      react {
        case StorageEnvelope(src, msg@Learned(_, _, success)) => {
          if(success) {
            //TODO Take care of duplicate messages
            count += 1
            debug("" + Xid + ": Receive record commit")
            if(count == participants.size && status == UNKNOWN){
              info("Transaction " + Xid + " committed")
              status = COMMITTED
              this ! TRX_EXIT
            }
          }else{
            if(status == UNKNOWN) {
              debug("" + Xid + ": Receive record abort")
              info("Transaction " + Xid + " aborted")
              status = ABORTED
              this ! TRX_EXIT
            }
          }
        }
        case TRX_EXIT => {
          sema.release()
          debug("" + Xid + ": TRX_EXIT requested")
          notifyAcceptors
          StorageRegistry.unregisterService(remoteHandle)
          //TODO Add finally remote
          exit()
        }
        case TRX_TIMEOUT => {
          debug("" + Xid + "Time out")
          sema.release()
        }
        case msg@_ =>
          throw new RuntimeException("Unknown message: " + msg)

      }
    }
  }
}
