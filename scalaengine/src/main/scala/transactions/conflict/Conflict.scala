package edu.berkeley.cs.scads.storage.transactions.conflict

import edu.berkeley.cs.scads.comm._

import edu.berkeley.cs.scads.storage.MDCCRecordReaderWriter
import edu.berkeley.cs.scads.storage.transactions._

import actors.threadpool.ThreadPoolExecutor.AbortPolicy
import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.ConcurrentHashMap
import java.util.Arrays

// TODO: Make thread-safe.  It might already be, by using TxDB

object Status extends Enumeration {
  type Status = Value
  val Commit, Abort, Unknown, Accept, Reject = Value
}

trait DBRecords {
  val db: TxDB[Array[Byte], Array[Byte]]
  val factory: TxDBFactory
}

trait PendingUpdates extends DBRecords {
  // Even on abort, store the xid and always abort it afterwards --> Check
  // NoOp property
  // Is only allowed to accept, if the operation will be successful, even
  // if all outstanding Cmd might be NullOps
  // TODO: return all cstructs of all keys, or None
  def accept(xid: ScadsXid, updates: Seq[RecordUpdate]): Boolean

  // Value is chosen (reflected in the db) and confirms trx state.
  def commit(xid: ScadsXid, updates: Seq[RecordUpdate]): Boolean

  def abort(xid: ScadsXid)

  def getDecision(xid: ScadsXid): Status.Status

  def getCStruct(key: Array[Byte]): Option[CStruct]

  def startup() = {}

  def shutdown() = {}
}

abstract class IntegrityConstraintChecker {
  def check(key: String, newValue: String): Boolean
}

abstract class ConflictResolver {
  def getLUB(sequences: Array[CStruct]): CStruct

  def getGLB(sequences: Array[CStruct]): CStruct
}

// Status of a transaction.  Stores all the updates in the transaction.
@serializable
case class TxStatusEntry(var status: Status.Status,
                         var updates: Seq[RecordUpdate])

class PendingUpdatesController(override val db: TxDB[Array[Byte], Array[Byte]],
                               override val factory: TxDBFactory) extends PendingUpdates {

  // Transaction state info. Maps txid -> txstatus/decision
  private val txStatus =
    factory.getNewDB[ScadsXid, TxStatusEntry](db.getName + ".txstatus")

  // For physical update conflict detection
  private val pendingKeys =
    factory.getNewDB[Array[Byte], RecordUpdate](db.getName + ".pendingkeys")

  // (de)serialize MDCCRecords from the db
  private val recReaderWriter = new MDCCRecordReaderWriter

  override def accept(xid: ScadsXid, updates: Seq[RecordUpdate]) = {
    var success = true
    val txn = db.txStart()

    // TODO: This is just for physical updates.  This should only be done if
    //       there are any physical updates.
    val pendingTxn = pendingKeys.txStart()
    try {
      updates.foreach(r => {
        if (success) {
          r match {
            case LogicalUpdate(key, schema, delta) => {}
            case ValueUpdate(key, oldValue, newValue) => {
              val newRec = recReaderWriter.fromBytes(newValue)
              val correctOldValue = (oldValue, db.get(txn, key)) match {
                case (Some(old), Some(v)) => {
                  // Record found in db, compare with the old version
                  val dbRec = recReaderWriter.fromBytes(v)
                  val oldRec = recReaderWriter.fromBytes(old)

                  // Set the correct next round for the update.
                  newRec.metadata.currentRound = dbRec.metadata.currentRound + 1

                  // TODO: Don't compare versions, but compare the list of
                  //       masters?
                  (oldRec.value, dbRec.value) match {
                    case (Some(a), Some(b)) => Arrays.equals(a, b)
                    case (None, None) => true
                    case (_, _) => false
                  }
                }
                case (None, None) => true
                case (_, _) => false
              }
              val noConflict =
                pendingKeys.putNoOverwrite(pendingTxn, key, r)
              success = success && correctOldValue && noConflict
            }
            case VersionUpdate(key, newValue) => {
              val newRec = recReaderWriter.fromBytes(newValue)
              val correctVersion = db.get(txn, key) match {
                case Some(v) => {
                  // Record found in db, verify the new round number
                  val dbRec = recReaderWriter.fromBytes(v)
                  (newRec.metadata.currentRound == dbRec.metadata.currentRound + 1)
                }
                case None => true
              }
              val noConflict =
                pendingKeys.putNoOverwrite(pendingTxn, key, r)
              success = success && correctVersion && noConflict
            }
          }
        }
      })
    } catch {
      case e: Exception => {}
      success = false
    }
    if (success) {
      db.txCommit(txn)
      pendingKeys.txCommit(pendingTxn);
      // TODO: Handle the case when the commit arrives before the prepare.
      txStatus.putNoOverwrite(null, xid, TxStatusEntry(Status.Accept, updates))
    } else {
      db.txAbort(txn)
      pendingKeys.txAbort(pendingTxn);
      // TODO: Handle the case when the commit arrives before the prepare.
      txStatus.putNoOverwrite(null, xid, TxStatusEntry(Status.Reject, updates))
    }
    success
  }

  override def commit(xid: ScadsXid, updates: Seq[RecordUpdate]) = {
    // TODO: Handle out of order commits to same records.
    var success = true
    val txn = db.txStart()
    val pendingTxn = pendingKeys.txStart()
    try {
      updates.foreach(r => {
        r match {
          case LogicalUpdate(key, schema, delta) => {}
          case ValueUpdate(key, oldValue, newValue) => {
            db.put(txn, key, newValue)
            pendingKeys.delete(pendingTxn, key)
          }
          case VersionUpdate(key, newValue) => {
            db.put(txn, key, newValue)
            pendingKeys.delete(pendingTxn, key)
          }
        }
      })
      db.txCommit(txn)
      pendingKeys.txCommit(pendingTxn)
    } catch {
      case e: Exception => {}
      db.txAbort(txn)
      pendingKeys.txAbort(pendingTxn)
      success = false
    }

    txStatus.put(null, xid, TxStatusEntry(Status.Commit, updates))
    success
  }

  override def abort(xid: ScadsXid) = {
    txStatus.get(null, xid) match {
      case None => {
        txStatus.put(null, xid, TxStatusEntry(Status.Abort, List[RecordUpdate]()))
      }
      case Some(status) => {
        status.updates foreach(r => {
          r match {
            case LogicalUpdate(key, schema, delta) => {}
            case ValueUpdate(key, oldValue, newValue) => {
              pendingKeys.delete(null, key)
            }
            case VersionUpdate(key, newValue) => {
              pendingKeys.delete(null, key)
            }
          }
          
        })
        txStatus.put(null, xid, TxStatusEntry(Status.Abort, status.updates))
      }
    }
  }

  override def getDecision(xid: ScadsXid) = {
    txStatus.get(null, xid) match {
      case None => Status.Unknown
      case Some(status) => status.status
    }
  }

  override def getCStruct(key: Array[Byte]) = {
    None
  }

  override def shutdown() = {
    txStatus.shutdown()
    pendingKeys.shutdown()
  }
}
