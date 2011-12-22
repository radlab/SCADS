package edu.berkeley.cs.scads.storage
package transactions
package conflict

import actors.threadpool.ThreadPoolExecutor.AbortPolicy
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import java.util.Arrays

import java.io._
import org.apache.avro.Schema
import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.scads.util.Logger
import _root_.transactions.protocol.MDCCRoutingTable

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
  //TODO: Gene all the operations should be ex

  // Even on abort, store the xid and always abort it afterwards --> Check
  // NoOp property
  // Is only allowed to accept, if the operation will be successful, even
  // if all outstanding Cmd might be NullOps
  // If accepted, returns all the cstructs for all the keys.  Otherwise, None
  // is returned.
  // If dbTxn is non null, it is used for all db operations, and the commit is
  // NOT performed at the end.  Otherwise, a new transaction is started and
  // committed.
  def acceptOptionTxn(xid: ScadsXid, updates: Seq[RecordUpdate], dbTxn: TransactionData = null, isFast: Boolean = false) : (Boolean, Seq[(Array[Byte], CStruct)])

  def acceptOption(xid: ScadsXid, update: RecordUpdate, isFast: Boolean = false)(implicit dbTxn: TransactionData): (Boolean, Array[Byte], CStruct)


  /**
   * The transaction was successful (we will never decide otherwise)
   */
  def commit(xid: ScadsXid)(implicit dbTxn: TransactionData): Boolean
  def commitTxn(xid: ScadsXid, dbTxn: TransactionData = null): Boolean

  /**
   * The transaction was learned as aborted (we will never decide otherwise)
   */
  def abort(xid: ScadsXid)(implicit dbTxn: TransactionData): Boolean

  // If dbTxn is non null, it is used for all db operations, and the commit is
  // NOT performed at the end.  Otherwise, a new transaction is started and
  // committed.
  def abortTxn(xid: ScadsXid, dbTxn: TransactionData = null): Boolean

  /**
   * Writes the new truth. Should only return false if something is messed up with the db
   */
  def overwrite(key: Array[Byte], safeValue: CStruct, newUpdates: Seq[SinglePropose], isFast: Boolean = false)(implicit dbTxn: TransactionData): Boolean

  def overwriteTxn(key: Array[Byte], safeValue: CStruct, newUpdates: Seq[SinglePropose], dbTxn: TransactionData = null, isFast: Boolean = false): Boolean

  def getDecision(xid: ScadsXid): Status.Status

  //Should return the CStruct or the default Cstruct
  def getCStruct(key: Array[Byte]): CStruct

  def startup() = {}

  def shutdown() = {}

  def setICs(ics: FieldICList)

  def getConflictResolver : ConflictResolver

  val routingTable: MDCCRoutingTable
}

// Status of a transaction.  Stores all the updates in the transaction.
// status is the toString() of the enum.
case class TxStatusEntry(var status: String,
                         var updates: Seq[RecordUpdate]) extends Serializable with AvroRecord

case class PendingStateInfo(var state: Array[Byte],
                            var xids: List[List[ScadsXid]]) extends Serializable with AvroRecord
case class PendingCommandsInfo(var base: Option[Array[Byte]],
                               var commands: ArrayBuffer[CStructCommand],
                               var states: ArrayBuffer[PendingStateInfo]) extends Serializable with AvroRecord {
  def appendCommand(command: CStructCommand) = {
    commands.append(command)
  }

  def replaceCommand(command: CStructCommand) = {
    // TODO: Linear search for xid.  Store hash for performance?
    val index = commands.indexWhere(x => x.xid == command.xid)
    if (index == -1) {
      // This commmand was not pending.
      appendCommand(command)
    } else if (commands(index).pending) {
      // Only update the existing command if it was pending.
      commands.update(index, command)
    }
  }

  // Correct flags for the command should already be set.
  def commitCommand(command: CStructCommand, logicalUpdater: LogicalRecordUpdater) = {
    // TODO: Linear search for xid.  Store hash for performance?
    val index = commands.indexWhere(x => x.xid == command.xid)
    if (index == -1) {
      // This commmand was not pending.
      commands.append(command)
      // Update all the states to include this command.  Only the states
      // have to change.
      val deltaBytes = command.command match {
        case LogicalUpdate(_, delta) => MDCCRecordUtil.fromBytes(delta).value
        case _ => None
      }

      if (deltaBytes.isDefined) {
        states = states.map(x => {
          x.state = logicalUpdater.applyDeltaBytes(Option(x.state), deltaBytes)
          x})
      }
    } else {
      commands.update(index, command)
      // Update all the states and the xid lists.
      val deltaBytes = command.command match {
        case LogicalUpdate(_, delta) => MDCCRecordUtil.fromBytes(delta).value
        case _ => None
      }

      if (deltaBytes.isDefined) {
        if (states.size == 1) {
          states = new ArrayBuffer[PendingStateInfo]
        } else if (states.size > 1) {
          states = states.filter(x => x.xids.exists(y => !y.contains(command.xid))).map(x => {
            x.xids = x.xids.filterNot(y => y.contains(command.xid))
            x.state = logicalUpdater.applyDeltaBytes(Option(x.state), deltaBytes)
            x})
        }
      }
    }
  }

  // Correct flags for the command should already be set.
  def abortCommand(command: CStructCommand) = {
    // TODO: Linear search for xid.  Store hash for performance?
    val index = commands.indexWhere(x => x.xid == command.xid)
    if (index != -1) {
      commands.remove(index)
      // Update the states and the xid lists to reflect this abort.
      if (states.size == 1) {
        states = new ArrayBuffer[PendingStateInfo]
      } else if (states.size > 1) {
        states = states.filter(x => x.xids.exists(y => !y.contains(command.xid))).map(x => {
          x.xids = x.xids.filterNot(y => y.contains(command.xid))
          x})
      }
    } else {
      // This command was not previously pending.  Append the aborted status.
      commands.append(command)
    }
  }

  def updateStates(newStates: Seq[PendingStateInfo]) = {
    states.clear
    states ++= newStates
  }
}

class PendingUpdatesController(override val db: TxDB[Array[Byte], Array[Byte]],
                               override val factory: TxDBFactory,
                               val keySchema: Schema,
                               val valueSchema: Schema,
                               val routingTable: MDCCRoutingTable) extends PendingUpdates {

  protected val logger = Logger(classOf[PendingUpdatesController])

  // Transaction state info. Maps txid -> txstatus/decision.
  private val txStatus = factory.getNewDB[ScadsXid, TxStatusEntry](
    db.getName + ".txstatus",
    new AvroKeySerializer[ScadsXid],
    new AvroValueSerializer[TxStatusEntry])
  // CStructs per key.
  private val pendingCStructs =
    factory.getNewDB[Array[Byte], PendingCommandsInfo](
      db.getName + ".pendingcstructs",
      new ByteArrayKeySerializer[Array[Byte]],
      new AvroValueSerializer[PendingCommandsInfo])

  // Detects conflicts for new updates.
  private var newUpdateResolver = new NewUpdateResolver(keySchema, valueSchema, valueICs)

  private val logicalRecordUpdater = new LogicalRecordUpdater(valueSchema)

  private var valueICs: FieldICList = null

  private var conflictResolver: ConflictResolver = null

  def setICs(ics: FieldICList) = {
    valueICs = ics
    newUpdateResolver = new NewUpdateResolver(keySchema, valueSchema, valueICs)
    conflictResolver = new ConflictResolver(valueSchema, valueICs)
    println("ics: " + valueICs)
  }

  // If accept was successful, returns the cstruct.  Otherwise, returns None.
  override def acceptOption(xid: ScadsXid, update: RecordUpdate, isFast: Boolean = false)(implicit dbTxn : TransactionData): (Boolean, Array[Byte], CStruct) = {
    val result = acceptOptionTxn(xid, update :: Nil, dbTxn)
    (result._1, result._2.head._1, result._2.head._2)
  }

  // Returns a tuple (success, list of (key, cstruct) pairs)
  override def acceptOptionTxn(xid: ScadsXid, updates: Seq[RecordUpdate], dbTxn: TransactionData = null, isFast: Boolean = false): (Boolean, Seq[(Array[Byte], CStruct)]) = {
    var success = true
    val txn = dbTxn match {
      case null => db.txStart()
      case x => x
    }
    val pendingCommandsTxn = pendingCStructs.txStart()
    var cstructs: Seq[(Array[Byte], CStruct)] = Nil
    try {
      cstructs = updates.map(r => {
        if (success) {
          val storedMDCCRec: Option[MDCCRecord] =
            db.get(txn, r.key).map(MDCCRecordUtil.fromBytes(_))
          val storedRecValue: Option[Array[Byte]] =
            storedMDCCRec match {
              case Some(v) => v.value
              case None => None
            }

          val commandsInfo = pendingCStructs.get(pendingCommandsTxn, r.key) match {
            case None => PendingCommandsInfo(None,
                                             new ArrayBuffer[CStructCommand],
                                             new ArrayBuffer[PendingStateInfo])
            case Some(c) => c
          }

          val numServers = routingTable.serversForKey(r.key).size

          // Add the updates to the pending list, if compatible.
          if (newUpdateResolver.isCompatible(xid, commandsInfo, storedMDCCRec, r, numServers, isFast)) {
            logger.debug("Update is compatible %s %s %s %s", xid, commandsInfo, storedMDCCRec, r)
            commandsInfo.appendCommand(CStructCommand(xid, r, true, true))
            pendingCStructs.put(pendingCommandsTxn, r.key, commandsInfo)
          } else {
            logger.debug("Update is not compatible %s %s %s %s", xid, commandsInfo, storedMDCCRec, r)
            success = false
          }
          (r.key, CStruct(commandsInfo.base, commandsInfo.commands))
        } else {
          (null, null)
        }
      })
    } catch {
      case e: Exception => {
        logger.debug("acceptOptionTxn Exception %s", e)
        success = false
      }
    }
    if (success) {
      pendingCStructs.txCommit(pendingCommandsTxn)
      if (dbTxn == null) {
        db.txCommit(txn)
      }
    } else {
      pendingCStructs.txAbort(pendingCommandsTxn)

      // On abort, still append a "reject" command to all the cstructs.
      val pendingCommandsTxn2 = pendingCStructs.txStart()
      cstructs = updates.map(r => {
        val commandsInfo = pendingCStructs.get(pendingCommandsTxn2, r.key) match {
          case None => PendingCommandsInfo(None,
                                           new ArrayBuffer[CStructCommand],
                                           new ArrayBuffer[PendingStateInfo])
          case Some(c) => c
        }
        commandsInfo.replaceCommand(CStructCommand(xid, r, true, false))
        pendingCStructs.put(pendingCommandsTxn2, r.key, commandsInfo)
        (r.key, CStruct(commandsInfo.base, commandsInfo.commands))
      })
      pendingCStructs.txCommit(pendingCommandsTxn2)

      if (dbTxn == null) {
        db.txAbort(txn)
      }
    }

    val entryStatus = success match {
      case true => Status.Accept.toString
      case false => Status.Reject.toString
    }

    // Merge the updates to the state of tx, in a transaction.
    val txStatusTxn = txStatus.txStart()
    val (newUpdates, emptyGet) = txStatus.get(txStatusTxn, xid) match {
      case None => (updates, true)
      case Some(s) => (s.updates ++ updates, false)
    }

    if (emptyGet) {
      val noOverwrite = txStatus.putNoOverwrite(txStatusTxn, xid, TxStatusEntry(entryStatus, newUpdates))
      if (!noOverwrite) {
        txStatus.put(txStatusTxn, xid, TxStatusEntry(entryStatus, newUpdates))
      }
    } else {
      txStatus.put(txStatusTxn, xid, TxStatusEntry(entryStatus, newUpdates))
    }
    txStatus.txCommit(txStatusTxn)

    (success, cstructs)
  }

  override def commit(xid: ScadsXid)(implicit dbTxn : TransactionData) : Boolean = {
    commitTxn(xid, dbTxn)
  }

  override def commitTxn(xid: ScadsXid, dbTxn : TransactionData = null) : Boolean = {
    // TODO: Handle out of order commits to same records.
    var success = true
    val txn = dbTxn match {
      case null => db.txStart()
      case x => x
    }
    val pendingCommandsTxn = pendingCStructs.txStart()
    val txStatusTxn = txStatus.txStart()

    txStatus.get(txStatusTxn, xid) match {
      case None => success = false
      case Some(s) => try {
        val decision = Status.withName(s.status)
        if (decision == Status.Commit || decision == Status.Abort) {
          // This tx is already committed or aborted.
          success = true
        } else {
          s.updates.foreach(r => {
            // TODO: These updates overwrite the metadata.  Probably have to
            //       selectively update only the value part of the record.
            r match {
              case LogicalUpdate(key, delta) => {
                db.get(txn, key) match {
                  case None => db.put(txn, key, delta)
                  case Some(recBytes) => {
                    val deltaRec = MDCCRecordUtil.fromBytes(delta)
                    val dbRec = MDCCRecordUtil.fromBytes(recBytes)
                    val newBytes = logicalRecordUpdater.applyDeltaBytes(dbRec.value, deltaRec.value)
                    val newRec = MDCCRecordUtil.toBytes(MDCCRecord(Some(newBytes), dbRec.metadata))
                    db.put(txn, key, newRec)
                  }
                }
              }
              case ValueUpdate(key, oldValue, newValue) => {
                db.put(txn, key, newValue)
              }
              case VersionUpdate(key, newValue) => {
                db.put(txn, key, newValue)
              }
            }

            // Commit the updates in the pending list.
            val commandsInfo = pendingCStructs.get(pendingCommandsTxn, r.key).getOrElse(PendingCommandsInfo(None, new ArrayBuffer[CStructCommand], new ArrayBuffer[PendingStateInfo]))
            commandsInfo.commitCommand(CStructCommand(xid, r, false, true), logicalRecordUpdater)

            pendingCStructs.put(pendingCommandsTxn, r.key, commandsInfo)
          })

          txStatus.put(txStatusTxn, xid, TxStatusEntry(Status.Commit.toString, s.updates))
        }
        txStatus.txCommit(txStatusTxn)
        pendingCStructs.txCommit(pendingCommandsTxn)
        if (dbTxn == null) {
          db.txCommit(txn)
        }
      } catch {
        case e: Exception => {
          println("commitTxn Exception " + e)
          e.printStackTrace()
        }
        txStatus.txAbort(txStatusTxn)
        pendingCStructs.txAbort(pendingCommandsTxn)
        if (dbTxn == null) {
          db.txAbort(txn)
        }
        success = false
      }
    }

    success
  }

  override def abort(xid: ScadsXid)(implicit dbTxn : TransactionData): Boolean = {
    abortTxn(xid, dbTxn)
  }

  override def abortTxn(xid: ScadsXid, dbTxn: TransactionData = null) : Boolean = {
    val pendingCommandsTxn = pendingCStructs.txStart()
    try {
      txStatus.get(null, xid) match {
        case None => {
          txStatus.put(null, xid, TxStatusEntry(Status.Abort.toString, List[RecordUpdate]()))
        }
        case Some(status) => {
          status.updates.foreach(r => {
            // Remove the updates in the pending list.
            val commandsInfo = pendingCStructs.get(pendingCommandsTxn, r.key).getOrElse(PendingCommandsInfo(None, new ArrayBuffer[CStructCommand], new ArrayBuffer[PendingStateInfo]))
            commandsInfo.abortCommand(CStructCommand(xid, r, false, false))

            pendingCStructs.put(pendingCommandsTxn, r.key, commandsInfo)
          })
          txStatus.put(null, xid, TxStatusEntry(Status.Abort.toString, status.updates))
        }
      }
      pendingCStructs.txCommit(pendingCommandsTxn)
      return true
    } catch {
      case e: Exception => {}
      pendingCStructs.txAbort(pendingCommandsTxn)
      return false
    }
  }

  // TODO(kraska): This probably doesn't belong here, since the conflict
  //               resolver is never used within PendingUpdates.  A
  //               ConflictResolver should just be created elsewhere.
  def getConflictResolver : ConflictResolver = conflictResolver

  def overwrite(key: Array[Byte], safeValue: CStruct, newUpdates: Seq[SinglePropose], isFast: Boolean = false)(implicit dbTxn: TransactionData) : Boolean = {
    overwriteTxn(key, safeValue, newUpdates, dbTxn, isFast)
}

  def overwriteTxn(key: Array[Byte], safeValue: CStruct, newUpdates: Seq[SinglePropose], dbTxn: TransactionData = null, isFast: Boolean = false): Boolean = {
    var success = true
    val txn = dbTxn match {
      case null => db.txStart()
      case x => x
    }
    val pendingCommandsTxn = pendingCStructs.txStart()

    // Apply all nonpending commands to the base of the cstruct.
    // TODO: This will apply all nonpending, committed updates, even if there
    //       are pending updates inter-mixed.  Not sure if that is correct...
    val newDBrec = ApplyUpdates.applyUpdatesToBase(
      logicalRecordUpdater, safeValue.value,
      safeValue.commands.filter(x => !x.pending && x.commit))

    val storedMDCCRec: Option[MDCCRecord] =
      db.get(txn, key).map(MDCCRecordUtil.fromBytes(_))
    val newMDCCRec = storedMDCCRec match {
      // TODO: I don't know if it is possible to not have a db record but have
      //       a cstruct.
      case None => throw new RuntimeException("When overwriting, db record should already exist.")
      case Some(r) => MDCCRecord(newDBrec, r.metadata)
    }

    // Update the stored cstruct.
    val commandsInfo = PendingCommandsInfo(safeValue.value,
                                           new ArrayBuffer[CStructCommand],
                                           new ArrayBuffer[PendingStateInfo])
    commandsInfo.commands ++= safeValue.commands

    // Update the pending states.
    // Assumption: The pending updates are all logical, or there is a single
    //             physical update.  Also, the command should already be
    //             compatible.
    val pending = safeValue.commands.filter(_.pending)
    pending.foreach(c => {
      if (!newUpdateResolver.isCompatible(c.xid, commandsInfo, newMDCCRec, c.command)) {
        throw new RuntimeException("All of the overwriting commands should be compatible.")
      }
    })

    // Store the new cstruct info.
    pendingCStructs.put(pendingCommandsTxn, key, commandsInfo)
    pendingCStructs.txCommit(pendingCommandsTxn)

    // Write the record to the database.
    db.put(txn, key, MDCCRecordUtil.toBytes(newMDCCRec))

    // Try the new updates.
    newUpdates.foreach(c => {
      acceptOption(c.xid, c.update, isFast)(dbTxn)
    })

    if (dbTxn == null) {
      db.txCommit(txn)
    }

    // TODO: Should the return value just be a boolean, or the cstruct, or
    //       something else?
    success
  }

  override def getDecision(xid: ScadsXid) = {
    txStatus.get(null, xid) match {
      case None => Status.Unknown
      case Some(s) => Status.withName(s.status)
    }
  }

  override def getCStruct(key: Array[Byte]) = {
    pendingCStructs.get(null, key) match {
      case None => CStruct(None, new ArrayBuffer[CStructCommand])
      case Some(c) => CStruct(c.base, c.commands)
    }
  }

  override def shutdown() = {
    txStatus.shutdown()
    pendingCStructs.shutdown()
  }
}

class NewUpdateResolver(val keySchema: Schema, val valueSchema: Schema,
                        val ics: FieldICList) {
  val avroUtil = new IndexedRecordUtil(valueSchema)
  val logicalRecordUpdater = new LogicalRecordUpdater(valueSchema)
  val icChecker = new ICChecker(valueSchema)

  protected val logger = Logger(classOf[NewUpdateResolver])

  def isCompatible(xid: ScadsXid,
                   commandsInfo: PendingCommandsInfo,
                   dbValue: Option[MDCCRecord],
                   newUpdate: RecordUpdate,
                   numServers: Int = 1,
                   isFast: Boolean = false): Boolean = {
    newUpdate match {
      case LogicalUpdate(key, delta) => {
        // TODO: what about deleted/non-existent records???
        if (!dbValue.isDefined) {
          throw new RuntimeException("base record should exist for logical updates")
        }
        val deltaRec = MDCCRecordUtil.fromBytes(delta)

        var oldStates = new HashMap[List[Byte], List[List[ScadsXid]]]()
        oldStates ++= commandsInfo.states.map(s => (s.state.toList, s.xids))
        var newStates = new HashMap[List[Byte], List[List[ScadsXid]]]()

        // Apply to base record first
        val newStateBytes = logicalRecordUpdater.applyDeltaBytes(dbValue.get.value, deltaRec.value)
        val newState = newStateBytes.toList
        val newXidList = oldStates.getOrElse(newState, List[List[ScadsXid]]()) ++ List(List(xid))

        var valid = newStates.put(newState, newXidList) match {
          case None => icChecker.check(avroUtil.fromBytes(newState.toArray), ics, dbValue.get.value, numServers, isFast)
          case Some(_) => true
        }

        if (!valid) {
          commandsInfo.updateStates(newStates.toList.map(x => PendingStateInfo(x._1.toArray, x._2)))
          false
        } else {

          // TODO: what if current old state is NOT currently in new states?
          commandsInfo.states.foreach(s => {
            if (valid) {
              val newState = logicalRecordUpdater.applyDeltaBytes(Option(s.state), deltaRec.value).toList
              val baseXidList = oldStates.get(s.state.toList).get.map(_ ++ List(xid))
              val newXidList = oldStates.getOrElse(newState, List[List[ScadsXid]]()) ++ baseXidList
              newStates.put(newState, newXidList)
              valid = newStates.put(newState, newXidList) match {
                case None => icChecker.check(avroUtil.fromBytes(newState.toArray), ics, Option(s.state), numServers, isFast)
                case Some(_) => true
              }
            }
          })

          commandsInfo.updateStates(newStates.toList.map(x => PendingStateInfo(x._1.toArray, x._2)))
          valid
        }
      }
      case ValueUpdate(key, oldValue, newValue) => {
        // Value updates conflict with all pending updates
        if (commandsInfo.commands.indexWhere(x => x.pending && x.commit) == -1) {
          // No pending commands.
          val newRec = MDCCRecordUtil.fromBytes(newValue)
          (oldValue, dbValue) match {
            case (Some(old), Some(dbRec)) => {

              // Record found in db, compare with the old version
              val oldRec = MDCCRecordUtil.fromBytes(old)

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
        } else {
          // There exists a pending command.  Value update is not compatible.
          // TODO: in a fast round, multiple physical updates are sometimes
          //       compatible.
          false
        }
      }
      case VersionUpdate(key, newValue) => {
        // Version updates conflict with all pending updates
        if (commandsInfo.commands.indexWhere(x => x.pending && x.commit) == -1) {
          // No pending commands.
          val newRec = MDCCRecordUtil.fromBytes(newValue)
          dbValue match {
            case Some(v) =>
              (newRec.metadata.currentVersion.round == v.metadata.currentVersion.round  + 1)
            case None => true
          }
        } else {
          // There exists a pending command.  Version update is not compatible.
          // TODO: in a fast round, multiple physical updates are sometimes
          //       compatible.
          false
        }
      }
    }
  } // isCompatible
}
