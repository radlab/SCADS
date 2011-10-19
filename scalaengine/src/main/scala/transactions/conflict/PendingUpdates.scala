package edu.berkeley.cs.scads.storage
package transactions
package conflict

import actors.threadpool.ThreadPoolExecutor.AbortPolicy
import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.ConcurrentHashMap
import java.util.Arrays

import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import java.io._
import org.apache.avro._
import org.apache.avro.io.{BinaryData, DecoderFactory, BinaryEncoder, BinaryDecoder, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader, SpecificRecordBase, SpecificRecord}
import org.apache.avro.Schema

import edu.berkeley.cs.avro.marker._

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
  def acceptOption(xid: ScadsXid, updates: Seq[RecordUpdate])(implicit txn : TransactionData) : (Boolean, Seq[(Array[Byte], CStruct)])


  def acceptOption(xid: ScadsXid, update: RecordUpdate)(implicit txn : TransactionData) : (Boolean, Array[Byte], CStruct)


  /**
   * The transaction was successful (we will never decide otherwise)
   */
  def commit(xid: ScadsXid)(implicit txn : TransactionData) : Boolean

  /**
   * The transaction was learned as aborted (we will never decide otherwise)
   */
  def abort(xid: ScadsXid)(implicit txn : TransactionData)  : Boolean

  /**
   * Writes the new truth. Should only return false if something is messed up with the db
   */
  def overwrite(key: Array[Byte], safeValue: CStruct, newUpdates : Seq[RecordUpdate])(implicit txn : TransactionData) : Boolean

  // Value is chosen (reflected in the db) and confirms trx state.
  @deprecated def commit(xid: ScadsXid, updates: Seq[RecordUpdate]): Boolean



  def getDecision(xid: ScadsXid): Status.Status

  def getCStruct(key: Array[Byte]): Option[CStruct]

  def startup() = {}

  def shutdown() = {}

  def setICs(ics: FieldICList)

  def getConflictResolver : ConflictResolver
}

// TODO: Enumerations do not work with AvroRecords. Must be changed if we want
//       to use AvroRecords for serialization.
// Status of a transaction.  Stores all the updates in the transaction.
case class TxStatusEntry(var status: Status.Status,
                         var updates: Seq[RecordUpdate]) extends Serializable

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
                               val valueSchema: Schema) extends PendingUpdates {

  def getConflictResolver : ConflictResolver = throw new RuntimeException("Gene implement me") //TODO implement

  def overwrite(key: Array[Byte], safeValue: CStruct, newUpdates : Seq[RecordUpdate])(implicit txn : TransactionData) : Boolean = throw new RuntimeException("Gene implement me")
  def commit(xid: ScadsXid)(implicit txn : TransactionData) : Boolean = throw new RuntimeException("Gene implement me")


  // Transaction state info. Maps txid -> txstatus/decision.
  private val txStatus =
    factory.getNewDB[ScadsXid, TxStatusEntry](db.getName + ".txstatus")
  // CStructs per key.
  private val pendingCStructs =
    factory.getNewDB[Array[Byte], PendingCommandsInfo](db.getName + ".pendingcstructs")

  // Detects conflicts for new updates.
  private var newUpdateResolver = new NewUpdateResolver(keySchema, valueSchema, valueICs)

  private val logicalRecordUpdater = new LogicalRecordUpdater(valueSchema)

  private var valueICs: FieldICList = null

  def setICs(ics: FieldICList) = {
    valueICs = ics
    newUpdateResolver = new NewUpdateResolver(keySchema, valueSchema, valueICs)
    println("ics: " + valueICs)
  }

  // If accept was successful, returns the cstruct.  Otherwise, returns None.
  override def acceptOption(xid: ScadsXid, update: RecordUpdate)(implicit txn : TransactionData): (Boolean, Array[Byte], CStruct) = {
    val result = acceptOption(xid, update :: Nil)
    (result._1, result._2.head._1, result._2.head._2)
  }

  // Returns a tuple (success, list of (key, cstruct) pairs)
  override def acceptOption(xid: ScadsXid, updates: Seq[RecordUpdate])(implicit txn : TransactionData): (Boolean, Seq[(Array[Byte], CStruct)]) = {
    var success = true
    val txn = db.txStart()
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

          // Add the updates to the pending list, if compatible.
          if (newUpdateResolver.isCompatible(xid, commandsInfo, storedMDCCRec, r)) {
            // No conflict
            commandsInfo.appendCommand(CStructCommand(xid, r, true, true))
            pendingCStructs.put(pendingCommandsTxn, r.key, commandsInfo)
          } else {
            success = false
          }
          (r.key, CStruct(commandsInfo.base, commandsInfo.commands))
        } else {
          (null, null)
        }
      })
    } catch {
      case e: Exception => {}
      success = false
    }
    if (success) {
      pendingCStructs.txCommit(pendingCommandsTxn)
      db.txCommit(txn)
      // TODO: Handle the case when the commit arrives before the prepare.
      txStatus.putNoOverwrite(null, xid, TxStatusEntry(Status.Accept, updates))
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

      db.txAbort(txn)
      // TODO: Handle the case when the commit arrives before the prepare.
      txStatus.putNoOverwrite(null, xid, TxStatusEntry(Status.Reject, updates))
    }
    (success, cstructs)
  }

  override def commit(xid: ScadsXid, updates: Seq[RecordUpdate]) = {
    // TODO: Handle out of order commits to same records.
    var success = true
    val txn = db.txStart()
    val pendingCommandsTxn = pendingCStructs.txStart()
    try {
      updates.foreach(r => {
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
      db.txCommit(txn)
      pendingCStructs.txCommit(pendingCommandsTxn)
    } catch {
      case e: Exception => {}
      db.txAbort(txn)
      pendingCStructs.txAbort(pendingCommandsTxn)
      success = false
    }

    txStatus.put(null, xid, TxStatusEntry(Status.Commit, updates))
    success
  }

  override def abort(xid: ScadsXid)(implicit txn : TransactionData) : Boolean = {
    val pendingCommandsTxn = pendingCStructs.txStart()
    try {
      txStatus.get(null, xid) match {
        case None => {
          txStatus.put(null, xid, TxStatusEntry(Status.Abort, List[RecordUpdate]()))
        }
        case Some(status) => {
          status.updates.foreach(r => {
            // Remove the updates in the pending list.
            val commandsInfo = pendingCStructs.get(pendingCommandsTxn, r.key).getOrElse(PendingCommandsInfo(None, new ArrayBuffer[CStructCommand], new ArrayBuffer[PendingStateInfo]))
            commandsInfo.abortCommand(CStructCommand(xid, r, false, false))

            pendingCStructs.put(pendingCommandsTxn, r.key, commandsInfo)
          })
          txStatus.put(null, xid, TxStatusEntry(Status.Abort, status.updates))
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
    pendingCStructs.shutdown()
  }
}

class NewUpdateResolver(val keySchema: Schema, val valueSchema: Schema,
                        val ics: FieldICList) {
  val util = new SpecificRecordUtil(valueSchema)
  val logicalRecordUpdater = new LogicalRecordUpdater(valueSchema)

  def isCompatible(xid: ScadsXid,
                   commandsInfo: PendingCommandsInfo,
                   dbValue: Option[MDCCRecord],
                   newUpdate: RecordUpdate): Boolean = {
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
          case None => ICChecker.check(util.fromBytes(newState.toArray), ics)
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
                case None => ICChecker.check(util.fromBytes(newState.toArray), ics)
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
              (newRec.metadata.currentRound == v.metadata.currentRound + 1)
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

class SpecificRecordUtil(val schema: Schema) {
  val reader = new SpecificDatumReader[SpecificRecord](schema)
  val writer = new SpecificDatumWriter[SpecificRecord](schema)
  val out = new java.io.ByteArrayOutputStream(128)
  val encoder = EncoderFactory.get().binaryEncoder(out, null)

  def fromBytes(bytes: Array[Byte]): SpecificRecord = {
    reader.read(null, DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(bytes), null)).asInstanceOf[SpecificRecord]
  }

  def toBytes(record: SpecificRecord): Array[Byte] = {
    out.reset()
    writer.write(record, encoder)
    encoder.flush
    out.toByteArray
  }
}

class LogicalRecordUpdater(val schema: Schema) {
  val util = new SpecificRecordUtil(schema)

  // base is the (optional) byte array of the serialized AvroRecord.
  // delta is the (optional) byte array of the serialized delta AvroRecord.
  // A byte array of the serialized resulting record is returned.
  def applyDeltaBytes(baseBytes: Option[Array[Byte]], deltaBytes: Option[Array[Byte]]): Array[Byte] = {
    if (deltaBytes.isEmpty) {
      throw new RuntimeException("Delta records should always exist.")
    }
    baseBytes match {
      case None => deltaBytes.get
      case Some(avroBytes) => {
        val avro = util.fromBytes(avroBytes)
        val avroDelta = util.fromBytes(deltaBytes.get)
        val avroNew = applyDeltaRecord(avro, avroDelta)
        util.toBytes(avroNew)
      }
    }
  }

  private def applyDeltaRecord(base: SpecificRecord, delta: SpecificRecord): SpecificRecord = {
    val schema = delta.getSchema
    val fields: Buffer[org.apache.avro.Schema.Field] = schema.getFields

    fields.foreach(field => {
      val fieldDelta = delta.get(field.pos)
      val baseField = base.get(field.pos)
      val newField: AnyRef = (baseField, fieldDelta) match {
        case (x: java.lang.Integer, y: java.lang.Integer) => {
          if (y == 0) {
            null
          } else {
            new java.lang.Integer(x.intValue + y.intValue)
          }
        }
        case (x: String, y: String) => {
          if (y.length == 0) {
            null
          } else {
            y
          }
        }
        case (x: org.apache.avro.util.Utf8, y: org.apache.avro.util.Utf8) => {
          if (y.length == 0) {
            null
          } else {
            y
          }
        }
        case (x: java.lang.Long, y: java.lang.Long) => {
          if (y == 0) {
            null
          } else {
            new java.lang.Long(x.longValue + y.longValue)
          }
        }
        case (x: java.lang.Float, y: java.lang.Float) => {
          if (y == 0) {
            null
          } else {
            new java.lang.Float(x.floatValue + y.floatValue)
          }
        }
        case (x: java.lang.Double, y: java.lang.Double) => {
          if (y == 0) {
            null
          } else {
            new java.lang.Double(x.doubleValue + y.doubleValue)
          }
        }
        case (_, _) => null
      }
      if (newField != null) {
        base.put(field.pos, newField)
      }
    })
    base
  }
}

object ICChecker {
  private def convertFieldToDouble(f: Any): Double = {
    f match {
      case x: java.lang.Integer => x.doubleValue
      case x: String => 0
      case x: org.apache.avro.util.Utf8 => 0
      case x: java.lang.Long => x.doubleValue
      case x: java.lang.Float => x.doubleValue
      case x: java.lang.Double => x
      case _ => 0
    }
  }

  def check(rec: SpecificRecord, ics: FieldICList): Boolean = {
    var valid = true
    if (ics == null) {
      true
    } else {
      ics.ics.foreach(ic => {
        if (valid) {
          val doubleField = convertFieldToDouble(rec.get(ic.fieldPos))

          val lowerValid = ic.lower match {
            case None => true
            case Some(FieldRestrictionGT(x)) => doubleField > x
            case Some(FieldRestrictionGE(x)) => doubleField >= x
            case _ => false
          }

          valid = lowerValid && (ic.upper match {
            case None => true
            case Some(FieldRestrictionLT(x)) => doubleField < x
            case Some(FieldRestrictionLE(x)) => doubleField <= x
            case _ => false
          })
        }
      })
      valid
    }
  }
}
