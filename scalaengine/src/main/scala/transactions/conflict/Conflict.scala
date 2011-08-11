package edu.berkeley.cs.scads.storage.transactions.conflict

import edu.berkeley.cs.scads.comm._

import edu.berkeley.cs.scads.storage.MDCCRecordReaderWriter
import edu.berkeley.cs.scads.storage.transactions._

import actors.threadpool.ThreadPoolExecutor.AbortPolicy
import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.ConcurrentHashMap
import java.util.Arrays

import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._

import java.io._
import org.apache.avro._
import org.apache.avro.io.{BinaryData, DecoderFactory, BinaryEncoder, BinaryDecoder, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader, SpecificRecordBase, SpecificRecord}

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
  // If accepted, returns all the cstructs for all the keys.  Otherwise, None
  // is returned.
  def accept(xid: ScadsXid, updates: Seq[RecordUpdate]): Option[Seq[(Array[Byte], CStruct)]]

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

  def isCompatible(commands: Seq[CStructCommand],
                   dbValue: Option[MDCCRecord],
                   newUpdate: RecordUpdate): Boolean
}

// Status of a transaction.  Stores all the updates in the transaction.
@serializable
case class TxStatusEntry(var status: Status.Status,
                         var updates: Seq[RecordUpdate])

class PendingUpdatesController(override val db: TxDB[Array[Byte], Array[Byte]],
                               override val factory: TxDBFactory) extends PendingUpdates {

  // Transaction state info. Maps txid -> txstatus/decision.
  private val txStatus =
    factory.getNewDB[ScadsXid, TxStatusEntry](db.getName + ".txstatus")
  // CStructs per key.
  private val pendingCStructs =
    factory.getNewDB[Array[Byte], ArrayBuffer[CStructCommand]](db.getName + ".pendingcstructs")

  // (de)serialize MDCCRecords from the db.
  private val recReaderWriter = new MDCCRecordReaderWriter

  // Detects conflicts for new updates.
  private val conflictResolver = new SimpleConflictResolver(recReaderWriter)

  override def accept(xid: ScadsXid, updates: Seq[RecordUpdate]) = {
    var success = true
    val txn = db.txStart()
    val pendingCommandsTxn = pendingCStructs.txStart()
    var cstructs: Seq[(Array[Byte], CStruct)] = null
    try {
      cstructs = updates.map(r => {
        if (success) {
          val storedMDCCRec: Option[MDCCRecord] =
            db.get(txn, r.key).map(recReaderWriter.fromBytes(_))
          val storedRecValue: Option[Array[Byte]] =
            storedMDCCRec match {
              case Some(v) => v.value
              case None => None
            }

          val commands = pendingCStructs.get(pendingCommandsTxn, r.key) match {
            case None => new ArrayBuffer[CStructCommand]
            case Some(c) => c
          }

          // Add the updates to the pending list, if compatible.
          if (conflictResolver.isCompatible(commands, storedMDCCRec, r)) {
            // No conflict
            commands.append(CStructCommand(xid, r, true))
            pendingCStructs.put(pendingCommandsTxn, r.key, commands)
          } else {
            success = false
          }
          (r.key, CStruct(storedRecValue, commands))
        } else {
          (null, null)
        }
      })
    } catch {
      case e: Exception => {}
      success = false
    }
    if (success) {
      db.txCommit(txn)
      pendingCStructs.txCommit(pendingCommandsTxn)
      // TODO: Handle the case when the commit arrives before the prepare.
      txStatus.putNoOverwrite(null, xid, TxStatusEntry(Status.Accept, updates))
    } else {
      db.txAbort(txn)
      pendingCStructs.txAbort(pendingCommandsTxn)
      // TODO: Handle the case when the commit arrives before the prepare.
      txStatus.putNoOverwrite(null, xid, TxStatusEntry(Status.Reject, updates))
      cstructs = null
    }
    Option(cstructs)
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
          case LogicalUpdate(key, schema, delta) => {
            db.get(txn, key) match {
              case None => db.put(txn, key, delta)
              case Some(recBytes) => {
                val deltaRec = recReaderWriter.fromBytes(delta)
                val dbRec = recReaderWriter.fromBytes(recBytes)
                val newBytes = LogicalRecordUpdater.applyDeltaBytes(schema, dbRec.value, deltaRec.value)
                val newRec = recReaderWriter.toBytes(MDCCRecord(Some(newBytes), dbRec.metadata))
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
        val commands = pendingCStructs.get(pendingCommandsTxn, r.key) match {
          case None => {
            val c = new ArrayBuffer[CStructCommand]
            c.append(CStructCommand(xid, r, false))
            c
          }
          case Some(c) => {
            // TODO: For now, linear search for xid.  Hash for performance?
            val index = c.indexWhere(x => x.xid == xid)
            if (index == -1) {
              // Update does not exist.
              c.append(CStructCommand(xid, r, false))
            } else {
              // Mark the update committed.
              c.update(index, CStructCommand(xid, r, false))
            }
            c
          }
        }
        pendingCStructs.put(pendingCommandsTxn, r.key, commands)
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

  override def abort(xid: ScadsXid) = {
    val pendingCommandsTxn = pendingCStructs.txStart()
    try {
      txStatus.get(null, xid) match {
        case None => {
          txStatus.put(null, xid, TxStatusEntry(Status.Abort, List[RecordUpdate]()))
        }
        case Some(status) => {
          status.updates foreach(r => {
            // Remove the updates in the pending list.
            val commands = pendingCStructs.get(pendingCommandsTxn, r.key) match {
              case None => new ArrayBuffer[CStructCommand]
              case Some(c) => {
                // TODO: For now, linear search for xid.  Hash for performance?
                val index = c.indexWhere(x => x.xid == xid)
                if (index != -1) {
                  // Remove the update committed.
                  c.remove(index)
                }
                c
              }
            }
            pendingCStructs.put(pendingCommandsTxn, r.key, commands)
          })
          txStatus.put(null, xid, TxStatusEntry(Status.Abort, status.updates))
        }
      }
      pendingCStructs.txCommit(pendingCommandsTxn)
    } catch {
      case e: Exception => {}
      pendingCStructs.txAbort(pendingCommandsTxn)
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

class SimpleConflictResolver(val recReaderWriter: MDCCRecordReaderWriter) {
  def getLUB(sequences: Array[CStruct]): CStruct = {
    null
  }

  def getGLB(sequences: Array[CStruct]): CStruct = {
    null
  }

  def isCompatible(commands: Seq[CStructCommand],
                   dbValue: Option[MDCCRecord],
                   newUpdate: RecordUpdate): Boolean = {
    newUpdate match {
      case LogicalUpdate(key, schema, delta) => {
        // TODO: do IC
        true
      }
      case ValueUpdate(key, oldValue, newValue) => {
        // Value updates conflict with all pending updates
        if (commands.indexWhere(x => x.pending) == -1) {
          // No pending commands.
          val newRec = recReaderWriter.fromBytes(newValue)
          (oldValue, dbValue) match {
            case (Some(old), Some(dbRec)) => {

              // Record found in db, compare with the old version
              val oldRec = recReaderWriter.fromBytes(old)

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
          false
        }
      }
      case VersionUpdate(key, newValue) => {
        // Version updates conflict with all pending updates
        if (commands.indexWhere(x => x.pending) == -1) {
          // No pending commands.
          val newRec = recReaderWriter.fromBytes(newValue)
          dbValue match {
            case Some(v) =>
              (newRec.metadata.currentRound == v.metadata.currentRound + 1)
            case None => true
          }
        } else {
          // There exists a pending command.  Version update is not compatible.
          false
        }
      }
    }
  } // isCompatible
}

object LogicalRecordUpdater {
  // base is the (optional) byte array of the serialized AvroRecord.
  // deltal is the (optional) byte array of the serialized delta AvroRecord.
  // An byte array of the serialized resulting record is returned.
  def applyDeltaBytes(schema: String, baseBytes: Option[Array[Byte]], deltaBytes: Option[Array[Byte]]): Array[Byte] = {
    if (deltaBytes.isEmpty) {
      throw new RuntimeException("Delta records should always exist.")
    }
    baseBytes match {
      case None => deltaBytes.get
      case Some(avroBytes) => {
        val s = Schema.parse(schema)
        val reader = new SpecificDatumReader[SpecificRecord](s)
        val avro = reader.read(null, DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(avroBytes), null)).asInstanceOf[SpecificRecord]
        val avroDelta = reader.read(null, DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(deltaBytes.get), null)).asInstanceOf[SpecificRecord]
        val avroNew = applyDeltaRecord(avro, avroDelta)
        val writer = new SpecificDatumWriter[SpecificRecord](s)
        val out = new java.io.ByteArrayOutputStream(128)
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        writer.write(avroNew, encoder)
        encoder.flush
        out.toByteArray
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
