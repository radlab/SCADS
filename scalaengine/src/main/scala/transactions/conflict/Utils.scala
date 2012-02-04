package edu.berkeley.cs.scads.storage
package transactions
package conflict

import java.io._
import org.apache.avro.io.{DecoderFactory, BinaryEncoder, BinaryDecoder, EncoderFactory}
import org.apache.avro.generic.{GenericDatumWriter, GenericDatumReader, IndexedRecord}
import org.apache.avro.Schema
import edu.berkeley.cs.scads.storage.transactions.mdcc.MDCCRecordHandler
import edu.berkeley.cs.scads.util.Logger

class IndexedRecordUtil(val schema: Schema) {
  val reader = new GenericDatumReader[IndexedRecord](schema)
  val writer = new GenericDatumWriter[IndexedRecord](schema)
  val out = new java.io.ByteArrayOutputStream(128)
  val encoder = EncoderFactory.get().binaryEncoder(out, null)

  def fromBytes(bytes: Array[Byte]): IndexedRecord = {
    reader.read(null, DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(bytes), null)).asInstanceOf[IndexedRecord]
  }

  def toBytes(record: IndexedRecord): Array[Byte] = {
    out.reset()
    writer.write(record, encoder)
    encoder.flush
    out.toByteArray
  }
}

class ICChecker(val schema: Schema) {
  val avroUtil = new IndexedRecordUtil(schema)
  protected val logger = Logger(classOf[ICChecker])

  protected val classicDemarcation = edu.berkeley.cs.scads.config.Config.config.getBool("scads.mdcc.classicDemarcation").getOrElse({
    logger.error("Config does not define scads.mdcc.classicDemarcation. Using classicDemarcation = false as default")
    val sysVal = System.getProperty("scads.mdcc.classicDemarcation")
    if (sysVal != null) {
      logger.error("Using system property for scads.mdcc.classicDemarcation = "  + sysVal)
      sysVal == "true"
    } else {
      logger.error("Config and system property do not define scads.mdcc.classicDemarcation. Using classicDemarcation = false as default")
      false
    }
  })

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

  def getQuorumLimit(baseField: Double, origLimit: Double, numServers: Int, isFast: Boolean): Double = {
    if (!isFast) {
      // If not fast, just keep the original limit.
      origLimit
    } else {
      // Fast limits have to be modified w.r.t. the fast quorum size.
      val quorumSize = if (classicDemarcation) {
        1
      } else {
        MDCCRecordHandler.fastQuorumSize(numServers)
      }
      if (baseField > origLimit) {
        (baseField - origLimit) * (numServers - quorumSize) / numServers + origLimit
      } else {
        origLimit - (origLimit - baseField) * (numServers - quorumSize) / numServers
      }
    }
  }

  // TODO(gpang): baserec is really only used to see if it was a decrement or
  //              increment.  just pass in the actual logical update to get
  //              that info.
  def check(rec: IndexedRecord, ics: FieldICList, safeRecBytes: Option[Array[Byte]], baseRecBytes: Option[Array[Byte]], numServers: Int = 1, isFast: Boolean = false): Boolean = {
    var valid = true
    if (ics == null) {
      true
    } else {
      if (baseRecBytes.isEmpty) {
        throw new RuntimeException("icchecker: base rec should not be None.")
      }
      if (safeRecBytes.isEmpty) {
        throw new RuntimeException("icchecker: safe rec should not be None.")
      }
      val baseRec = avroUtil.fromBytes(baseRecBytes.get)
      val safeRec = avroUtil.fromBytes(safeRecBytes.get)
      ics.ics.foreach(ic => {
        if (valid) {
          val field = convertFieldToDouble(rec.get(ic.fieldPos))
          val baseField = convertFieldToDouble(baseRec.get(ic.fieldPos))
          val safeField = convertFieldToDouble(safeRec.get(ic.fieldPos))

          if (field < baseField) {
            // Check lower bound.
            valid = ic.lower match {
              case None => true
              case Some(FieldRestrictionGT(x)) =>
                logger.debug(" " + Thread.currentThread.getName + " > new field: " + field + " safe field: " + safeField + " origLimit: " + x + " newLimit: " + getQuorumLimit(safeField, x, numServers, isFast) + " numServers: " + numServers + " isFast: " + isFast)
                field > getQuorumLimit(safeField, x, numServers, isFast)
              case Some(FieldRestrictionGE(x)) =>
                logger.debug(" " + Thread.currentThread.getName + " >= new field: " + field + " safe field: " + safeField + " origLimit: " + x + " newLimit: " + getQuorumLimit(safeField, x, numServers, isFast) + " numServers: " + numServers + " isFast: " + isFast)
                field >= getQuorumLimit(safeField, x, numServers, isFast)
              case _ => false
            }
          } else if (field > baseField) {
            // Check upper bound.
            valid = ic.upper match {
              case None => true
              case Some(FieldRestrictionLT(x)) =>
                logger.debug(" " + Thread.currentThread.getName + " < new field: " + field + " safe field: " + safeField + " origLimit: " + x + " newLimit: " + getQuorumLimit(safeField, x, numServers, isFast) + " numServers: " + numServers + " isFast: " + isFast)
                field < getQuorumLimit(safeField, x, numServers, isFast)
              case Some(FieldRestrictionLE(x)) =>
                logger.debug(" " + Thread.currentThread.getName + " <= new field: " + field + " safe field: " + safeField + " origLimit: " + x + " newLimit: " + getQuorumLimit(safeField, x, numServers, isFast) + " numServers: " + numServers + " isFast: " + isFast)
                field <= getQuorumLimit(safeField, x, numServers, isFast)
              case _ => false
            }
          }

        }
      })
      valid
    }
  }
}

object ApplyUpdates {
  // updates is a sequence of nonpending, committed updates, which will be
  // applied to the base record.  The result is returned.
  def applyUpdatesToBase(logicalRecordUpdater: LogicalRecordUpdater,
                         origBase: Option[Array[Byte]],
                         updates: Seq[CStructCommand]): Option[Array[Byte]] = {
    // Just need apply last physical update, and possible additional
    // logical updates.
    val lastPhysical = updates.lastIndexWhere(_.command match {
      case LogicalUpdate(_, _) => false
      case _ => true
    })

    // If the base is empty, a physical update must exist.
    // If the base exists, a physical update may or may not exist.
    // base is the (optional) byte array of the record.
    // remainingCommands is a seq of logical updates to apply to the base.
    val (base, remainingCommands) = lastPhysical match {
      case -1 => {
        // If no physical update, base cannot be empty.
        assert(!origBase.isEmpty)
        (origBase, updates)
      }
      case _ => {
        val relevantCommands = updates.drop(lastPhysical)
        val remaining = relevantCommands.tail
        val rec = MDCCRecordUtil.fromBytes(relevantCommands.head.command.asInstanceOf[PhysicalUpdate].newValue)
        // The record value in the physical update.
        if (rec.value.isEmpty) {
          // If the physical update deletes the record, there should be no
          // subsequent logical updates.
          assert(remaining.isEmpty)
        }
        (rec.value, remaining)
      }
    }

    val newBase = Some(logicalRecordUpdater.applyDeltaBytes(base, remainingCommands.map(x => MDCCRecordUtil.fromBytes(x.command.asInstanceOf[LogicalUpdate].delta).value)))
    newBase
  }
}
