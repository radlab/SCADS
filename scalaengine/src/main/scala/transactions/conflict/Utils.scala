package edu.berkeley.cs.scads.storage
package transactions
package conflict

import java.io._
import org.apache.avro.io.{DecoderFactory, BinaryEncoder, BinaryDecoder, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader, SpecificRecord}
import org.apache.avro.Schema

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
        val rec = MDCCRecordUtil.fromBytes(relevantCommands.head.asInstanceOf[PhysicalUpdate].newValue)
        // The record value in the physical update.
        if (rec.value.isEmpty) {
          // If the physical update deletes the record, there should be no
          // subsequent logical updates.
          assert(remaining.isEmpty)
        }
        (rec.value, remaining)
      }
    }

    val newBase = Some(logicalRecordUpdater.applyDeltaBytes(base, remainingCommands.map(x => MDCCRecordUtil.fromBytes(x.asInstanceOf[LogicalUpdate].delta).value)))
    newBase
  }
}
