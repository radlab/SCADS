package edu.berkeley.cs.scads.storage
package transactions
package conflict

import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._

import org.apache.avro.specific.SpecificRecord
import org.apache.avro.Schema

class LogicalRecordUpdater(val schema: Schema) {
  val avroUtil = new SpecificRecordUtil(schema)

  // base is the (optional) byte array of the serialized AvroRecord.
  // delta is the (optional) byte array of the serialized delta AvroRecord.
  // A byte array of the serialized resulting record is returned.
  def applyDeltaBytes(baseBytes: Option[Array[Byte]], deltaBytes: Option[Array[Byte]]): Array[Byte] = {
    applyDeltaBytes(baseBytes, List(deltaBytes))
  }

  def applyDeltaBytes(baseBytes: Option[Array[Byte]], deltaBytesList: Seq[Option[Array[Byte]]]): Array[Byte] = {
    if(deltaBytesList.length == 0){
      //Gene is this correct?
      assert(baseBytes.isDefined)
      return baseBytes.get
    }

    val headDelta = deltaBytesList.head
    val baseAvro = baseBytes match {
      case None => avroUtil.fromBytes(headDelta.get)
      case Some(avroBytes) => {
        val avro = avroUtil.fromBytes(avroBytes)
        val avroDelta = avroUtil.fromBytes(headDelta.get)
        applyDeltaRecord(avro, avroDelta)
      }
    }

    val result = deltaBytesList.tail.foldLeft(baseAvro)((avro, deltaBytes) => {
      assert(!deltaBytes.isEmpty)
      val avroDelta = avroUtil.fromBytes(deltaBytes.get)
      applyDeltaRecord(avro, avroDelta)
    })

    avroUtil.toBytes(result)
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
