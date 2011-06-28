package edu.berkeley.cs.scads.axer

import org.apache.avro.io.{DatumWriter,Encoder}
import org.apache.avro.util.Utf8
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericRecord

class AxerDatumWriter[D <: GenericRecord] extends DatumWriter[D] {

  private var schema:AxerSchema = null

  /** Set the schema. */
  def setSchema(s:Schema) {
    schema = AxerSchema.getAxerSchema(s)
  }
  
  def setSchema(as:AxerSchema) {
    schema = as
  }

  /* this lets us extend DatumWriter
   * TODO, what if schema is null */
  def write(datum:D, out:Encoder) {
    write(schema, datum, out)
  }

  def write(avroschema:Schema, datum:D, out:Encoder) {
    write(AxerSchema.getAxerSchema(avroschema),datum,out)
  }

  def write(schema:AxerSchema, datum:D, out:Encoder) {
    // TODO: probably check that D is of right type
    out match {
      case ae:AxerEncoder => writeRecord(schema,datum,ae)
      case _ => throw new Exception("Can only write axer data with an axer encoder")
    }
  }

  private def writerForType(t:AxerType, out:AxerEncoder) = {
    t match {
      case BOOLEAN => out.writeBoolean _
      case INT => out.writeInt _
      case FLOAT => out.writeFloat _
      case LONG => out.writeLong _
      case DOUBLE => out.writeLong _
      case NULL => (_:Any) => out.writeNull
      case BYTES => out.writeBytes(_:Array[Byte])
      case FIXED => out.writeFixed(_:Array[Byte])
      case STRING => out.writeString(_:String)
      case t:ARRAY => writeArray(_:Array[_],t,out)
    }
  }


  private def writeArray(a:Array[_], t:ARRAY, out:AxerEncoder):Unit = {
    val writer = writerForType(t.elementType,out).asInstanceOf[Function1[Any,Unit]]
    out.setItemCount(a.length)
    t.elementType match {
      case NULL => out.writeInt(a.length) // just record the # of nulls
      case c:ConstType => a.foreach(elem => writer(elem))
      case v:VarType => {
        out.writeInt(a.length)
        // location to write offset of each element
        var itemLenOff = out.length
        // reserve space for offsets
        out.reserve(a.length*4)
        a.foreach(elem => {
          val sp = out.length
          writer(elem)
          out.writeIntInto(sp,itemLenOff)
          itemLenOff += 4
        })
      }
    }
  }

  private def writeRecord(schema:AxerSchema, rec:D, out:AxerEncoder) {
    out.reserve(4) // reserve 4 bytes for the record length
    schema.constFields.foreach(field_name => {
      schema.getType(field_name).asInstanceOf[ConstType] match {
        case BOOLEAN => out.writeBoolean(rec.get(field_name).asInstanceOf[Boolean])
        case INT => out.writeInt(rec.get(field_name).asInstanceOf[Int])
        case FLOAT => out.writeFloat(rec.get(field_name).asInstanceOf[Float])
        case LONG => out.writeLong(rec.get(field_name).asInstanceOf[Long])
        case DOUBLE => out.writeDouble(rec.get(field_name).asInstanceOf[Double])
        case FIXED => out.writeFixed(rec.get(field_name).asInstanceOf[Array[Byte]])
      }
    })
      
    var varLenOff = out.length // starting point for writing var length offsets
    // now we reserve an int for each var field where we will store its offset
    out.reserve(schema.varFields.length * 4)

    schema.varFields.foreach(field_name => {
      val sp = out.length
      schema.getType(field_name).asInstanceOf[VarType] match {
        case STRING => out.writeString(rec.get(field_name).asInstanceOf[String])
        case BYTES =>  out.writeBytes(rec.get(field_name).asInstanceOf[Array[Byte]])
        case t:ARRAY => writeArray(rec.get(field_name).asInstanceOf[Array[_]],t,out)
      }
      out.writeIntInto(sp,varLenOff)
      varLenOff += 4
    })
    out.writeIntInto(out.length,0)
  }

}
