package edu.berkeley.cs.scads.axer

import org.apache.avro.io.{DatumReader,Decoder}
import org.apache.avro.util.Utf8
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericRecord,GenericData}


class AxerDatumReader[D <: GenericRecord] extends DatumReader[D] {

  private var schema:AxerSchema = null
  private var expected:Schema = null

  private var constList:List[(String,Int,(AxerDecoder)=>AnyVal)] = Nil

  def setSchema(avroSchema:Schema) {
    expected = avroSchema
    schema = AxerSchema.getAxerSchema(avroSchema)
  }

  def setSchema(as:AxerSchema) {
    schema = as

    schema.constFields.foreach(field_name => {
      constList = (field_name,
                   as.getOffset(field_name),
                   as.getType(field_name).asInstanceOf[ConstType] match {
                     case BOOLEAN => (dec:AxerDecoder)=>{dec.readBoolean}
                     case INT     => (dec:AxerDecoder)=>{dec.readInt}
                     case FLOAT   => (dec:AxerDecoder)=>{dec.readFloat}
                     case LONG    => (dec:AxerDecoder)=>{dec.readLong}
                     case DOUBLE  => (dec:AxerDecoder)=>{dec.readDouble}
                     case FIXED   => (dec:AxerDecoder)=>{
                       val len = dec.readInt
                       val ret = new Array[Byte](len)
                       dec.readFixed(ret,0,len)
                       ret.asInstanceOf[AnyVal]
                     }
                   }) :: constList
    })
    constList = constList.reverse
  }

  def setExpected(avroSchema:Schema) {
    expected = avroSchema
  }

  def read(reuse:D, in:Decoder):D = {
    in match {
      case ad:AxerDecoder => {
        val ret =
          if (reuse == null)
            new GenericData.Record(expected)
          else
            reuse
        //schema.constFields.foreach(field_name => {
          //ret.put(field_name,readField(field_name,ad))
        //})
        constList.foreach(or => {
          ad.setOffset(or._2)
          ret.put(or._1,or._3(ad))
        })
        schema.varFields.foreach(field_name => {
          ret.put(field_name,readField(field_name,ad))
        })
        ret.asInstanceOf[D]
      }
      case _ => throw new Exception("Can only use AxerDatumReader with AxerDecoder")
    }
  }

  private def readConst(field:String, t:ConstType, dec:AxerDecoder):AnyVal = {
    dec.setOffset(schema.getOffset(field))
    t match {
      case BOOLEAN => dec.readBoolean
      case INT     => dec.readInt
      case FLOAT   => dec.readFloat
      case LONG    => dec.readLong
      case DOUBLE  => dec.readDouble
      case FIXED   => {
        val len = dec.readInt
        val ret = new Array[Byte](len)
        dec.readFixed(ret,0,len)
        ret.asInstanceOf[AnyVal]
      }
    }
  }

  private abstract class ArrayReader[T : Manifest](length:Int, dec:AxerDecoder, reader:Function0[T]) {
    val array = new Array[T](length)
    /* When calling this the decoder MUST already have its offset
     * at the first item  */
    def fillArray():Array[T]
  }

  private class ConstArrayReader[T : Manifest](numBytes:Int, skipAmt:Int, dec:AxerDecoder, reader:Function0[T]) extends ArrayReader[T](numBytes/skipAmt,dec,reader) {
    /* When calling this the decoder MUST already have its offset
     * at the first item  */
    def fillArray():Array[T] = {
      for (i <- 0 to (array.length-1)) {
        array(i) = reader()
        dec.offset += skipAmt
      }
      array
    }
  }

  private class VarArrayReader[T : Manifest](length:Int, finalByte:Int, dec:AxerDecoder, reader:Function0[T]) extends ArrayReader[T](length,dec,reader) {
    def fillArray():Array[T] = {
      var itemLenOff = dec.offset
      for (i <- 0 to (length - 1)) {
        dec.offset = itemLenOff
        val sp = dec.readInt
        dec.varLen =
          if (i == (length - 1))
            finalByte - sp
          else {
            dec.offset += 4
            dec.readInt - sp
          }
        dec.offset = sp
        array(i) = reader()
        itemLenOff += 4
      }
      array
    }
  }

  private class NullArrayReader(length:Int) extends ArrayReader[Null](length,null,null) {
    def fillArray():Array[Null] = {
      array
    }
  }

  private def readerForType(ax:AxerType, numBytes:Int, dec:AxerDecoder) = {
    ax match {
      case c:ConstType => {
        val skipAmt = AxerSchema.numBytesForType(c)
        c match {
          case BOOLEAN => new ConstArrayReader[Boolean](numBytes, skipAmt, dec, dec.readBoolean _)
          case INT     => new ConstArrayReader[Int](numBytes, skipAmt, dec, dec.readInt _)
          case FLOAT   => new ConstArrayReader[Float](numBytes, skipAmt, dec, dec.readFloat _)
          case LONG    => new ConstArrayReader[Long](numBytes, skipAmt, dec, dec.readLong _)
          case DOUBLE  => new ConstArrayReader[Double](numBytes, skipAmt, dec, dec.readDouble _)
          case FIXED   => throw new Exception("Hrmm, fixed in array")
        }
      }
      case v:VarType => {
        val finalByte = dec.offset + numBytes
        val len = dec.readInt
        dec.offset += 4
        v match {
          case STRING => new VarArrayReader[String](len, finalByte, dec, () => {dec.readString(null).toString})
          case BYTES => new VarArrayReader[java.nio.ByteBuffer](len, finalByte, dec, () =>{dec.readBytes(null)})
          case t:ARRAY => throw new Exception("Array of array.... hrmm")
        }
      }
      case NULL => {
        val len = dec.readInt
        new NullArrayReader(len)
      }
    }
  }

  private def readVar(field:String, v:VarType, dec:AxerDecoder):AnyRef = {
    dec.setOffsetVar(field,schema)
    v match {
      case STRING => dec.readString(null)
      case BYTES => dec.readBytes(null)
      case t:ARRAY => {
        val numBytes = dec.readArrayStart().asInstanceOf[Int]
        val reader = readerForType(t.elementType,numBytes,dec)
        reader.fillArray
        reader.array
      }
    }
  }

  def readField(field:String,ad:AxerDecoder):AnyRef = {
    schema.getType(field) match {
      case c:ConstType => readConst(field,c,ad).asInstanceOf[AnyRef]
      case v:VarType => readVar(field,v,ad)
      case NULL => null
    }
  }

}
