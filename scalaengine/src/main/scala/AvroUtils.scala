package edu.berkeley.cs.scads.storage

import org.apache.avro.Schema
import org.apache.avro.io.BinaryDecoder

object AvroUtils {

  private def testFieldNull(dec:BinaryDecoder,
                            schema:Schema,
                            skip:Boolean):Boolean = {
    schema.getType match {
      case org.apache.avro.Schema.Type.ARRAY =>
        if (!skip)
          false
        else {
          /* this is gross, but it's how you're supposed to do it.
           * see the avro docs on BinaryDecoder.skipArray */
          var i = dec.skipArray()
          while(i != 0) {
            for (j <- (1L to i))
              testFieldNull(dec,schema.getElementType(),true)
            i  = dec.skipArray()
          }
          false
        }
      case org.apache.avro.Schema.Type.BOOLEAN =>
        dec.readBoolean
      false
      case org.apache.avro.Schema.Type.BYTES =>
        if (!skip)
          false
        else {
          dec.skipBytes
          false
        }
      case org.apache.avro.Schema.Type.DOUBLE =>
        dec.readDouble
        false
      case org.apache.avro.Schema.Type.ENUM =>
        dec.readEnum
        false
      case org.apache.avro.Schema.Type.FIXED =>
        throw new Exception("Fixed not supported yet")
      case org.apache.avro.Schema.Type.FLOAT =>
        dec.readFloat
        false
      case org.apache.avro.Schema.Type.INT =>
        dec.readInt
        false
      case org.apache.avro.Schema.Type.LONG =>
        dec.readLong
        false
      case org.apache.avro.Schema.Type.MAP =>
        if (!skip)
          false
        else {
          dec.skipMap
          false
        }
      case org.apache.avro.Schema.Type.NULL =>
        dec.readNull
      true
      case org.apache.avro.Schema.Type.RECORD =>
        //loopFields(dec,schema.getFields,name)
        false
      case org.apache.avro.Schema.Type.STRING => 
        if (!skip)
          false
        else {
          dec.skipString
          false
        }
      case org.apache.avro.Schema.Type.UNION =>
        val i = dec.readIndex
        return testFieldNull(dec,schema.getTypes.get(i),skip)
      case other =>
        throw new Exception("Unsupported type: "+other)
    }
  }

  def testNull(bytes:Array[Byte],
               schema:Schema,
               name:String):Boolean = {
    val dec = new BinaryDecoder(new java.io.ByteArrayInputStream(bytes))
    val fields = schema.getFields
    for (i <- (0 to (fields.size() - 1))) {
      val field = fields.get(i)
      if (field.name.equals(name)) {
        return testFieldNull(dec,field.schema,false)
      }
      else
        testFieldNull(dec,field.schema,true)
    }
    true
  }



  private def skipOrReturn(dec:BinaryDecoder,
                           schema:Schema,
                           name:List[String],
                           skip:Boolean):AnyRef = {
    schema.getType match {
      case org.apache.avro.Schema.Type.ARRAY =>
        if (!skip)
          throw new Exception("Can't extract arrays yet")
        else {
          /* this is gross, but it's how you're supposed to do it.
           * see the avro docs on BinaryDecoder.skipArray */
          var i = dec.skipArray()
          while(i != 0) {
            for (j <- (1L to i))
              skipOrReturn(dec,schema.getElementType(),name,true)
            i  = dec.skipArray()
          }
          null
        }
      case org.apache.avro.Schema.Type.BOOLEAN =>
        boolean2Boolean(dec.readBoolean)
      case org.apache.avro.Schema.Type.BYTES =>
        if (!skip)
          dec.readBytes(null)
        else {
          dec.skipBytes
          null
        }
      case org.apache.avro.Schema.Type.DOUBLE =>
        double2Double(dec.readDouble)
      case org.apache.avro.Schema.Type.ENUM =>
        intWrapper(dec.readEnum)
      case org.apache.avro.Schema.Type.FIXED =>
        throw new Exception("Fixed not supported yet")
      case org.apache.avro.Schema.Type.FLOAT =>
        float2Float(dec.readFloat)
      case org.apache.avro.Schema.Type.INT =>
        intWrapper(dec.readInt)
      case org.apache.avro.Schema.Type.LONG =>
        long2Long(dec.readLong)
      case org.apache.avro.Schema.Type.MAP =>
        if (!skip)
          throw new Exception("Map not supported yet")
        else
          long2Long(dec.skipMap)
      case org.apache.avro.Schema.Type.NULL =>
        dec.readNull
      null
      case org.apache.avro.Schema.Type.RECORD =>
        if (skip)
          loopFields(dec,schema.getFields,null)
        else {
          if (name.size == 1)
            throw new Exception("Can't extract whole records")
          else
            loopFields(dec,schema.getFields,name.tail)
        }
      case org.apache.avro.Schema.Type.STRING => 
        if (!skip)
          dec.readString(null)
        else {
          dec.skipString
          null
        }
      case org.apache.avro.Schema.Type.UNION =>
        val i = dec.readIndex
        skipOrReturn(dec,schema.getTypes.get(i),name,skip)
      case other =>
        throw new Exception("Unsupported type: "+other)
    }
  }


  def loopFields(dec:BinaryDecoder, fields:java.util.List[Schema.Field],name:List[String]):AnyRef = {
    for (i <- (0 to (fields.size() - 1))) {
      val field = fields.get(i)
      if (name != null && field.name.equals(name.head)) 
        return skipOrReturn(dec,field.schema,name,false)
      else
        skipOrReturn(dec,field.schema,name,true)
    }
    null
  }

  def extractField(bytes:Array[Byte],
                   schema:Schema,
                   name:String):AnyRef = {
    val dec = new BinaryDecoder(new java.io.ByteArrayInputStream(bytes))
    loopFields(dec,schema.getFields,name.split('.').toList)
  }

}
