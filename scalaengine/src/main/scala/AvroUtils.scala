package edu.berkeley.cs.scads.storage

import org.apache.avro.Schema
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.specific.SpecificData
import org.apache.avro.generic.GenericData.{Array => AvroArray}

object AvroUtils {

  private val specData:SpecificData = SpecificData.get

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
        dec.skipFixed(schema.getFixedSize)
        false
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
        if (!skip) {
          val size = schema.getFixedSize
          val ret = new Array[Byte](size)
          dec.readFixed(ret,0,size)
          ret
        }
        else { 
          dec.skipFixed(schema.getFixedSize)
          null
        }
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




  private def fillField(dec:BinaryDecoder,
                        schema:Schema,
                        idx:Int,
                        base:SpecificRecordBase):Unit = {
    schema.getType match {
      case org.apache.avro.Schema.Type.ARRAY =>
        var array = new AvroArray[AnyRef](1024,schema)
        var i = dec.readArrayStart()
        val n = List[String]()
        while(i != 0) {
          for (j <- (1L to i)) 
            array.add(skipOrReturn(dec,schema.getElementType(),n,false))
          i = dec.arrayNext()
        }
        base.put(idx,array)
      case org.apache.avro.Schema.Type.BOOLEAN =>
        base.put(idx,dec.readBoolean)
      case org.apache.avro.Schema.Type.BYTES =>
        base.put(idx,dec.readBytes(null))
      case org.apache.avro.Schema.Type.DOUBLE =>
        base.put(idx,dec.readDouble)
      case org.apache.avro.Schema.Type.ENUM =>
        base.put(idx,dec.readEnum)
      case org.apache.avro.Schema.Type.FIXED =>
        val size = schema.getFixedSize
        val ret = new Array[Byte](size)
        dec.readFixed(ret,0,size)
        base.put(idx,ret)
      case org.apache.avro.Schema.Type.FLOAT =>
        base.put(idx,dec.readFloat)
      case org.apache.avro.Schema.Type.INT =>
        base.put(idx,dec.readInt)
      case org.apache.avro.Schema.Type.LONG =>
        base.put(idx,dec.readLong)
      case org.apache.avro.Schema.Type.MAP =>
        throw new Exception("Map not supported yet")
      case org.apache.avro.Schema.Type.NULL =>
        dec.readNull
        base.put(idx,null)
      case org.apache.avro.Schema.Type.RECORD =>
        val rc = specData.getClass(schema)
        val rec =
          if (base.get(idx) == null)
            rc.newInstance.asInstanceOf[SpecificRecordBase]
          else
            base.get(idx).asInstanceOf[SpecificRecordBase]
        loopFieldsFill(dec,schema.getFields,rec)
        base.put(idx,rec)
      case org.apache.avro.Schema.Type.STRING => 
        base.put(idx,dec.readString(null))
      case org.apache.avro.Schema.Type.UNION =>
        val i = dec.readIndex
        fillField(dec,schema.getTypes.get(i),idx,base)
      case other =>
        throw new Exception("Unsupported type: "+other)
    }
  }


  def loopFieldsFill(dec:BinaryDecoder, fields:java.util.List[Schema.Field], base:SpecificRecordBase):AnyRef = {
    for (i <- (0 to (fields.size() - 1))) {
      val field = fields.get(i)
      fillField(dec,field.schema,i,base)
    }
    null
  }

  def fillRecord(bytes:Array[Byte],
                 base:SpecificRecordBase):Unit = {
    val dec = new BinaryDecoder(new java.io.ByteArrayInputStream(bytes))
    loopFieldsFill(dec,base.getSchema.getFields,base)
  }


}
