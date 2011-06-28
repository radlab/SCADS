package edu.berkeley.cs.scads.axer

/* Class that builds an AxerSchema from an Avro schema.  We
 * need a new type of schema that stores some information
 * about field offsets and so on */


import scala.collection.mutable.ArrayBuilder
import org.apache.avro.{Schema => AvroSchema} // Re-name is for less confusion in code
import scala.collection.JavaConversions._

//RECORD, ENUM, ARRAY, MAP, UNION, 
//FIXED, STRING, BYTES, INT, LONG,
//FLOAT, DOUBLE, BOOLEAN, NULL

sealed abstract class AxerType
sealed abstract class ConstType extends AxerType
sealed abstract class VarType extends AxerType

// variable length types
case class ARRAY(elementType:AxerType) extends VarType
case object STRING extends VarType
case object BYTES extends VarType

// constant length types
case object BOOLEAN extends ConstType
case object INT extends ConstType
case object FLOAT extends ConstType
case object LONG extends ConstType
case object DOUBLE extends ConstType
case object FIXED extends ConstType

// null doesn't extend ConstType 
// so addConst match is exhaustive
case object NULL extends AxerType

private class SchemaBuilder(name:String) {
  private val constNames = new ArrayBuilder.ofRef[String]
  private val optNames = new ArrayBuilder.ofRef[String]
  private var numOpt = 0
  private val optMap = new AxerIntTable(443)
  private val constOffsetMap = new AxerIntTable(443)
  private val typeMap = new AxerRefTable[AxerType](701)
  private var constOffset = 4

  private val varNames = new ArrayBuilder.ofRef[String]
  private var numVars = 0
  private val varNumMap = new AxerIntTable(443)

  private def addConst(name:String, t:ConstType, fs:Int=0) {
    val size = AxerSchema.numBytesForType(t,fs)
    constNames += name
    constOffsetMap.put(name,constOffset)
    typeMap.put(name,t)
    constOffset += size
  }

  private def addOptionalConst(name:String, t:ConstType) {
    optMap.put(name, numOpt)
    optNames += name
    numOpt += 1
  }

  private def addVar(name:String, t:VarType) {
    varNames += name
    varNumMap.put(name,numVars)
    typeMap.put(name,t)
    numVars += 1
  }

  def result():AxerSchema = {
    new AxerSchema(name,
                   constNames.result,
                   optNames.result,
                   varNames.result,
                   constOffsetMap,
                   optMap,
                   typeMap,
                   varNumMap,
                   constOffset)
  }

  def addBoolean(name:String,opt:Boolean=false) = if (opt) addOptionalConst(name,BOOLEAN) else addConst(name,BOOLEAN)
  def addInt    (name:String,opt:Boolean=false) = if (opt) addOptionalConst(name,INT)     else addConst(name,INT)
  def addFloat  (name:String,opt:Boolean=false) = if (opt) addOptionalConst(name,FLOAT)   else addConst(name,FLOAT)
  def addLong   (name:String,opt:Boolean=false) = if (opt) addOptionalConst(name,LONG)    else addConst(name,LONG)
  def addDouble (name:String,opt:Boolean=false) = if (opt) addOptionalConst(name,DOUBLE)  else addConst(name,DOUBLE)

  def addFixed  (name:String,fs:Int) = addConst(name,FIXED,fs)

  def addBytes  (name:String) = addVar(name,BYTES)
  def addString (name:String) = addVar(name,STRING)
  def addArray  (name:String,elementType:AxerType) = addVar(name,new ARRAY(elementType))

  def addNull(name:String) = typeMap.put(name,NULL)

}

case class AxerSchema(name:String,
                      constFields:Array[String],
                      optNames:Array[String],
                      varFields:Array[String],
                      private val constOffsetMap:AxerIntTable,
                      private val optMap:AxerIntTable,
                      private val typeMap:AxerRefTable[AxerType],
                      private val varNumMap:AxerIntTable,
                      constOffset:Int) {

  def getType(name:String):AxerType = {
    typeMap.get(name)
  }

  def getOffset(name:String):Int = {
    constOffsetMap.get(name)
  }

  def getVarPosition(name:String):Int = {
    varNumMap.get(name)
  }

}

object AxerSchema {

  def numBytesForType(t:ConstType, fs:Int = 0):Int = {
    t match {
      case BOOLEAN => 1
      case INT     => 4
      case FLOAT   => 4
      case LONG    => 8
      case DOUBLE  => 8
      case FIXED   => fs
    }
  }

  private def avroToAxerType(as:AvroSchema):AxerType = {
    as.getType match {
      case AvroSchema.Type.BOOLEAN => BOOLEAN
      case AvroSchema.Type.INT => INT
      case AvroSchema.Type.FLOAT => FLOAT
      case AvroSchema.Type.LONG => LONG
      case AvroSchema.Type.DOUBLE => DOUBLE
      
      case AvroSchema.Type.FIXED => FIXED
      case AvroSchema.Type.NULL => NULL
      
      case AvroSchema.Type.STRING => STRING
      case AvroSchema.Type.BYTES => BYTES
      case AvroSchema.Type.ARRAY => ARRAY(avroToAxerType(as.getElementType))
      case AvroSchema.Type.RECORD => throw new Exception("Hrmm, what to do here")
    }
  }

  private def addToSchema(as:AvroSchema, builder:SchemaBuilder, prefix:String) {
    as.getFields.foreach(field => {
      val name = prefix+field.name
      field.schema.getType match {
        case AvroSchema.Type.BOOLEAN => builder.addBoolean(name)
        case AvroSchema.Type.INT => builder.addInt(name)
        case AvroSchema.Type.FLOAT => builder.addFloat(name)
        case AvroSchema.Type.LONG => builder.addLong(name)
        case AvroSchema.Type.DOUBLE => builder.addDouble(name)

        case AvroSchema.Type.FIXED => builder.addFixed(name,field.schema.getFixedSize)
        case AvroSchema.Type.NULL => builder.addNull(name)

        case AvroSchema.Type.STRING => builder.addString(name)
        case AvroSchema.Type.BYTES => builder.addBytes(name)
        case AvroSchema.Type.ARRAY => builder.addArray(name,avroToAxerType(field.schema.getElementType()))
        case AvroSchema.Type.RECORD => addToSchema(field.schema,builder,name+".")
      }
    })
  }

  def getAxerSchema(s:AvroSchema):AxerSchema = {
    val builder = new SchemaBuilder(s.getName)
    addToSchema(s,builder,"")
    builder.result
  }
}
