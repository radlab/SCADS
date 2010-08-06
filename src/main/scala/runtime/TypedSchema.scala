package org.apache.avro

import java.util.{List => JList}
import org.codehaus.jackson.JsonGenerator

/**
 * Delegating Schema which carries a type parameter
 */
class TypedSchema[T](impl: Schema) extends Schema(impl.getType) {

  import Schema._

  override def getProp(name: String) = 
    impl.getProp(name)

  override def addProp(name: String, value: String) = 
    impl.addProp(name, value)

  override def getType = 
    impl.getType

  override def getField(fieldname: String) = 
    impl.getField(fieldname)

  override def getFields = 
    impl.getFields

  override def setFields(fields: JList[Field]) =
    impl.setFields(fields)

  override def getEnumSymbols =
    impl.getEnumSymbols

  override def getEnumOrdinal(symbol: String) =
    impl.getEnumOrdinal(symbol)

  override def hasEnumSymbol(symbol: String) = 
    impl.hasEnumSymbol(symbol)

  override def getName =
    impl.getName

  override def getDoc = 
    impl.getDoc

  override def getNamespace = 
    impl.getNamespace

  override def getFullName =
    impl.getFullName

  override def isError = 
    impl.isError

  override def getElementType =
    impl.getElementType

  override def getValueType =
    impl.getValueType

  override def getTypes =
    impl.getTypes

  override def getFixedSize = 
    impl.getFixedSize

  override def toString = 
    impl.toString

  override def toString(pretty: Boolean) = 
    impl.toString(pretty)

  override def toJson(names: Names, gen: JsonGenerator) =
    impl.toJson(names, gen)

  override def writeProps(gen: JsonGenerator) = 
    impl.writeProps(gen)

  override def fieldsToJson(names: Names, gen: JsonGenerator) = 
    impl.fieldsToJson(names, gen)

  override def equals(o: Any) = 
    impl.equals(o)

  override def hashCode = 
    impl.hashCode
}
