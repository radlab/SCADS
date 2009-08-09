package optional

import java.{ lang => jl }
import jl.reflect.{ Array => _, _ }

trait ArgInfo {
  def name: String
  def tpe: Type
  def isOptional: Boolean
  def fromString(value: String): AnyRef
  
  private def usageFormat = if (isOptional) "[--%s %s]" else "<%s: %s>"
  def usage: String = usageFormat.format(name, tpe)
}

abstract class TpeInfo[T <: AnyRef](tpe: Type) {
  def fromString(value: String): T
  def str: String
  
  override def toString() = str
}
class StringInfo extends TpeInfo[String](classOf[String]) {
  def fromString(value: String) = value
  def str = "String"
}

// trait SomeType[T <: AnyRef] {
//   def isOptional: Boolean  
//   def tpe: Type
//   def fromString(value: String): T
//   // override def toString: String
// }
// trait StringType extends SomeType[String] {
//   val tpe = classOf[String]
//   def fromString(value: String) = value
//   override def toString = "String"
// }
//   
