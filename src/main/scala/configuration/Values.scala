package deploylib.configuration

import java.io.File

/* Support to late binding of Values */
abstract trait Value[Type] {
	def getValue(rm: RemoteMachine): Type
	override def toString: String
}

class LateBoundValue[Type](description: String, func: RemoteMachine => Type) extends Value[Type] {
	def getValue(rm: RemoteMachine): Type = func(rm)
	override def toString: String = "<LB " + description + ">"
}

class LateStringBuilder(parts: Value[String]*) extends Value[String] {
	def getValue(rm: RemoteMachine): String = parts.map(_.getValue(rm)).mkString("", "", "")
	override def toString: String = parts.map(_.toString).mkString("", "", "")
}

class ConstantValue[Type](value: Type) extends Value[Type] {
	def getValue(rm: RemoteMachine): Type = value
	override def toString: String = value.toString
}

object ValueConverstion {
	implicit def toConstantValue[Type](value: Type): ConstantValue[Type] = new ConstantValue[Type](value)
  implicit def toFile(filename: String): File = new File(filename)
}
