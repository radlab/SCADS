abstract class Key extends Ordered[Key] {
	override def compare(other: Key) = {
		other match {
			case MaxKey => -1
			case MinKey => 1
			case k:Key => serialize.compare(k.serialize)
		}
	}
	
	def toString : String 
	def serialize: String
}

@serializable
case class StringKey(stringVal: String) extends Key {
	override def toString:String = stringVal
	override def serialize:String = stringVal
}

object IntKey {
	val keyFormat = new java.text.DecimalFormat("0000000000000000")
}

@serializable
case class IntKey(intVal: Int) extends Key {
	override def toString:String = IntKey.keyFormat.format(intVal)
	def serialize:String = IntKey.keyFormat.format(intVal)
}

class InvalidKey extends Exception

@serializable
object MinKey extends Key {
	override def compare(other: Key) = -1
	override def toString : String  = "MinKey"
	def serialize: String = throw new InvalidKey
}

@serializable
object MaxKey extends Key {
	override def compare(other: Key) = 1
	override def toString : String  = "MaxKey"
	def serialize: String = throw new InvalidKey
}

object AutoKey {
	implicit def stringToKey(s:String) = new StringKey(s)
	implicit def intToKey(i:Int) = new IntKey(i)
}