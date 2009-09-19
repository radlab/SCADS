package edu.berkeley.cs.scads.model

import java.text.ParsePosition
import java.util.regex.Pattern
import org.apache.log4j.Logger

case class DeserializationException(data: String, pos: ParsePosition) extends Exception

/**
 * The base abstract class for defining types of datat that can be stored in SCADS as either attributes or keys.
 */
abstract class Field extends Ordered[Field] {
	val logger = Logger.getLogger("scads.field")
	override def compare(other: Field) = serializeKey.compare(other.serializeKey)
	override def equals(other: Any) = {
		other match {
			case f:Field => (serializeKey equals f.serializeKey)
			case _ => false
		}
	}

	/**
	 * Return a string that is the serialized representation of the data stored such that resulting strings will sort correctly when compared byte-wise
	 * Ex. 2 and 10 should be "02" and "10" not "10" and "2"
	 */
	def serializeKey(): String

	/**
	 * Take a string created by <code>serializeKey</code> and retrieve the value.
	 * @param pos a parse position object that specifies where to start deserializeing and will represent what data remains after one field has been serialized after execution.
	 */
	def deserializeKey(data: String, pos: ParsePosition): Unit

	/**
	 * Helper function that assumes deserialization begins at the beginning of the string.
	 */
	def deserializeKey(data: String): Unit = deserializeKey(data, new ParsePosition(0))

	/**
	 * Return a string that is the compact serialized representation of the data stored.
	 */
	def serialize(): String

	/**
	 * Take a string created by <code>serializeKey</code> and retrieve the value.
	 * @param pos a parse position object that specifies where to start deserializeing and will represent what data remains after one field has been serialized after execution.
	 */
	def deserialize(data: String, pos: ParsePosition): Unit

	/**
	 * Helper function that assumes deserialization begins at the begining of the string.
	 */
	def deserialize(data: String): Unit = deserialize(data, new ParsePosition(0))

	/**
	 * Return a duplicate of the current field.
	 */
	def duplicate(): Field
}

/*
 * Helper class that has generic functions for getting and setting fields with simple types
 */
abstract class ValueHoldingField[Type] extends Field {
	/**
	 * The current value stored in the field
	 */
	var value: Type

	/**
	 * Change the value currently stored in the field
	 */
	def apply(newValue: Type) = {value = newValue; this}

	/**
	 * Returns the value currently stored in the field
	 */
	def is:Type = value
	override def toString() = this.getClass().getSimpleName() + "<" + value + ">"
}

/**
 * Trait that can be mixed in for field types that use the same serialization for both key and nonkey methods.
 */
abstract trait SerializeAsKey {
	def serializeKey(): String
	def deserializeKey(data: String, pos: ParsePosition): Unit

	def serialize(): String = serializeKey()
	def deserialize(data: String, pos: ParsePosition): Unit = deserializeKey(data, pos)
}

/**
 * A Field Type that can be used to store strings.
 * They are serialized as 'quoted' and \escaped.
 * TODO: The current implementation doesn't collate correctly when <code>''</code> and <code>'\0'</code> are stored.
 */
class StringField extends ValueHoldingField[String] with SerializeAsKey {
	var value:String = ""

	def serializeKey(): String = "'" + value.replace("'", "\\'") + "'"
	def deserializeKey(data: String, pos: ParsePosition): Unit = {
		val builder = new StringBuilder()
		assert(data.charAt(pos.getIndex) equals '\'')
		pos.setIndex(pos.getIndex + 1)

		while(data.charAt(pos.getIndex) != '\'' || data.charAt(pos.getIndex - 1) == '\\') {
			builder.append(data.charAt(pos.getIndex))
			pos.setIndex(pos.getIndex + 1)
		}
		pos.setIndex(pos.getIndex + 1)
		value = builder.toString().replace("\\'", "'")
	}
	def duplicate() = (new StringField)(value)
}

/**
 * Helper methods for integer fields
 */
object IntegerField {
	/**
	 * Create and populate an integer field.
	 */
	def apply(v: Int) = (new IntegerField)(v)
}

/**
 * Field type for storing integers.
 * They are serialized as zero padded 10 character length decimal numbers.
 * Negative numbers are stored subtracted from maxint so that they sort backwards.
 */
class IntegerField extends ValueHoldingField[Int] with SerializeAsKey {
	val keyFormat = new java.text.DecimalFormat("0000000000\0")
	val maxKey = 2147483647
	var value = 0

	def serializeKey(): String =
	if(value >= 0)
		keyFormat.format(value)
	else
		keyFormat.format((maxKey - Math.abs(value)) * -1)

	def deserializeKey(data: String, pos: ParsePosition): Unit = {
		logger.debug("Deserializeing " + data)
		val num = keyFormat.parse(data, pos).intValue()
		if(num < 0)
			value = (maxKey - Math.abs(num)) * -1
		else
			value = (num)
	}

	def duplicate() = (new IntegerField)(value)
}

/**
 * A class for creating a key that is a composite of two other field types.
 * TODO: Handle 3,4,5 etc length keys, either with more classes or something more elegant.
 */
class CompositeKey[T1 <: Field, T2 <: Field](k1: T1, k2: T2) extends Field {
	def serializeKey(): String = null
	def deserializeKey(data: String, pos: ParsePosition): Unit = null

	def serialize(): String = null
	def deserialize(data: String, pos: ParsePosition): Unit = null

	def duplicate() = null
}
