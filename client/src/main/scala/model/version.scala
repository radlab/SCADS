package edu.berkeley.cs.scads.model

import java.text.ParsePosition

/**
* Abstract definition of version for tuples stored in scads.
*/
abstract class Version {
	/**
	 * Duplicate and increment from the current version
	 */
	def next(): Version = {
		val n:Version = duplicate
		n.increment
		n
	}

	/**
	 * Compare two versions
	 * @return true if this version supersedes the other version
	 */
	def >(other: Version):Boolean

	/**
	 * Advance this object to the next version.
	 * Called anytime data is modified.
	 */
	def increment(): Unit

	/**
	 * Return a duplicate version object
	 */
	def duplicate(): Version

	/**
	 * Serialize the current version to a string for storage
	 */
	def serialize: String

	/**
	 * The length in characters that should be used to do a prefix comparison to make sure that test_and_set if overwriting the expected version
	 */
	def prefixLength: Int

	/**
	 * Deserialize a version stored in a string
	 */
	def deserialize(data: String, pos: ParsePosition): Unit

	/**
	 * Helper method that assumes deserialization should begin at index 0 of the string.
	 */
	def deserialize(data: String): Unit = deserialize(data, new ParsePosition(0))
}

/**
 * Version type when no versioning is desired.
 */
object Unversioned extends Version {
	def increment(): Unit = true
	def duplicate(): Version = this
	def serialize: String = null
	def prefixLength: Int = 0
	def deserialize(data: String, pos: ParsePosition): Unit = true
	def >(other: Version):Boolean = false
}

/**
 * Trivial versioning based on data generation number.
 */
class IntegerVersion extends Version {
	val ver = IntegerField(0)

	def increment() = ver(ver.value + 1)

	def >(other: Version):Boolean = {
		other match {
			case i: IntegerVersion => ver.value > i.ver.value
		}
	}

	def duplicate(): Version = {
		val ret = new IntegerVersion
		ret.ver(ver.value)
		return ret
	}

	def serialize: String =
		if(ver.value == 0)
			null
		else
			ver.serializeKey

	def prefixLength = {
		if(ver.value == 0)
			0
		else
			11
	}


	def deserialize(data: String, pos: ParsePosition): Unit = ver.deserialize(data, pos)
}
