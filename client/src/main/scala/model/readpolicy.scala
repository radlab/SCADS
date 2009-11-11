package edu.berkeley.cs.scads.model

import java.text.ParsePosition
import edu.berkeley.cs.scads.thrift.Record
import edu.berkeley.cs.scads.thrift.RecordSet
import edu.berkeley.cs.scads.thrift.RangeSet
import edu.berkeley.cs.scads.keys.TransparentKey
import org.apache.log4j.Logger


case class Tuple(key: Field, version: Version, value: String)

abstract class ReadPolicy {
	def get(namespace: String, key: String, keyClass: List[Class[Field]], versioned: Boolean)(implicit env: Environment): Tuple
	def get_set(namespace: String, startKey: String, endKey: String, limit: Int, keyClass: List[Class[Field]], versioned: Boolean)(implicit env: Environment): List[Tuple]

	protected def makeTuple(rec: Record, keyClass: List[Class[Field]], versioned: Boolean): Tuple = {
		val key = CompositeField(keyClass)
		val version = if(versioned) new IntegerVersion else Unversioned
		val pos = new ParsePosition(0)
		key.deserializeKey(rec.key)
		version.deserialize(rec.value, pos)
		val value = rec.value.substring(pos.getIndex)
		Tuple(key, version, value)
	}
}

object ReadRandomPolicy extends ReadPolicy {
	val logger = Logger.getLogger("scads.readpolicy.readrandom")
	val rand = new scala.util.Random()

	def get(namespace: String, key: String, keyClass: List[Class[Field]], versioned: Boolean)(implicit env: Environment): Tuple = {
		val nodes = env.placement.lookup(namespace, new TransparentKey(key))
		val node = nodes(rand.nextInt(nodes.length))

		val rec = node.useConnection((c) => {
			c.get(namespace, key)
		})
		makeTuple(rec, keyClass, versioned)
	}

	def get_set(namespace: String, startKey: String, endKey: String, limit: Int, keyClass: List[Class[Field]], versioned: Boolean)(implicit env: Environment): List[Tuple] = {
		val node = env.placement.lookup(namespace, new TransparentKey(startKey))

		val result = node(0).useConnection((c) => {
			val range = new RangeSet(startKey, endKey, 0, limit)
			val rset = new RecordSet()

			rset.setType(edu.berkeley.cs.scads.thrift.RecordSetType.RST_RANGE)
			rset.setRange(range)
			c.get_set(namespace, rset)
		})

		logger.debug("result: " + result)
		val tuples = new scala.collection.mutable.ListBuffer[Tuple]()
		var i = 0
		while(i < result.size) {
			logger.debug("Making a tuple from " + result.get(i) + " key: " + keyClass + " ver: " + versioned)
			tuples.append(makeTuple(result.get(i), keyClass, versioned))
			i += 1
		}
		tuples.toList
	}
}
