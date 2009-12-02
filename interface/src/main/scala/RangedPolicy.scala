package edu.berkeley.cs.scads.thrift

import scala.collection.jcl.Conversions

import edu.berkeley.cs.scads.thrift._

import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

object RangedPolicy {
	def apply(policy: Array[Byte]): RangedPolicy = {
		val bytes = new ByteArrayInputStream(policy)
		val objectIn = new ObjectInputStream(bytes)

		new RangedPolicy(objectIn.readObject().asInstanceOf[Array[(String, String)]])
	}

	def convert(policy: java.util.List[RecordSet]): Seq[(String, String)] = {
		Conversions.convertList(policy).map(p => {
			(p.getRange.getStart_key, p.getRange.getEnd_key)
		})
	}

	def convert(policy: (String, String)): java.util.List[RecordSet] = convert(List(policy))
	def convert(policy: List[(String, String)]): java.util.List[RecordSet] = {
		val ret = new java.util.LinkedList[RecordSet]
		policy.foreach(p => {
			val recSet = new RecordSet
			val rangeSet = new RangeSet
			if(p._1 != null) rangeSet.setStart_key(p._1)
			if(p._2 != null) rangeSet.setEnd_key(p._2)
			recSet.setType(RecordSetType.RST_RANGE)
			recSet.setRange(rangeSet)
			ret.add(recSet)
		})
		ret
	}
}

class RangedPolicy(val policy: Array[(String, String)]) {
	def contains(key: String): Boolean = {
		policy.foreach(p => {
			if((p._1 == null || p._1.compare(key) <= 0) && (p._2 == null || p._2.compare(key) > 0))
				return true
		})
		return false
	}

	def getBytes(): Array[Byte] = {
		val bytes = new ByteArrayOutputStream()
		val objectOut = new ObjectOutputStream(bytes)

		objectOut.writeObject(policy)
		bytes.toByteArray()
	}
}
