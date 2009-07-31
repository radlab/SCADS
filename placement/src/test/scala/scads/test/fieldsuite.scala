package edu.berkeley.cs.scads.test

import org.scalatest.Suite
import java.text.ParsePosition
import edu.berkeley.cs.scads.model._

abstract class FieldTest[ValueType, FieldType <: ValueHoldingField[ValueType]] extends Suite {
	val values: Seq[ValueType]
	def build(): FieldType
	val f2 = build()

	def fields: Seq[FieldType] = values.map((v) => {
		val f = build()
		f.apply(v)
		f
	})

	def testSerialization = {
		fields.foreach((f) => {
			expect(f){ f2.deserialize(f.serialize); f2 }
		})
	}

	def testKeySerialization = {
		fields.foreach((f) => {
			expect(f) { f2.deserializeKey(f.serializeKey); f2 }
		})
	}

	def testSorting = {
		val linedUp  = values.toList.zip(fields.toList.sort((e1, e2) => e1 < e2))

		linedUp.foreach((p) =>
			assert(p._1 === p._2.value)
		)
	}

	def testConcatSerialization = {
		val pos = new ParsePosition(0)
		val serialized = fields.foldLeft("")((st: String, f: FieldType) => st + f.serialize)

		fields.foreach((f) => {
			f2.deserialize(serialized, pos)
			assert(f === f2)
		})
	}
}

class StringFieldTest extends FieldTest[String, StringField] {
	val values = Array("a", "b", "c", "aa", "bb", "cc", "test'escaping", "z", "0123", "")
	def build() = new StringField()

	scala.util.Sorting.quickSort(values)
}

class IntegerFieldTest extends FieldTest[Int, IntegerField] {
	val values = Array(Math.MAX_INT -1, Math.MIN_INT + 10, -1, 0, 1, -100, 100)
	def build() = new IntegerField

	scala.util.Sorting.quickSort(values)
}
