package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import java.text.ParsePosition
import edu.berkeley.cs.scads.model._

class FieldSpec[ValueType, FieldType <: ValueHoldingField[ValueType]](values: Seq[ValueType])(implicit manifest : scala.reflect.Manifest[FieldType]) extends SpecificationWithJUnit("Field Specification") {
	val const = manifest.erasure.getConstructors()(0)

	"A field" should {
		val field = const.newInstance().asInstanceOf[FieldType]
		val fields = values.map((v) => const.newInstance().asInstanceOf[FieldType].apply(v))

		"serialize and deserialize" in {
			fields.foreach((f) => {
				field.deserialize(f.serialize)
				field must_== f
			})
		}

		"serialize and deserialize as a key" in {
			fields.foreach((f) => {
				field.deserializeKey(f.serializeKey)
				field must_== f
			})
		}

		"sort correctly as a key" in {
			fields.toList.sort((e1, e2) => e1 < e2).map(_.value) must containInOrder(values)
		}

		"serialize and deserialize when concatinated" in {
			val pos = new ParsePosition(0)
			val serialized = fields.foldLeft("")((st: String, f: ValueHoldingField[ValueType]) => st + f.serialize)

			fields.foreach((f) => {
				field.deserialize(serialized, pos)
				f must_== field
			})
		}
	}
}

object StringFieldSpec extends FieldSpec[String, StringField](List("a", "b", "c", "aa", "bb", "cc", "test'escaping", "z", "0123", "").sort(_<_))
object IntegerFieldSpec extends FieldSpec[Int, IntegerField](List(Math.MAX_INT -1, Math.MIN_INT + 10, -1, 0, 1, -100, 100).sort(_<_))

class StringFieldTest extends JUnit4(StringFieldSpec)
class IntergerFieldTest extends JUnit4(IntegerFieldSpec)
