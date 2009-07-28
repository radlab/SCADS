package edu.berkeley.cs.scads.test

import org.scalatest.Suite
import edu.berkeley.cs.scads.keys._

class KeyTest extends Suite {
	def testMinMaxSelfCompare() = {
		// equality should work
		assert( (MaxKey <= MaxKey) && (MaxKey >= MaxKey) )
		assert( (MinKey <= MinKey) && (MinKey >= MinKey) )
		assert( (MaxKey == MaxKey) && (MinKey == MinKey) )
		
		// inequality should work
		assert( !(MinKey < MinKey) && !(MinKey > MinKey) )
		assert( !(MaxKey < MaxKey) && !(MaxKey > MaxKey) )
	}
	def testKeySerialization() = {
		val sk1 = new StringKey("Hello World")
		val sk2 = new StringKey("1231234123412341234")
		val sk3 = new StringKey("")

		Array(sk1, sk2, sk3).foreach(k => {
			assert(k == StringKey.deserialize(k.serialize, new java.text.ParsePosition(0)))
		})

		val nk1 = new NumericKey[Int](0)
		val nk2 = new NumericKey[Int](1)
		val nk3 = new NumericKey[Int](-1)
		val nk4 = new NumericKey[Int](100)
		val nk5 = new NumericKey[Long](System.currentTimeMillis())

		Array(nk1, nk2, nk3, nk4, nk5).foreach(k => {
			val k2 = NumericKey.deserialize(k.serialize, new java.text.ParsePosition(0))
			assert(k == k2, "Error" + k + "!=" + k2 )
		})

		val nks = Array(System.currentTimeMillis() * -1L, -10L, -5L, -1L, -0L, 0L, 1L, 5L, 10L, System.currentTimeMillis()).map((n) => {(new NumericKey[Long](n)).serialize}).toList

		for(i <- (0 to nks.length - 2)) {
			val n1 = nks(i)
			val n2 = nks(i+1)
			assert((n1 compareTo n2) <= 0, "Error: " + n1 + " compare " + n2 + "=" + (n1 compareTo n2) + "DS: " + NumericKey.deserialize(n1, new java.text.ParsePosition(0)) + NumericKey.deserialize(n2, new java.text.ParsePosition(0)))
		}

		val t = new NumericKey[Long](System.currentTimeMillis() * -1L)
		val u = new StringKey("UserName")
		val serkey = u.serialize+t.serialize
		val pos = new java.text.ParsePosition(0)
		val u2 = StringKey.deserialize(serkey, pos)
		val t2 = NumericKey.deserialize(serkey, pos)

		assert(t == t2)
		assert(u == u2)
	}
}

class KeyRangeTest extends Suite with AutoKey {
	def testAddition() {
		assert(KeyRange("a","c") + KeyRange("b", "d") == KeyRange("a", "d"))
		assert(KeyRange("b", "d") + KeyRange("a","c") == KeyRange("a", "d"))
		assert(KeyRange(MinKey,"c") + KeyRange("b", "d") == KeyRange(MinKey, "d"))
		assert(KeyRange("a",MaxKey) + KeyRange("b", "d") == KeyRange("a", MaxKey))
		assert(KeyRange("a","c") + KeyRange(MinKey, "d") == KeyRange(MinKey, "d"))
		assert(KeyRange("a","c") + KeyRange("b", MaxKey) == KeyRange("a", MaxKey))

		assert(KeyRange("a","b") + KeyRange("b", "c") == KeyRange("a", "c"))
		assert(KeyRange("b", "c") + KeyRange("a","b") == KeyRange("a", "c"))

		assert(KeyRange("a", "z") + KeyRange("m","n") == KeyRange("a", "z"))
		assert(KeyRange("m", "n") + KeyRange("a","z") == KeyRange("a", "z"))

		assert(KeyRange.EmptyRange + KeyRange("a", "z") ==  KeyRange("a", "z"))
		assert(KeyRange("a", "z") + KeyRange.EmptyRange ==  KeyRange("a", "z"))

		intercept[NotContiguousException] {
			KeyRange("a","b") + KeyRange("c", "d")
		}

		intercept[NotContiguousException] {
			KeyRange("c","d") + KeyRange("a", "b")
		}
	}

	def testSubtraction() {
		assert(KeyRange("a", "c") - KeyRange("b", "c") == KeyRange("a", "b"))
		assert(KeyRange("a", "c") - KeyRange("b", "d") == KeyRange("a", "b"))

		assert(KeyRange("b", "d") - KeyRange("a", "c") == KeyRange("c", "d"))
		assert(KeyRange("b", "d") - KeyRange("b", "c") == KeyRange("c", "d"))

		assert(KeyRange("a", "b") - KeyRange("a", "b") == KeyRange.EmptyRange)

		assert(KeyRange.EmptyRange - KeyRange("a", "z") ==  KeyRange.EmptyRange)
		assert(KeyRange("a", "z") - KeyRange.EmptyRange ==  KeyRange("a", "z"))
	}

	def testAnd() {
		assert((KeyRange("a","c") & KeyRange("b", "d")) == KeyRange("b", "c"))
		assert((KeyRange("b", "d") & KeyRange("a","c")) == KeyRange("b", "c"))
		assert((KeyRange(MinKey,"c") & KeyRange("b", "d")) == KeyRange("b", "c"))
		assert((KeyRange("a",MaxKey) & KeyRange("b", "d")) == KeyRange("b", "d"))
		assert((KeyRange("a","c") & KeyRange(MinKey, "d")) == KeyRange("a", "c"))
		assert((KeyRange("a","c") & KeyRange("b", MaxKey)) == KeyRange("b", "c"))
		assert((KeyRange("a","c") & KeyRange.EmptyRange) == KeyRange.EmptyRange)
		assert((KeyRange.EmptyRange & KeyRange("a","c")) == KeyRange.EmptyRange)

		assert((KeyRange("m",MaxKey) & KeyRange("friend-8a43af10-180a-012c-331d-001b6391e19a-of-", "friend-8a43af10-180a-012c-331d-001b6391e19a-of/")) == KeyRange.EmptyRange)
	}
	def testNonCovered() = {
		assert( !KeyRange.isCovered(KeyRange("a","da"), Set(KeyRange("a","c"))) )
		assert( !KeyRange.isCovered(KeyRange("a","da"), Set(KeyRange("b","d"),KeyRange("a","b"))) )
		assert( !KeyRange.isCovered(KeyRange("aa","e"), Set(KeyRange("ab","b"),KeyRange("d","e"))) )
		
		assert( KeyRange.isCovered(KeyRange("a","c"), Set(KeyRange("a","b"),KeyRange("a","c"))) )
		assert( KeyRange.isCovered(KeyRange("a","c"), Set(KeyRange("b","c"),KeyRange("a","b"))) )
		assert( KeyRange.isCovered(KeyRange("a","d"), Set(KeyRange("a","b"),KeyRange("b","c"),KeyRange("c","d"))) )
		assert( KeyRange.isCovered(KeyRange("a","ef"), Set(KeyRange("a","b"),KeyRange("b","c"),KeyRange("c","f"))) )

		assert( KeyRange.isCovered(KeyRange("a","f"), Set(KeyRange("a","c"),KeyRange("d","f"),KeyRange("a","d"))) )
	}
}