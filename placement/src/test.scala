import org.scalatest.Suite

class KeySpaceSuite extends Suite {
	def testKeySpace() = {
		val n1 = Node("test1", 0)
		val n2 = Node("test2", 0)
		val n3 = Node("test3", 0)
		val n4 = Node("test4", 0)

		val ks = new SimpleKeySpace()

		ks.assign(n1, KeyRange("a", "c"))
		ks.assign(n2, KeyRange("b", "m"))
		ks.assign(n3, KeyRange("m", "n"))
		ks.assign(n4, KeyRange("n", "z"))

		assert(ks.lookup("a") contains n1)
		assert(ks.lookup("b") contains n1)
		assert(ks.lookup("b") contains n2)
	}
}

class KeyRangeSuite extends Suite {
	def testAddition() {
		assert(KeyRange("a","c") + KeyRange("b", "d") == KeyRange("a", "d"))
		assert(KeyRange("b", "d") + KeyRange("a","c") == KeyRange("a", "d"))
		assert(KeyRange(null,"c") + KeyRange("b", "d") == KeyRange(null, "d"))
		assert(KeyRange("a",null) + KeyRange("b", "d") == KeyRange("a", null))
		assert(KeyRange("a","c") + KeyRange(null, "d") == KeyRange(null, "d"))
		assert(KeyRange("a","c") + KeyRange("b", null) == KeyRange("a", null))

		assert(KeyRange("a","b") + KeyRange("b", "c") == KeyRange("a", "c"))
		assert(KeyRange("b", "c") + KeyRange("a","b") == KeyRange("a", "c"))

		assert(KeyRange("a", "z") + KeyRange("m","n") == KeyRange("a", "z"))
		assert(KeyRange("m", "n") + KeyRange("a","z") == KeyRange("a", "z"))

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
	}

	def testAnd() {
		assert((KeyRange("a","c") & KeyRange("b", "d")) == KeyRange("b", "c"))
		assert((KeyRange("b", "d") & KeyRange("a","c")) == KeyRange("b", "c"))
		assert((KeyRange(null,"c") & KeyRange("b", "d")) == KeyRange("b", "c"))
		assert((KeyRange("a",null) & KeyRange("b", "d")) == KeyRange("b", "d"))
		assert((KeyRange("a","c") & KeyRange(null, "d")) == KeyRange("a", "c"))
		assert((KeyRange("a","c") & KeyRange("b", null)) == KeyRange("b", "c"))
	}
}

object RunTests {
	def main(args: Array[String]) = {
		(new KeyRangeSuite).execute()
		(new KeySpaceSuite).execute()
	}
}