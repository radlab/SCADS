package edu.berkeley.cs.scads

import org.scalatest.Suite
import AutoKey._

class ClientLibrarySuite extends Suite with ThriftConversions {

	val rec1 = new SCADS.Record("a","a-val".getBytes())
	val rec2 = new SCADS.Record("b","b-val".getBytes())
	val rec3 = new SCADS.Record("c","c-val".getBytes())
	val rec4 = new SCADS.Record("d","d-val".getBytes())
	val rec5 = new SCADS.Record("e","e-val".getBytes())

	def testSingleNode() = {
		val clientlib = new LocalROWAClientLibrary
		val n1 = new TestableStorageNode()

		val ks = new SimpleKeySpace()
		ks.assign(n1, KeyRange("a", "ca"))
		clientlib.add_namespace("db_single",ks)

		// put two records
		assert( clientlib.put("db_single",rec1) )
		assert( clientlib.put("db_single",rec2) )

		// do a single get in range, on boundaries, outside responsibility
		assert( clientlib.get("db_single","a") == (new SCADS.Record("a","a-val".getBytes())) )
		assert( clientlib.get("db_single","b") == (new SCADS.Record("b","b-val".getBytes())) )
		assert( clientlib.get("db_single","c") == (new SCADS.Record("c",null)) )
		intercept[NoNodeResponsibleException] {
			clientlib.get("db_single","d")
		}

		// get a range of records, within range and outside range
		var results = clientlib.get_set("db_single", this.keyRangeToScadsRangeSet(KeyRange("a","bb")) )
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		results = clientlib.get_set("db_single", this.keyRangeToScadsRangeSet(KeyRange("a","c")) )
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))

		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_single", this.keyRangeToScadsRangeSet(KeyRange("1","b")) )
		}
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_single", this.keyRangeToScadsRangeSet(KeyRange("a","d")) )
		}
	}

	def testOffsetLimit() = {
		val clientlib = new LocalROWAClientLibrary
		val n1 = new TestableStorageNode()

		val ks = new SimpleKeySpace()
		ks.assign(n1, KeyRange("a", "f"))
		clientlib.add_namespace("db_offsetlimit",ks)

		// put two records
		assert( clientlib.put("db_offsetlimit",rec1) )
		assert( clientlib.put("db_offsetlimit",rec2) )
		assert( clientlib.put("db_offsetlimit",rec3) )
		assert( clientlib.put("db_offsetlimit",rec4) )
		assert( clientlib.put("db_offsetlimit",rec5) )

		// get range of records with an offset
		var desired = this.keyRangeToScadsRangeSet(KeyRange("a","ba"))
		desired.range.setOffset(1)
		var results = clientlib.get_set("db_offsetlimit", desired)
		assert(results.size()==1)
		assert(rec2==results.get(0))
		desired.range.setOffset(2)
		results = clientlib.get_set("db_offsetlimit", desired)
		assert(results.size()==0)

		// get range of records with a limit
		desired = this.keyRangeToScadsRangeSet(KeyRange("a","ca"))
		desired.range.setLimit(1)
		results = clientlib.get_set("db_offsetlimit", desired)
		assert(results.size()==1)
		assert(rec1==results.get(0))
		desired.range.setLimit(2)
		results = clientlib.get_set("db_offsetlimit", desired)
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		desired.range.setLimit(3)
		results = clientlib.get_set("db_offsetlimit", desired)
		assert(results.size()==3)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		assert(rec3==results.get(2))

		// get range with offset and limit
		desired = this.keyRangeToScadsRangeSet(KeyRange("a","ca"))
		desired.range.setLimit(1)
		desired.range.setOffset(1)
		results = clientlib.get_set("db_offsetlimit", desired)

		assert(results.size()==1)
		assert(rec2==results.get(0))
	}

	def testDoubleNodePartition() = {
		val clientlib = new LocalROWAClientLibrary
		val n1 = new TestableStorageNode()
		val n2 = new TestableStorageNode()

		val ks = new SimpleKeySpace()
		ks.assign(n1, KeyRange("a", "c"))
		ks.assign(n2, KeyRange("c", "e"))
		clientlib.add_namespace("db_double_p",ks)

		// put some records
		assert( clientlib.put("db_double_p",rec1) )
		assert( clientlib.put("db_double_p",rec2) )
		assert( clientlib.put("db_double_p",rec3) )
		assert( clientlib.put("db_double_p",rec4) )
		intercept[NoNodeResponsibleException] {
			clientlib.put("db_double_p",rec5)
		}

		// do a single get
		assert( clientlib.get("db_double_p","a") == (new SCADS.Record("a","a-val".getBytes())) )
		assert( clientlib.get("db_double_p","b") == (new SCADS.Record("b","b-val".getBytes())) )
		assert( clientlib.get("db_double_p","c") == (new SCADS.Record("c","c-val".getBytes())) )
		assert( clientlib.get("db_double_p","d") == (new SCADS.Record("d","d-val".getBytes())) )
		intercept[NoNodeResponsibleException] {
			clientlib.get("db_double_p","e")
		}

		// get a range of records, within range and outside range
		var results = clientlib.get_set("db_double_p", this.keyRangeToScadsRangeSet(KeyRange("a","bb")) )
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		/* TODO: this case doesn't work with the differing inclusiveness semantics of KeyRange and RangeSet
		results = clientlib.get_set("db_double_p", this.keyRangeToScadsRangeSet(KeyRange("c","d")) )
		assert(results.size()==1)
		assert(rec3==results.get(0))
		*/
		results = clientlib.get_set("db_double_p", this.keyRangeToScadsRangeSet(KeyRange("c","dd")) )
		assert(results.size()==2)
		assert(rec3==results.get(0))
		assert(rec4==results.get(1))
		results = clientlib.get_set("db_double_p", this.keyRangeToScadsRangeSet(KeyRange("b","dd")) )
		assert(results.size()==3)
		assert(rec2==results.get(0))
		assert(rec3==results.get(1))
		assert(rec4==results.get(2))
		results = clientlib.get_set("db_double_p", this.keyRangeToScadsRangeSet(KeyRange("a","dd")) )
		assert(results.size()==4)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		assert(rec3==results.get(2))
		assert(rec4==results.get(3))

		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_p", this.keyRangeToScadsRangeSet(KeyRange("1","c")) )
		}
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_p", this.keyRangeToScadsRangeSet(KeyRange("a","f")) )
		}

	}

	def testDoubleNodeReplica() = {
		val clientlib = new LocalROWAClientLibrary
		val n1 = new TestableStorageNode()
		val n2 = new TestableStorageNode()

		val ks = new SimpleKeySpace()
		ks.assign(n1, KeyRange("a", "ca"))
		ks.assign(n2, KeyRange("a", "ca"))
		clientlib.add_namespace("db_double_r",ks)

		// put two records
		assert( clientlib.put("db_double_r",rec1) )
		assert( clientlib.put("db_double_r",rec2) )

		// do a single get in range, on boundaries, outside responsibility
		assert( clientlib.get("db_double_r","a") == (new SCADS.Record("a","a-val".getBytes())) )
		assert( clientlib.get("db_double_r","b") == (new SCADS.Record("b","b-val".getBytes())) )
		assert( clientlib.get("db_double_r","c") == (new SCADS.Record("c",null)) )
		intercept[NoNodeResponsibleException] {
			clientlib.get("db_double_r","d")
		}

		// get a range of records, within range and outside range
		var results = clientlib.get_set("db_double_r", this.keyRangeToScadsRangeSet(KeyRange("a","bb")) )
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))

		results = clientlib.get_set("db_double_r", this.keyRangeToScadsRangeSet(KeyRange("a","c")) )
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))

		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_r", this.keyRangeToScadsRangeSet(KeyRange("1","b")) )
		}
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_r", this.keyRangeToScadsRangeSet(KeyRange("a","d")) )
		}
	}

	def testDoubleNodeOverlapPartition() = {
		val clientlib = new LocalROWAClientLibrary
		val n1 = new TestableStorageNode()
		val n2 = new TestableStorageNode()

		val ks = new SimpleKeySpace()
		ks.assign(n1, KeyRange("a", "c"))
		ks.assign(n2, KeyRange("b", "e"))
		clientlib.add_namespace("db_double_op",ks)

		// put some records
		assert( clientlib.put("db_double_op",rec1) )
		assert( clientlib.put("db_double_op",rec2) )
		assert( clientlib.put("db_double_op",rec3) )
		assert( clientlib.put("db_double_op",rec4) )
		intercept[NoNodeResponsibleException] {
			clientlib.put("db_double_op",rec5)
		}

		// do a single get
		assert( clientlib.get("db_double_op","a") == (new SCADS.Record("a","a-val".getBytes())) )
		assert( clientlib.get("db_double_op","b") == (new SCADS.Record("b","b-val".getBytes())) )
		assert( clientlib.get("db_double_op","c") == (new SCADS.Record("c","c-val".getBytes())) )
		assert( clientlib.get("db_double_op","d") == (new SCADS.Record("d","d-val".getBytes())) )
		intercept[NoNodeResponsibleException] {
			clientlib.get("db_double_op","e")
		}

		// get a range of records, within range and outside range
		var results = clientlib.get_set("db_double_op", this.keyRangeToScadsRangeSet(KeyRange("a","bb")) )
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		results = clientlib.get_set("db_double_op", this.keyRangeToScadsRangeSet(KeyRange("a","cc")) )
		assert(results.size()==3)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		assert(rec3==results.get(2))
		results = clientlib.get_set("db_double_op", this.keyRangeToScadsRangeSet(KeyRange("c","dd")) )
		assert(results.size()==2)
		assert(rec3==results.get(0))
		assert(rec4==results.get(1))
		results = clientlib.get_set("db_double_op", this.keyRangeToScadsRangeSet(KeyRange("b","e")) )
		assert(results.size()==3)
		assert(rec2==results.get(0))
		assert(rec3==results.get(1))
		assert(rec4==results.get(2))

		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_op", this.keyRangeToScadsRangeSet(KeyRange("1","c")) )
		}
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_op", this.keyRangeToScadsRangeSet(KeyRange("a","f")) )
		}
	}

	def testDoubleNodePartitionGap() {
		val clientlib = new LocalROWAClientLibrary
		val n1 = new TestableStorageNode()
		val n2 = new TestableStorageNode()

		val ks = new SimpleKeySpace()
		ks.assign(n1, KeyRange("a", "c"))
		ks.assign(n2, KeyRange("d", "f"))
		clientlib.add_namespace("db_double_gp",ks)

		// put some records
		assert( clientlib.put("db_double_gp",rec1) )
		assert( clientlib.put("db_double_gp",rec2) )
		assert( clientlib.put("db_double_gp",rec5) )
		assert( clientlib.put("db_double_gp",rec4) )
		intercept[NoNodeResponsibleException] {
			clientlib.put("db_double_gp",rec3) // key "c" should be left out this time
		}

		// do a single get
		assert( clientlib.get("db_double_gp","a") == (new SCADS.Record("a","a-val".getBytes())) )
		assert( clientlib.get("db_double_gp","b") == (new SCADS.Record("b","b-val".getBytes())) )
		assert( clientlib.get("db_double_gp","e") == (new SCADS.Record("e","e-val".getBytes())) )
		assert( clientlib.get("db_double_gp","d") == (new SCADS.Record("d","d-val".getBytes())) )
		intercept[NoNodeResponsibleException] {
			clientlib.get("db_double_gp","c")
		}

		// get a range of records, within range and outside range
		var results = clientlib.get_set("db_double_gp", this.keyRangeToScadsRangeSet(KeyRange("a","c")) )
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		results = clientlib.get_set("db_double_gp", this.keyRangeToScadsRangeSet(KeyRange("d","f")) )
		assert(results.size()==2)
		assert(rec4==results.get(0))
		assert(rec5==results.get(1))

		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_gp", this.keyRangeToScadsRangeSet(KeyRange("b","d")) )
		}
	}
}

class XTraceSuite extends Suite {
	def testCLtoStorageNode() = {
		val rec1 = new SCADS.Record("a","a-val".getBytes())
		val rec2 = new SCADS.Record("b","b-val".getBytes())

		val clientlib = new LocalROWAClientLibrary
		val n1 = new XtStorageNode("127.0.0.1",9000,9091)
		n1.protocol.xtrace_set_severity(1)

		val ks = new SimpleKeySpace()
		ks.assign(n1, KeyRange("a", "ca"))
		clientlib.add_namespace("db_xtrace",ks)

		// put record with xtrace
		println("start: "+System.currentTimeMillis().toString)
		assert( clientlib.put("db_xtrace",rec1) )
		println("end: "+System.currentTimeMillis().toString)
		println("start: "+System.currentTimeMillis().toString)
		assert( clientlib.put("db_xtrace",rec1) )
		println("end: "+System.currentTimeMillis().toString)
		println("start: "+System.currentTimeMillis().toString)
		assert( clientlib.put("db_xtrace",rec2) )
		println("end: "+System.currentTimeMillis().toString)

		// put without xtrace
		n1.protocol.xtrace_set_severity(6)
		println("start: "+System.currentTimeMillis().toString)
		assert( clientlib.put("db_xtrace",rec2) )
		println("end: "+System.currentTimeMillis().toString)
	}
}

class KeySpaceSuite extends Suite {
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

		println(nks)

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

	def testKeySpace() = {
		val n1 = new TestableStorageNode()
		val n2 = new TestableStorageNode()
		val n3 = new TestableStorageNode()
		val n4 = new TestableStorageNode()

		val ks = new SimpleKeySpace()

		ks.assign(n1, KeyRange("a", "c"))
		ks.assign(n2, KeyRange("b", "m"))
		ks.assign(n3, KeyRange("m", "n"))
		ks.assign(n4, KeyRange("n", "z"))

		assert(ks.lookup("a") contains n1)
		assert(ks.lookup("b") contains n1)
		assert(ks.lookup("b") contains n2)
	}
	def testNonCovered() = {
		val ks = new SimpleKeySpace()

		assert( !ks.isCovered(KeyRange("a","da"), Set(KeyRange("a","c"))) )
		assert( !ks.isCovered(KeyRange("a","da"), Set(KeyRange("b","d"),KeyRange("a","b"))) )
		assert( !ks.isCovered(KeyRange("aa","e"), Set(KeyRange("ab","b"),KeyRange("d","e"))) )

		assert( ks.isCovered(KeyRange("a","c"), Set(KeyRange("a","b"),KeyRange("a","c"))) )
		assert( ks.isCovered(KeyRange("a","c"), Set(KeyRange("b","c"),KeyRange("a","b"))) )
		assert( ks.isCovered(KeyRange("a","d"), Set(KeyRange("a","b"),KeyRange("b","c"),KeyRange("c","d"))) )
		assert( ks.isCovered(KeyRange("a","ef"), Set(KeyRange("a","b"),KeyRange("b","c"),KeyRange("c","f"))) )

		assert( ks.isCovered(KeyRange("a","f"), Set(KeyRange("a","c"),KeyRange("d","f"),KeyRange("a","d"))) )
	}
}

class KeyRangeSuite extends Suite {
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
}

class MovementMechanismTest extends Suite {
	val keyFormat = new java.text.DecimalFormat("0000")
	val keys = (0 to 1000).map((k) => keyFormat.format(k))

	class ConcurrentWriter(ks: KeySpace, prefix: String) extends Runnable{
		def run() = putKeys(ks, prefix)
	}

	def testSimpleMove() {
		val n1 = new TestableStorageNode()
		val n2 = new TestableStorageNode()
		val dp = new SimpleDataPlacement("test")

		dp.assign(n1, KeyRange("0000", "1001"))
		putKeys(dp, "value")
		checkKeys(dp, "value")

		dp.move(KeyRange("0500", "1001"), n1,n2)

		assert(dp.lookup("0000").contains(n1))
		assert(!dp.lookup("0000").contains(n2))
		assert(dp.lookup("0499").contains(n1))
		assert(!dp.lookup("0499").contains(n2))
		assert(!dp.lookup("0500").contains(n1))
		assert(dp.lookup("0500").contains(n2))
		assert(!dp.lookup("1000").contains(n1))
		assert(dp.lookup("1000").contains(n2))

		checkKeys(dp, "value")
	}

	def testSimpleCopy() {
		val n1 = new TestableStorageNode()
		val n2 = new TestableStorageNode()
		val dp = new SimpleDataPlacement("test")

		dp.assign(n1, KeyRange("0000", "1001"))
		putKeys(dp, "value")
		checkKeys(dp, "value")

		dp.copy(KeyRange("0000", "1001"), n1,n2)

		assert(dp.lookup("0000").contains(n1))
		assert(dp.lookup("0000").contains(n2))
		assert(dp.lookup("0499").contains(n1))
		assert(dp.lookup("0499").contains(n2))
		assert(dp.lookup("0500").contains(n1))
		assert(dp.lookup("0500").contains(n2))
		assert(dp.lookup("1000").contains(n1))
		assert(dp.lookup("1000").contains(n2))

		checkKeys(dp, "value")
	}

	def testRemove() {
		val n1 = new TestableStorageNode()
		val n2 = new TestableStorageNode()
		val dp = new SimpleDataPlacement("test")

		dp.assign(n1, KeyRange("0000", "1001"))
		putKeys(dp, "value")
		checkKeys(dp, "value")

		dp.copy(KeyRange("0000", "1001"), n1,n2)

		checkKeys(dp, "value")

		dp.remove(KeyRange("0000", "1001"), n1)

		assert(!dp.lookup("0000").contains(n1))
		assert(!dp.lookup("0499").contains(n1))
		assert(!dp.lookup("0500").contains(n1))
		assert(!dp.lookup("1000").contains(n1))

		checkKeys(dp, "value")
	}

	def testConcurrentMove(){
		val n1 = new TestableStorageNode()
		val n2 = new TestableStorageNode()
		val dp = new SimpleDataPlacement("test")

		dp.assign(n1, KeyRange("0000", "1001"))
		putKeys(dp, "00value")
		checkKeys(dp, "00value")

		val thread = new Thread(new ConcurrentWriter(dp, "01value"), "concurrentWriter")
		thread.start
		dp.move(KeyRange("0500", "1001"), n1.clone(),n2.clone())
		thread.join

		checkKeys(dp, "01value")
	}

	def testConcurrentCopy(){
		val n1 = new TestableStorageNode()
		val n2 = new TestableStorageNode()
		val dp = new SimpleDataPlacement("test")

		dp.assign(n1, KeyRange("0000", "1001"))
		putKeys(dp, "00value")
		checkKeys(dp, "00value")

		val thread = new Thread(new ConcurrentWriter(dp, "01value"), "concurrentWriter")
		thread.start
		dp.copy(KeyRange("0500", "1001"), n1.clone(),n2.clone())
		thread.join

		checkKeys(dp, "01value")
	}

	private def putKeys(ks: KeySpace, prefix: String) {
		keys.foreach((k) => {
			assert(ks.lookup(k).toList.length >= 1, "no one has key: " + k)
			ks.lookup(k).foreach((n) => {
				try {
					n.getClient().put("test", new SCADS.Record(k, (prefix + k).getBytes))
				}
				catch {
					case ex: SCADS.NotResponsible =>
				}
			})
		})
	}

	private def checkKeys(ks: KeySpace, prefix: String) {
		keys.foreach((k) => {
			assert(ks.lookup(k).toList.length >= 1, "no one has key: " + k)
			ks.lookup(k).foreach((n) => {
				val ret = new String(n.getClient().get("test", k).value)
				assert(ret == (prefix + k), "check failed on node: " + n + ", for key: " + k + ", got: " + ret + ", expected: " + (prefix + k))
			})
		})
	}
}

class RemoteKeySpaceTest extends Suite {
	class RemoteProviderTest extends RemoteKeySpaceProvider {
		val host = "localhost"
		val port = 8000
	}

	def testSimple() {
		val dp = new SimpleDataPlacement("test")
		val n1 = new TestableStorageNode
		val n2 = new TestableStorageNode
		dp.assign(n1, KeyRange("a", "z"))

		val server = new KeySpaceServer(8000)
		server.add("test", dp)

		val rks = new RemoteProviderTest
		assert(rks.getKeySpace("test").lookup("y") contains n1)
		dp.move(KeyRange("m", "z"), n1, n2)
		assert(dp.lookup("y") contains n2)
		assert(rks.getKeySpace("test").lookup("y") contains n1)

		rks.refreshKeySpace()
		assert(rks.getKeySpace("test").lookup("y") contains n2)
	}
}


object RunTests {
	def main(args: Array[String]) = {
		(new KeyRangeSuite).execute()
		(new KeySpaceSuite).execute()
		(new MovementMechanismTest).execute()
		(new ClientLibrarySuite).execute()
		(new RemoteKeySpaceTest).execute()
		System.exit(0)
	}
}
