import org.scalatest.Suite

case class ClientApp(h: String, p: Int) extends ThriftConnection {
	val host = h
	val port = p 

	val client = new SCADS.ClientLibrary.Client(protocol)
	
}

class ClientLibraryServer(p: Int) extends ThriftServer {
	val port = p
	val clientlib = new ROWAClientLibrary
	val processor = new SCADS.ClientLibrary.Processor(clientlib)

	val n1 = new StorageNode("localhost", 9000)
	val ks = new SimpleKeySpace()
	ks.assign(n1, KeyRange("a", "c"))
	clientlib.add_namespace("db",ks)
}

class ClientLibrarySuite extends Suite {
	
	def testSingleNode() = {
		val clientlib = new ROWAClientLibrary
		val n1 = new StorageNode("localhost", 9000)
		n1.connect
		
		val ks = new SimpleKeySpace()
		ks.assign(n1, KeyRange("a", "c"))
		clientlib.add_namespace("db_single",ks)
		
		val rec1 = new SCADS.Record("a","a-val".getBytes())
		val rec2 = new SCADS.Record("b","b-val".getBytes())
		
		clientlib.put("db_single",rec1)
		clientlib.put("db_single",rec2)
		
		// do a single get
		val result = clientlib.get("db_single","a")
		assert(result==(new SCADS.Record("a","a-val".getBytes())))
		val result2 = clientlib.get("db_single","b")
		assert(result2==(new SCADS.Record("b","b-val".getBytes())))	
		
		// get a range of records
		val desired = new SCADS.RecordSet
		val range = new SCADS.RangeSet
		desired.setType(SCADS.RecordSetType.RST_RANGE)
		desired.setRange(range)
		range.setStart_key("a")
		range.setEnd_key("c")
			
		val results = clientlib.get_set("db_single",desired)

		assert(results.size()==2)
		assert(rec1==results.get(0)) // should really do a sort first
		assert(rec2==results.get(1))
	}

	def testDoubleNode() = {
		val clientlib = new ROWAClientLibrary
		val n1 = new StorageNode("localhost", 9000)
		n1.connect
		val n2 = new StorageNode("localhost", 9001)
		n2.connect

		val ks = new SimpleKeySpace()
		ks.assign(n1, KeyRange("a", "c"))
		ks.assign(n2, KeyRange("b", "d"))
		clientlib.add_namespace("db_double",ks)
		
		assert(clientlib.getMap contains "db_double")
		assert(clientlib.getMap("db_double").lookup("b") contains n1)
		assert(clientlib.getMap("db_double").lookup("b") contains n2)
		
		val rec1 = new SCADS.Record("a","a-val".getBytes())
		val rec2 = new SCADS.Record("b","b-val".getBytes())
		val rec3 = new SCADS.Record("c","c-val".getBytes())
		
		clientlib.put("db_double",rec1)
		clientlib.put("db_double",rec2)
		clientlib.put("db_double",rec3)
		
		// do a single get
		val result = clientlib.get("db_double","a")
		assert(result==(new SCADS.Record("a","a-val".getBytes())))
		val result2 = clientlib.get("db_double","b")
		assert(result2==(new SCADS.Record("b","b-val".getBytes())))
		val result3 = clientlib.get("db_double","c")
		assert(result3==(new SCADS.Record("c","c-val".getBytes())))
	
		// get a range of records
		val desired = new SCADS.RecordSet
		val range = new SCADS.RangeSet
		desired.setType(SCADS.RecordSetType.RST_RANGE)
		desired.setRange(range)
		range.setStart_key("a")
		range.setEnd_key("ca")
		val results = clientlib.get_set("db_double",desired)
	
		assert(results.size()==3)
		//assert(rec1==results.get(0))
		//assert(rec2==results.get(1))
		//assert(rec3==results.get(2))
	}

	/*
	def testNonCovered() = {
		val clientlib = new ROWAClientLibrary
		val n1 = new StorageNode("localhost", 9000)
		n1.connect
		
		val ks = new SimpleKeySpace()
		ks.assign(n1, KeyRange("a", "c"))
		clientlib.add_namespace("db_cover",ks)
		
		val rec1 = new SCADS.Record("a","a-val".getBytes())
		val rec2 = new SCADS.Record("b","b-val".getBytes())
		
		clientlib.put("db_cover",rec1)
		clientlib.put("db_cover",rec2)
		
		// get a range of records
		val desired = new SCADS.RecordSet(3, new SCADS.RangeSet("a","da",0,100),null) // TODO: fix limit
		//val results = clientlib.get_set("db_cover",desired)
		
		assert( !ks.isCovered(KeyRange("a","da"), Set(KeyRange("a","c"))) )
	}
	*/
}

class KeySpaceSuite extends Suite {
	def testKeySpace() = {
		val n1 = new StorageNode("localhost", 9000)
		val n2 = new StorageNode("localhost", 9001)
		val n3 = new StorageNode("localhost", 9002)
		val n4 = new StorageNode("localhost", 9003)

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
		//(new KeyRangeSuite).execute()
		//(new KeySpaceSuite).execute()
		(new ClientLibrarySuite).execute()
	}
}