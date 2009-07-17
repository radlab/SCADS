package edu.berkeley.cs.scads.test

import org.scalatest.Suite

import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.nodes.{StorageNode,TestableBdbStorageNode}
import edu.berkeley.cs.scads.placement.{LocalDataPlacementProvider}
import edu.berkeley.cs.scads.client.{LocalROWAClientLibrary}
import edu.berkeley.cs.scads.thrift.{Record,RecordSet,RangeConversion}

class ClientLibraryTest extends Suite with RangeConversion with AutoKey {
	val rec1 = new Record("a","a-val")
	val rec2 = new Record("b","b-val")
	val rec3 = new Record("c","c-val")
	val rec4 = new Record("d","d-val")
	val rec5 = new Record("e","e-val")
	
	def testSingleNode() = {
		val clientlib = new LocalROWAClientLibrary
		val n1 = new TestableBdbStorageNode()
		
		val ks = Map[StorageNode,KeyRange](n1->KeyRange("a", "ca"))
		clientlib.add_namespace("db_single",ks)
		
		// put two records
		assert( clientlib.put("db_single",rec1) )
		assert( clientlib.put("db_single",rec2) )
		
		// do a single get in range, on boundaries, outside responsibility
		assert( clientlib.get("db_single","a") == (new Record("a","a-val")) )
		assert( clientlib.get("db_single","b") == (new Record("b","b-val")) )
		assert( clientlib.get("db_single","c") == (new Record("c",null)) )
		intercept[NoNodeResponsibleException] {
			clientlib.get("db_single","d")
		}	
		
		// get a range of records, within range and outside range
		var results = clientlib.get_set("db_single", new KeyRange("a","bb") )
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		results = clientlib.get_set("db_single", new KeyRange("a","c") )
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_single", new KeyRange("1","b") )
		}
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_single", new KeyRange("a","d") )
		}
		assert(true)
	}

	def testOffsetLimit() = {
		val clientlib = new LocalROWAClientLibrary
		val n1 = new TestableBdbStorageNode()
		
		val ks = Map[StorageNode,KeyRange](n1->KeyRange("a", "f"))
		clientlib.add_namespace("db_offsetlimit",ks)
		
		// put records
		assert( clientlib.put("db_offsetlimit",rec1) )
		assert( clientlib.put("db_offsetlimit",rec2) )
		assert( clientlib.put("db_offsetlimit",rec3) )
		assert( clientlib.put("db_offsetlimit",rec4) )
		assert( clientlib.put("db_offsetlimit",rec5) )
		
		// no offset
		var results = clientlib.get_set("db_offsetlimit", new KeyRange("a","bb") )
		assert(results.size()==2)
		
		// get range of records with an offset
		var desired = keyRangeToScadsRangeSet(new KeyRange("a","ba"))
		desired.range.setOffset(1)
		results = clientlib.get_set("db_offsetlimit", desired)
		assert(results.size()==1)
		assert(rec2==results.get(0))
		
		desired.range.setOffset(2)
		results = clientlib.get_set("db_offsetlimit", desired)
		assert(results.size()==0)
		
		// get range of records with a limit
		desired = keyRangeToScadsRangeSet(new KeyRange("a","ca"))
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
		desired = keyRangeToScadsRangeSet(new KeyRange("a","ca"))
		desired.range.setLimit(1)
		desired.range.setOffset(1)
		results = clientlib.get_set("db_offsetlimit", desired)

		assert(results.size()==1)
		assert(rec2==results.get(0))
	}
	
	def testDoubleNodePartition() = {
		val clientlib = new LocalROWAClientLibrary
		val n1 = new TestableBdbStorageNode()
		val n2 = new TestableBdbStorageNode()

		val ks = Map[StorageNode,KeyRange](n1->KeyRange("a", "c"), n2->KeyRange("c", "e"))
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
		assert( clientlib.get("db_double_p","a") == (new Record("a","a-val")) )
		assert( clientlib.get("db_double_p","b") == (new Record("b","b-val")) )
		assert( clientlib.get("db_double_p","c") == (new Record("c","c-val")) )
		assert( clientlib.get("db_double_p","d") == (new Record("d","d-val")) )
		intercept[NoNodeResponsibleException] {
			clientlib.get("db_double_p","e")
		}
		
		// get a range of records, within range and outside range
		var results = clientlib.get_set("db_double_p", new KeyRange("a","bb") )
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		//TODO: this case doesn't work with the differing inclusiveness semantics of KeyRange and RangeSet
		//results = clientlib.get_set("db_double_p", new KeyRange("c","d")) )
		//assert(results.size()==1)
		//assert(rec3==results.get(0))
		
		results = clientlib.get_set("db_double_p", new KeyRange("c","dd"))
		assert(results.size()==2)
		assert(rec3==results.get(0))
		assert(rec4==results.get(1))
		results = clientlib.get_set("db_double_p", new KeyRange("b","dd") )
		assert(results.size()==3)
		assert(rec2==results.get(0))
		assert(rec3==results.get(1))
		assert(rec4==results.get(2))
		results = clientlib.get_set("db_double_p", new KeyRange("a","dd") )
		assert(results.size()==4)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		assert(rec3==results.get(2))
		assert(rec4==results.get(3))
		
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_p", new KeyRange("1","c"))
		}		
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_p", new KeyRange("a","f"))
		}
		assert(true)
	}

	def testDoubleNodeReplica() = {
		val clientlib = new LocalROWAClientLibrary
		val n1 = new TestableBdbStorageNode()
		val n2 = new TestableBdbStorageNode()
		
		val ks = Map[StorageNode,KeyRange](n1->KeyRange("a", "ca"), n2->KeyRange("a", "ca"))
		clientlib.add_namespace("db_double_r",ks)
		
		// put two records
		assert( clientlib.put("db_double_r",rec1) )
		assert( clientlib.put("db_double_r",rec2) )
		
		// do a single get in range, on boundaries, outside responsibility
		assert( clientlib.get("db_double_r","a") == (new Record("a","a-val")) )
		assert( clientlib.get("db_double_r","b") == (new Record("b","b-val")) )
		assert( clientlib.get("db_double_r","c") == (new Record("c",null)) )
		intercept[NoNodeResponsibleException] {
			clientlib.get("db_double_r","d")
		}	
		
		// get a range of records, within range and outside range
		var results = clientlib.get_set("db_double_r", new KeyRange("a","bb")) 
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		
		results = clientlib.get_set("db_double_r", new KeyRange("a","c")) 
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_r", new KeyRange("1","b")) 
		}
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_r", new KeyRange("a","d")) 
		}
		assert(true)
	}

	def testDoubleNodeOverlapPartition() = {
		val clientlib = new LocalROWAClientLibrary
		val n1 = new TestableBdbStorageNode()
		val n2 = new TestableBdbStorageNode()

		val ks = Map[StorageNode,KeyRange](n1->KeyRange("a", "c"), n2->KeyRange("b", "e"))
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
		assert( clientlib.get("db_double_op","a") == (new Record("a","a-val")) )
		assert( clientlib.get("db_double_op","b") == (new Record("b","b-val")) )
		assert( clientlib.get("db_double_op","c") == (new Record("c","c-val")) )
		assert( clientlib.get("db_double_op","d") == (new Record("d","d-val")) )
		intercept[NoNodeResponsibleException] {
			clientlib.get("db_double_op","e")
		}
		
		// get a range of records, within range and outside range
		var results = clientlib.get_set("db_double_op", new KeyRange("a","bb"))
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		results = clientlib.get_set("db_double_op", new KeyRange("a","cc"))
		assert(results.size()==3)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		assert(rec3==results.get(2))
		results = clientlib.get_set("db_double_op", new KeyRange("c","dd"))
		assert(results.size()==2)
		assert(rec3==results.get(0))
		assert(rec4==results.get(1))
		results = clientlib.get_set("db_double_op", new KeyRange("b","e"))
		assert(results.size()==3)
		assert(rec2==results.get(0))
		assert(rec3==results.get(1))
		assert(rec4==results.get(2))
		
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_op", new KeyRange("1","c"))
		}
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_op", new KeyRange("a","f"))
		}
		assert(true)
	}

	def testDoubleNodePartitionGap() {
		val clientlib = new LocalROWAClientLibrary
		val n1 = new TestableBdbStorageNode()
		val n2 = new TestableBdbStorageNode()

		val ks = Map[StorageNode,KeyRange](n1->KeyRange("a", "c"), n2->KeyRange("d", "f"))
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
		assert( clientlib.get("db_double_gp","a") == (new Record("a","a-val")) )
		assert( clientlib.get("db_double_gp","b") == (new Record("b","b-val")) )
		assert( clientlib.get("db_double_gp","e") == (new Record("e","e-val")) )
		assert( clientlib.get("db_double_gp","d") == (new Record("d","d-val")) )
		intercept[NoNodeResponsibleException] {
			clientlib.get("db_double_gp","c")
		}
		
		// get a range of records, within range and outside range
		var results = clientlib.get_set("db_double_gp", new KeyRange("a","c"))
		assert(results.size()==2)
		assert(rec1==results.get(0))
		assert(rec2==results.get(1))
		results = clientlib.get_set("db_double_gp", new KeyRange("d","f"))
		assert(results.size()==2)
		assert(rec4==results.get(0))
		assert(rec5==results.get(1))
		
		intercept[NonCoveredRangeException] {
			clientlib.get_set("db_double_gp", new KeyRange("b","d"))
		}
	}

}
