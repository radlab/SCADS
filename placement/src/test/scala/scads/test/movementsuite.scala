package edu.berkeley.cs.scads.test

import org.scalatest.Suite

import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.nodes.{TestableSimpleStorageNode,TestableBdbStorageNode,StorageNode}
import edu.berkeley.cs.scads.placement.{SimpleKnobbedDataPlacementServer,LocalDataPlacementProvider,RemoteDataPlacementProvider}
import edu.berkeley.cs.scads.thrift.{Record,DataPlacement,DataPlacementServer,RangeConversion,NotResponsible}
import edu.berkeley.cs.scads.client._
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}
import org.apache.log4j.Logger

class MovementMechanismTest extends Suite with AutoKey with RangeConversion {
	val keyFormat = new java.text.DecimalFormat("0000")
	val keys = (0 to 1000).map((k) => keyFormat.format(k))

	class ConcurrentWriter(ns: String, dp: SimpleKnobbedDataPlacementServer, prefix: String) extends Runnable{
		def run() = putKeys(ns,dp, prefix)
	}
	class TestPlacement extends SimpleKnobbedDataPlacementServer {
		def lookup(key: String):List[StorageNode] = {
			val dps = lookup_key("test",key)
			var ret = List[StorageNode]()
			val iter = dps.iterator
			while (iter.hasNext) {
				val entry:DataPlacement = iter.next
				ret += new StorageNode(entry.node, entry.thriftPort, entry.syncPort)
			}
			ret
		}
	}

	def testSimpleMove() {
		val n1 = new TestableSimpleStorageNode()
		val n2 = new TestableSimpleStorageNode()
		val dp = new TestPlacement
		val ns = "test"
		
		val list = new java.util.ArrayList[DataPlacement]()
		list.add(new DataPlacement(n1.host,n1.thriftPort,n1.syncPort,KeyRange("0000", "1001")))
		dp.add(ns,list)

		//dp.assign(n1, KeyRange("0000", "1001"))
		putKeys(ns,dp, "value")
		checkKeys(ns,dp, "value")

		dp.move(ns,KeyRange("0500", "1001"), n1.host, n1.thriftPort, n1.syncPort,n2.host, n2.thriftPort, n2.syncPort)

		assert(dp.lookup("0000").contains(n1))
		assert(!dp.lookup("0000").contains(n2))
		assert(dp.lookup("0499").contains(n1))
		assert(!dp.lookup("0499").contains(n2))
		assert(!dp.lookup("0500").contains(n1))
		assert(dp.lookup("0500").contains(n2))
		assert(!dp.lookup("1000").contains(n1))
		assert(dp.lookup("1000").contains(n2))

		checkKeys(ns,dp, "value")
	
	}

	def testSimpleCopy() {
		val n1 = new TestableSimpleStorageNode()
		val n2 = new TestableSimpleStorageNode()
		val dp = new TestPlacement
		val ns = "test"

		val list = new java.util.ArrayList[DataPlacement]()
		list.add(new DataPlacement(n1.host,n1.thriftPort,n1.syncPort,KeyRange("0000", "1001")))
		dp.add(ns,list)
		putKeys(ns,dp, "value")
		checkKeys(ns,dp, "value")

		dp.copy(ns,KeyRange("0000", "1001"), n1.host, n1.thriftPort, n1.syncPort,n2.host, n2.thriftPort, n2.syncPort)
	
		assert(dp.lookup("0000").contains(n1))
		assert(dp.lookup("0000").contains(n2))
		assert(dp.lookup("0499").contains(n1))
		assert(dp.lookup("0499").contains(n2))
		assert(dp.lookup("0500").contains(n1))
		assert(dp.lookup("0500").contains(n2))
		assert(dp.lookup("1000").contains(n1))
		assert(dp.lookup("1000").contains(n2))

		checkKeys(ns,dp, "value")
	}

	def testRemove() {
		val n1 = new TestableSimpleStorageNode()
		val n2 = new TestableSimpleStorageNode()
		val dp = new TestPlacement
		val ns = "test"

		val list = new java.util.ArrayList[DataPlacement]()
		list.add(new DataPlacement(n1.host,n1.thriftPort,n1.syncPort,KeyRange("0000", "1001")))
		dp.add(ns,list)
		putKeys(ns,dp, "value")
		checkKeys(ns,dp, "value")

		dp.copy(ns,KeyRange("0000", "1001"), n1.host, n1.thriftPort, n1.syncPort,n2.host, n2.thriftPort, n2.syncPort)

		checkKeys(ns,dp, "value")

		val list2 = new java.util.ArrayList[DataPlacement]()
		list2.add(new DataPlacement(n1.host,n1.thriftPort,n1.syncPort,KeyRange("0000", "1001")))
		dp.remove(ns, list2)

		assert(!dp.lookup("0000").contains(n1))
		assert(!dp.lookup("0499").contains(n1))
		assert(!dp.lookup("0500").contains(n1))
		assert(!dp.lookup("1000").contains(n1))

		checkKeys(ns,dp, "value")
	}

	def testConcurrentMove(){
		val n1 = new TestableSimpleStorageNode()
		val n2 = new TestableSimpleStorageNode()
		val dp = new TestPlacement
		val ns = "test"

		val list = new java.util.ArrayList[DataPlacement]()
		list.add(new DataPlacement(n1.host,n1.thriftPort,n1.syncPort,KeyRange("0000", "1001")))
		dp.add(ns,list)
		putKeys(ns,dp, "00value")
		checkKeys(ns,dp, "00value")

		val thread = new Thread(new ConcurrentWriter(ns,dp, "01value"), "concurrentWriter")
		thread.start
		dp.move(ns,KeyRange("0500", "1001"), n1.host, n1.thriftPort, n1.syncPort,n2.host, n2.thriftPort, n2.syncPort)
		thread.join

		checkKeys(ns,dp, "01value")
	}

	def testConcurrentCopy(){
		val n1 = new TestableSimpleStorageNode()
		val n2 = new TestableSimpleStorageNode()
		val dp = new TestPlacement
		val ns = "test"

		val list = new java.util.ArrayList[DataPlacement]()
		list.add(new DataPlacement(n1.host,n1.thriftPort,n1.syncPort,KeyRange("0000", "1001")))
		dp.add(ns,list)
		putKeys(ns,dp, "00value")
		checkKeys(ns,dp, "00value")

		val thread = new Thread(new ConcurrentWriter(ns,dp, "01value"), "concurrentWriter")
		thread.start
		dp.copy(ns,KeyRange("0500", "1001"), n1.host, n1.thriftPort, n1.syncPort,n2.host, n2.thriftPort, n2.syncPort)
		thread.join

		checkKeys(ns,dp, "01value")
	}

	private def putKeys(ns: String, dp: SimpleKnobbedDataPlacementServer, prefix: String) {
		var n:StorageNode = null		
		keys.foreach((k) => {
			assert(dp.lookup_key(ns,k).size >= 1, "no one has key: " + k)
			val iter = dp.lookup_key(ns,k).iterator
			var entry:DataPlacement = null
			while (iter.hasNext) {
				entry = iter.next
				n = new StorageNode(entry.node, entry.thriftPort, entry.syncPort)
				try {
					n.useConnection((c) => c.put(ns, new Record("'" + k + "'", (prefix + k))))
				}
				catch {
					case ex: NotResponsible =>
				}
			}
		})
	}
	
	private def checkKeys(ns: String, dp: SimpleKnobbedDataPlacementServer, prefix: String) {
		var serialized_k:String=null
		var n:StorageNode = null		
		keys.foreach((k) => {
			assert(dp.lookup_key(ns,k).size >= 1, "no one has key: " + k)
			val iter = dp.lookup_key(ns,k).iterator
			var entry:DataPlacement = null
			while (iter.hasNext) {
				entry = iter.next
				n = new StorageNode(entry.node, entry.thriftPort, entry.syncPort)
				serialized_k="'" + k + "'"
				val ret = new String(n.useConnection((c) => c.get(ns, serialized_k).value))
				assert(ret == (prefix + k), "check failed on node: " + n + ", for key: " + k + ", got: " + ret + ", expected: " + (prefix + k))
			}
		})
	}
}