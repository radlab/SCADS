package edu.berkeley.cs.scads.test

import org.scalatest.Suite

import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.nodes.{TestableSimpleStorageNode,TestableBdbStorageNode,StorageNode}
import edu.berkeley.cs.scads.placement.{RunnableDataPlacementServer,LocalDataPlacementProvider,RemoteDataPlacementProvider}
import edu.berkeley.cs.scads.thrift.{Record,KnobbedDataPlacementServer,DataPlacement,RangeConversion,NotResponsible}
import edu.berkeley.cs.scads.client._
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}
import org.apache.log4j.Logger

class PlacementTest extends Suite with AutoKey {
	case class TestProvider extends LocalROWAClientLibrary

	def testNamespaceLookup() = {
		val n1 = new TestableBdbStorageNode()
		val n2 = new TestableBdbStorageNode()
		val n3 = new TestableBdbStorageNode()
		val n4 = new TestableBdbStorageNode()

		val ns = "namespacelookup"
		val ks = new TestProvider
		val mapping = Map[StorageNode,KeyRange](n1 -> KeyRange("a", "c"), n2 -> KeyRange("b", "m"), n3 -> KeyRange("m", "n"), n4 -> KeyRange("n", "z"))
		ks.add_namespace(ns, mapping)

		assert(ks.lookup(ns,"a") contains n1)
		assert(ks.lookup(ns,"b") contains n1)
		assert(ks.lookup(ns,"b") contains n2)
	}
}

class DataPlacementServerTest extends Suite with AutoKey with RangeConversion {
	val logger = Logger.getLogger("placement.testing")

	val p = 8000
	val h = "localhost"
	System.setProperty("placementport",p.toString)

	val dp = new RunnableDataPlacementServer // start up dp server on port p
	Thread.sleep(1000)
	val transport = new TFramedTransport(new TSocket(h, p))
	val protocol = new TBinaryProtocol(transport)
	val dpclient = new KnobbedDataPlacementServer.Client(protocol)
	transport.open()

	class LocalProviderTest extends LocalROWAClientLibrary
	class RemoteProviderTest extends SCADSClient(h,p)

	def testSetAndLookup {
		val namespace = "setandlookup"
		val node = new TestableSimpleStorageNode
		val list = new java.util.ArrayList[DataPlacement]()
		list.add(new DataPlacement(node.host,node.thriftPort,node.syncPort,KeyRange("0","9")))
		dpclient.add(namespace,list)

		val ks = new LocalProviderTest
		var mapping = Map[StorageNode, KeyRange]()
		val entries = dpclient.lookup_namespace(namespace)
		val iter = entries.iterator
		var entry:DataPlacement = null
		while (iter.hasNext) {
			entry = iter.next
			mapping += ( new StorageNode(entry.node,entry.thriftPort, entry.syncPort) -> entry.rset.range ) // implicit conversion
		}
		ks.add_namespace(namespace,mapping)
		assert(ks.lookup(namespace, new StringKey("0")).length==1)
		assert(ks.lookup(namespace, new StringKey("0"))(0)==node)
	}

	def testSetNull {
		val namespace = "setnull"
		val node = new TestableSimpleStorageNode
		val list = new java.util.ArrayList[DataPlacement]()
		list.add(new DataPlacement(node.host,node.thriftPort,node.syncPort,KeyRange(MinKey,MaxKey)))
		dpclient.add(namespace,list)

		val ks = new LocalProviderTest
		var mapping = Map[StorageNode, KeyRange]()
		val entries = dpclient.lookup_namespace(namespace)
		val iter = entries.iterator
		var entry:DataPlacement = null
		while (iter.hasNext) {
			entry = iter.next
			mapping += ( new StorageNode(entry.node,entry.thriftPort, entry.syncPort) -> entry.rset.range ) // implicit conversion
		}
		ks.add_namespace(namespace,mapping)
		logger.info("found: "+ks.lookup(namespace, new StringKey("0")).length)
		assert(ks.lookup(namespace, new StringKey("0")).length==1)
		assert(ks.lookup(namespace, new StringKey("0"))(0)==node)
	}
}