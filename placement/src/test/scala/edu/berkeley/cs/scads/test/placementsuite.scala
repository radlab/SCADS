package edu.berkeley.cs.scads.test

import org.scalatest.Suite
import edu.berkeley.cs.scads.AutoKey._
import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.TestableStorageNode

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