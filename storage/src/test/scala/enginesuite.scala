package edu.berkeley.cs.scads.test

import org.scalatest.Suite

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.nodes.ConnectionPool
import edu.berkeley.cs.scads.nodes.StorageNode
import edu.berkeley.cs.scads.nodes.TestableBdbStorageNode
import edu.berkeley.cs.scads.nodes.TestableSimpleStorageNode

class NodeTest extends Suite {
  class ParallelGet(n: StorageNode) extends Runnable {
    def run() = {
      for(i <- (1 to 100))
        assert(n.useConnection((c) => c.get("test", "test")) == new Record("test", null))
    }
  }

  val sn = new TestableSimpleStorageNode()
  val bn = new TestableBdbStorageNode()
  
  def testHarness() {
    assert(sn.useConnection((c) => c.get("test", "test")) == new Record("test", null))
    assert(bn.useConnection((c) => c.get("test", "test")) == new Record("test", null))
  }

  def testEquality() {
    val sn2 = new StorageNode(sn.host, sn.thriftPort, sn.syncPort)

    assert(sn != bn)
    assert(sn2 == sn)
  }

  def testThreadSafety() {
    val threads = (1 to 100).toList.map((i) => new Thread(new ParallelGet(bn)))
    threads.foreach((t: Thread) => t.start)
    threads.foreach((t: Thread) => t.join)

    val threads2 = (1 to 100).toList.map((i) => new Thread(new ParallelGet(bn)))
    threads2.foreach((t: Thread) => t.start)
    threads2.foreach((t: Thread) => t.join)

    assert(ConnectionPool.connections(bn).pool.size > 0)
    assert(ConnectionPool.connections(bn).pool.size <= 100)
  }

  def testResponsibility() {
	val sn = new TestableBdbStorageNode()
	sn.useConnection((c) => {
		val range = new RangeSet()
		val rs = new RecordSet(RecordSetType.RST_RANGE, range, null, null)
		val rec = new Record("beth", "sexy")
		c.set_responsibility_policy("tr", rs)
		c.put("tr", new Record("beth", "sexy"))
		assert(rec === c.get("tr", "beth"))
		assert(rec === c.get_set("tr", rs).get(0))

		val range2 = new RangeSet
		range2.setStart_key("a")
		range2.setEnd_key("m")
		rs.setRange(range2)
		assert(rec === c.get_set("tr", rs).get(0))

	})
  }
}
