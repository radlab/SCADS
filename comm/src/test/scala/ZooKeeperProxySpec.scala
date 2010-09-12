package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import java.util.concurrent.CountDownLatch

import edu.berkeley.cs.scads.comm._

@RunWith(classOf[JUnitRunner])
class ZookeeperProxySpec extends Spec with ShouldMatchers {
  val zk1 = ZooKeeperHelper.getTestZooKeeper()
  val zk2 = new ZooKeeperProxy(zk1.address)

  def spawn(f: => Unit) = new Thread {
    start()
    override def run() { f }
  }

  describe("The zookeeper proxy") {
    it("should create nodes") {
      zk1.root.createChild("createTest")
      zk2.root("createTest")
    }

    it("should create nodes relativly") {
      zk1.root.createChild("level1").createChild("level2")
      zk2.root("level1/level2")

      zk1.root("level1").createChild("anotherLevel2")
      zk2.root("level1/anotherLevel2")
    }

    it("should get or create paths") {
      val newNode = zk1.root.getOrCreate("path1/path2/path3")
      newNode.path should equal("/path1/path2/path3")

      zk2.root("path1/path2/path3")
    }

    it("should delete nodes") {
      val newNode = zk1.root.createChild("test")
      zk1.root.children should contain(newNode)
      zk1.root("test")

      newNode.delete
      zk1.root.children should not contain(newNode)
      zk1.root.get("test") should equal(None)
    }

    it("should awaitChild properly") {

      val barrierA = new CountDownLatch(1)

      // wait thread
      spawn {
        zk1.root.awaitChild("testAwaitChild")
        barrierA.countDown()
        assert(zk1.root("testAwaitChild") != null)
      }

      Thread.sleep(3000) // try to give the wait thread a chance to block
      zk2.root.createChild("testAwaitChild")
      barrierA.await()


    }
  }
}
