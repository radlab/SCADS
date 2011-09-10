package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import java.util.concurrent.CountDownLatch

import edu.berkeley.cs.scads.comm._

@RunWith(classOf[JUnitRunner])
class ZookeeperProxySpec extends Spec with ShouldMatchers {
  //Try to make two seperate connections to zookeeper
  val zk1 = ZooKeeperHelper.getTestZooKeeper()
  val zk2 = new ZooKeeperProxy(zk1.proxy.address).root(zk1.path.drop(1))

  def spawn(f: => Unit) = new Thread {
    start()
    override def run() { f }
  }

  describe("The zookeeper proxy") {
    it("should create nodes") {
      zk1.createChild("createTest")
      zk2("createTest")
    }

    it("should create nodes relativly") {
      zk1.createChild("level1").createChild("level2")
      zk2("level1/level2")

      zk1("level1").createChild("anotherLevel2")
      zk2("level1/anotherLevel2")
    }

    it("should get or create paths") {
      val newNode = zk1.getOrCreate("path1/path2/path3")
      newNode.path should endWith("path1/path2/path3")

      zk2("path1/path2/path3")
    }

    it("should delete nodes") {
      val newNode = zk1.createChild("test")
      zk1.children should contain(newNode)
      zk1("test")

      newNode.delete
      zk1.children should not contain(newNode)
      zk1.get("test") should equal(None)
    }

    it("should awaitChild properly") {

      val barrierA = new CountDownLatch(1)

      // wait thread
      spawn {
        zk1.awaitChild("testAwaitChild")
        barrierA.countDown()
        assert(zk1("testAwaitChild") != null)
      }

      Thread.sleep(3000) // try to give the wait thread a chance to block
      zk2.createChild("testAwaitChild")
      barrierA.await()


    }
		it("should do onDataChange properly") {
			var changed = new scala.concurrent.SyncVar[Int]
			val newNode = zk1.createChild("changingNode")
			newNode.onDataChange( ()=> { changed.set(1)})
			newNode.data = "hi!".getBytes
			changed.get(1000).isDefined should equal(true)
		}
  }
}
