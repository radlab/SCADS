package edu.berkeley.cs.scads
package storage
package test

import comm._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterAll, WordSpec}

import net.lag.logging.Logger
import java.util.concurrent.ConcurrentHashMap

@RunWith(classOf[JUnitRunner])
class QuorumProtSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll {
  protected val logger = Logger()

  implicit def toOption[A](a: A): Option[A] = Option(a)

  val cluster = TestScalaEngine.newScadsCluster(10)
  val storageServers = cluster.managedServices.toIndexedSeq

  override def afterAll(): Unit = {
    cluster.shutdownCluster()
  }

  def createNS(name: String, repFactor: Int, readQuorum: Double, writeQuorum: Double): SpecificNamespace[IntRec, IntRec] = {
    require(repFactor <= 10)
    val namespace = cluster.getNamespace[IntRec, IntRec](name)
    namespace.setPartitionScheme(List((None, storageServers.slice(0, repFactor))))
    namespace.setReadWriteQuorum(readQuorum, writeQuorum)
    return namespace
  }

  def getPartitions(storageServer: StorageService): List[PartitionService] = {
    storageServer !? GetPartitionsRequest() match {
      case GetPartitionsResponse(partitions) => partitions
      case _ => throw new RuntimeException("Unknown StorageMessage")
    }
  }

  def getAllVersions(ns: SpecificNamespace[IntRec, IntRec], key: Int): Seq[Int] = {
    ns.getAllVersions(ns.keyToBytes(IntRec(key))).map(v => ns.bytesToValue(v._2.get).f1)
  }

  private final val V = new Object

  /* Dummy Key */

  class TestBlocker extends MessageHandlerListener {
    private val blockedSenders = new ConcurrentHashMap[ServiceId, Object]
    private val blockedReceivers = new ConcurrentHashMap[ServiceId, Object]

    def blockSenders(ra: Seq[RemoteServiceProxy[StorageMessage]]) = ra foreach blockSender

    def blockSender(ra: RemoteServiceProxy[StorageMessage]) {
      blockedSenders.put(ra.id, V)
    }

    def unblockSenders(ra: Seq[RemoteServiceProxy[StorageMessage]]) = ra foreach unblockSender

    def unblockSender(ra: RemoteServiceProxy[StorageMessage]) {
      blockedSenders.remove(ra.id)
    }

    def blockReceivers(ra: Seq[RemoteServiceProxy[StorageMessage]]) = ra foreach blockReceiver

    def blockReceiver(ra: RemoteServiceProxy[StorageMessage]) {
      blockedReceivers.put(ra.id, V)
    }

    def unblockReceivers(ra: Seq[RemoteServiceProxy[StorageMessage]]) = ra foreach unblockReceiver

    def unblockReceiver(ra: RemoteServiceProxy[StorageMessage]) {
      blockedReceivers.remove(ra.id)
    }

    def handleEvent(evt: MessageHandlerEvent) = evt match {
      case MessagePending(remote, Left(msg)) =>
        val src = Option(msg.get(0).asInstanceOf[ServiceId])
        src.map(id => if (blockedSenders.containsKey(id)) DropMessage else RelayMessage).getOrElse(RelayMessage)
        RelayMessage
      case MessagePending(remote, Right(msg)) => {
        val dest = msg.get(0).asInstanceOf[ServiceId]
        if (blockedReceivers.containsKey(dest)) {
          logger.info("Dropping StorageMessage: %s", evt)
          DropMessage
        }
        else {
          RelayMessage
        }
      }
    }

    def withBlocker(f: TestBlocker => Unit) {
      val blocker = new TestBlocker
      StorageRegistry.registerListener(blocker)
      try f(blocker) finally {
        StorageRegistry.unregisterListener(blocker)
      }
    }

    "A Quorum Protocol" should {

      "respond after the write quorum" in {
        withBlocker {
          blocker =>
            val ns = createNS("quorum3:2:2_read", 3, 0.6, 0.6)
            val blockedPartitions = getPartitions(storageServers.head)
            blocker.blockReceivers(blockedPartitions)
            ns.put(IntRec(1), IntRec(2))
            blocker.unblockReceivers(blockedPartitions)
            ns.get(IntRec(1)).get.f1 should equal(2)
        }
      }


      "respond after the read quorum for get" in {
        withBlocker {
          blocker =>
            val ns = createNS("quorum3:2:2_write", 3, 0.6, 0.6)
            val blockedPartitions = getPartitions(storageServers.head)
            ns.put(IntRec(1), IntRec(2))
            blocker.blockReceivers(blockedPartitions)
            val startTime = System.currentTimeMillis
            ns.get(IntRec(1)).get.f1 should equal(2)
            val endTime = System.currentTimeMillis
            blocker.unblockReceivers(blockedPartitions)

            (endTime - startTime) should be <= (500L)
        }
      }

      "respond after the read quorum for getRange" in {
        withBlocker {
          blocker =>
            val ns = createNS("quorum3:2:2_write", 3, 0.6, 0.6)
            val blockedPartitions = getPartitions(storageServers.head)
            ns.put(IntRec(1), IntRec(2))
            ns.put(IntRec(2), IntRec(3))


            blocker.blockReceivers(blockedPartitions)
            val startTime = System.currentTimeMillis
            ns.getRange(None, None).size should equal(2)
            val endTime = System.currentTimeMillis
            blocker.unblockReceivers(blockedPartitions)

            (endTime - startTime) should be <= (500L)
        }
      }

      "read repair on get" in {
        withBlocker {
          blocker =>
            val ns = createNS("quorum3:2:2_repair", 3, 0.6, 0.6)
            val blockedPartitions = getPartitions(storageServers.head)
            ns.put(IntRec(1), IntRec(1))
            ns.get(IntRec(1)).get.f1 should equal(1)
            blocker.blockReceivers(blockedPartitions)
            ns.put(IntRec(1), IntRec(2))
            blocker.unblockReceivers(blockedPartitions)
            var values = getAllVersions(ns, 1)

            values should contain(1)
            values should contain(2)
            ns.get(IntRec(1)) //should trigger read repair
            QuorumProtocol.flushReadRepair
            values = getAllVersions(ns, 1)
            values should have length (3)
            values should (contain(2) and not contain (1))
        }
      }

      "read repair with several servers" in {
        withBlocker {
          blocker =>
            val ns = createNS("quorum10:6:6_repair", 10, 0.51, 0.51)
            val blockedPartitions = storageServers.slice(0, 4).flatMap(getPartitions(_))

            ns.put(IntRec(1), IntRec(1))
            ns.get(IntRec(1)).get.f1 should equal(1)
            blocker.blockReceivers(blockedPartitions)
            ns.put(IntRec(1), IntRec(2))
            blocker.unblockReceivers(blockedPartitions)
            var values = getAllVersions(ns, 1)
            values should contain(1)
            values should contain(2)
            ns.get(IntRec(1)) //should trigger read repair
            Thread.sleep(1000)
            values = getAllVersions(ns, 1)
            values should have length (10)
            values should (contain(2) and not contain (1))
        }
      }

      "read repair range requests" in {
        withBlocker {
          blocker =>
            val ns = createNS("quorum3:2:2_range", 3, 0.51, 0.51)
            val blockedPartitions = getPartitions(storageServers.head)
            (1 to 50).foreach(i => ns.put(IntRec(i), IntRec(1)))
            blocker.blockReceivers(blockedPartitions)
            (1 to 50).foreach(i => ns.put(IntRec(i), IntRec(2)))
            blocker.unblockReceivers(blockedPartitions)
            Thread.sleep(1000)
            ns.getRange(None, None)
            Thread.sleep(1000)
            val allVersions = (1 to 50).flatMap(i => getAllVersions(ns, i))
            allVersions should (contain(2) and not contain (1))
        }
      }

      "tolerate message delays" is (pending)

      "increase the quorum when adding a new server" is (pending)

      "decrease the quorum when deleting a partition" is (pending)
    }
  }

}
