package edu.berkeley.cs.scads.test
import edu.berkeley.cs.scads.storage.{ScadsCluster, SpecificNamespace, TestScalaEngine}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{WordSpec, Spec}
import edu.berkeley.cs.scads.comm._

import java.util.concurrent.ConcurrentHashMap

@RunWith(classOf[JUnitRunner])
class QuorumProtSpec extends WordSpec with ShouldMatchers {

  val storageHandlers = (1 to 10).map(_ => TestScalaEngine.getTestHandler)
  val cluster = TestScalaEngine.getTestCluster()
  
  implicit def toOption[A](a: A): Option[A] = Option(a)

  val storageServers =  cluster.getAvailableServers

  def createNS(name : String, repFactor : Int, readQuorum : Double, writeQuorum : Double) : SpecificNamespace[IntRec, IntRec] = {
    require(repFactor <= 10)
    val namespace = cluster.createNamespace[IntRec, IntRec](name, List((None,storageServers.slice(0, repFactor) )))
    namespace.setReadWriteQuorum(readQuorum, writeQuorum)
    return namespace
  }

  def getPartitions(storageServer : StorageService) : List[PartitionService] = {
    storageServer !? GetPartitionsRequest() match {
      case GetPartitionsResponse(partitions) => partitions
      case _ => throw new RuntimeException("Unknown message")
    }
  }

  def getAllVersions(ns : SpecificNamespace[IntRec, IntRec], key : Int) : Seq[Int] = {
    ns.getAllVersions(IntRec(key)).map(_._2.get.f1)
  }

  private final val V = new Object /* Dummy Key */

  class TestBlocker extends MessageHandlerListener[Message, Message] {
    private val blockedSenders   = new ConcurrentHashMap[ActorId, Object]
    private val blockedReceivers = new ConcurrentHashMap[ActorId, Object]

    def blockSenders(ra: List[RemoteActorProxy]) = ra foreach blockSender 
    def blockSender(ra: RemoteActorProxy) {
      blockedSenders.put(ra.id, V)
    }

    def unblockSenders(ra: List[RemoteActorProxy]) = ra foreach unblockSender 
    def unblockSender(ra: RemoteActorProxy) {
      blockedSenders.remove(ra.id)
    }

    def blockReceivers(ra: List[RemoteActorProxy]) = ra foreach blockReceiver 
    def blockReceiver(ra: RemoteActorProxy) {
      blockedReceivers.put(ra.id, V)
    }

    def unblockReceivers(ra: List[RemoteActorProxy]) = ra foreach unblockReceiver 
    def unblockReceiver(ra: RemoteActorProxy) {
      blockedReceivers.remove(ra.id)
    }

    def handleEvent(evt: MessageHandlerEvent[Message, Message]) = evt match {
      case MessagePending(remote, Left(msg)) =>
        msg.src.map(id => if (blockedSenders.containsKey(id)) DropMessage else RelayMessage).getOrElse(RelayMessage)
      case MessagePending(remote, Right(msg)) =>
        if (blockedReceivers.containsKey(msg.dest)) DropMessage else RelayMessage
    }
  }

  def withBlocker(f: TestBlocker => Unit) {
    val blocker = new TestBlocker
    MessageHandler.registerListener(blocker)
    try f(blocker) finally {
      MessageHandler.unregisterListener(blocker)
    }
  }

  "A Quorum Protocol" should {

    "respond after the read quorum" in {
      withBlocker { blocker =>
        val ns = createNS("quorum3:2:2_read",3, 0.6, 0.6)
        val blockedPartitions = getPartitions(storageServers.head)
        blocker.blockReceivers(blockedPartitions)
        ns.put(IntRec(1), IntRec(2))
        blocker.unblockReceivers(blockedPartitions)
        ns.get(IntRec(1)).get.f1 should equal (2)
      }
    }

    "respond after the write quorum" in {
      withBlocker { blocker =>
        val ns = createNS("quorum3:2:2_write",3, 0.6, 0.6)
        val blockedPartitions = getPartitions(storageServers.head)
        ns.put(IntRec(1), IntRec(2))
        blocker.blockReceivers(blockedPartitions)
        ns.get(IntRec(1)).get.f1 should equal (2)
        blocker.unblockReceivers(blockedPartitions)
      }
    }

    "read repair on get" in {
      withBlocker { blocker =>
        val ns = createNS("quorum3:2:2_repair",3, 0.6, 0.6)
        val blockedPartitions = getPartitions(storageServers.head)
        ns.put(IntRec(1), IntRec(1))
        ns.get(IntRec(1)).get.f1  should equal(1)
        blocker.blockReceivers(blockedPartitions)
        ns.put(IntRec(1), IntRec(2))
        blocker.unblockReceivers(blockedPartitions)
        var values = getAllVersions(ns, 1)

        values should contain(1)
        values should contain(2)
        ns.get(IntRec(1)) //should trigger read repair
        Thread.sleep(1000)      
        values = getAllVersions(ns, 1)
        values should have length (3)
        values should (contain (2) and not contain (1))
      }
    }

    "read repair with several servers" in {
      withBlocker { blocker =>
        val ns = createNS("quorum10:6:6_repair",10, 0.51, 0.51)
        val blockedPartitions = storageServers.slice(0, 4).flatMap(getPartitions(_))

        ns.put(IntRec(1), IntRec(1))
        ns.get(IntRec(1)).get.f1  should equal(1)
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
        values should (contain (2) and not contain (1))
      }
    }
    
    "read repair range requests" in {
      withBlocker { blocker =>
        val ns = createNS("quorum3:2:2_range",3, 0.51, 0.51)
        val blockedPartitions = getPartitions(storageServers.head)
        (1 to 50).foreach(i => ns.put(IntRec(i),IntRec(1)))
        blocker.blockReceivers(blockedPartitions)
        (1 to 50).foreach(i => ns.put(IntRec(i),IntRec(2)))
        blocker.unblockReceivers(blockedPartitions)
        Thread.sleep(1000)
        ns.getRange(None, None)
        Thread.sleep(1000)
        val allVersions = (1 to 50).flatMap(i => getAllVersions(ns, i))
        allVersions should (contain (2) and not contain (1))
      }
    }

    "tolerate message delays" is (pending)

    "increase the quorum when adding a new server" is (pending)

    "decrease the quorum when deleting a partition" is (pending)



  }

}

