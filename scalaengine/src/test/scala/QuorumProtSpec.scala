package edu.berkeley.cs.scads.test
import edu.berkeley.cs.scads.storage.{ScadsCluster, SpecificNamespace, TestScalaEngine}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{WordSpec, Spec}
import edu.berkeley.cs.scads.comm.{MessageHandlerFactory, MessageHandler, IntRec, StringRec}

@RunWith(classOf[JUnitRunner])
class QuorumProtSpec extends WordSpec with ShouldMatchers {
  val cluster = TestScalaEngine.getTestCluster(5)

  implicit def toOption[A](a: A): Option[A] = Option(a)

  MessageHandlerFactory.creatorFn = () => new MessageHandlerWrapper()
  val storageHandlers = TestScalaEngine.getTestHandler(10)

  val client1 = new ScadsCluster(storageHandlers.head.root, 0)
  val storageServers =  client1.getAvailableServers

  def createNS(name : String, repFactor : Int, readQuorum : Double, writeQuorum : Double) : SpecificNamespace[IntRec, IntRec] = {
    require(repFactor < 10)
    val namespace = client1.createNamespace[IntRec, IntRec](name, List((None,storageServers.slice(0, repFactor) )))
    namespace.setReadWriteQuorum(readQuorum, writeQuorum)
    return namespace
  }

  "A Quorum Protocl" should {

    "respond after the read quorum" in {
      
      val ns = createNS("quorum3:2:2_read",3, 2, 2)
      messageHandler.blockReceiver(storageServers.head)
      ns.put(IntRec(1), IntRec(2))
      messageHandler.unblockReceiver(storageServers.head)
    }

    "respond after the write quorum" in {
      val ns = createNS("quorum3:2:2_write",3, 2, 2)
      ns.put(IntRec(1), IntRec(2))
      messageHandler.blockReceiver(storageServers.head)
      ns.get(IntRec(1))
      messageHandler.unblockReceiver(storageServers.head)
    }

    "read repair on get" is (pending)
    
    "read repair range requests" is (pending)

    "tolerate dead servers" is (pending)

    "tolerate message delays" is (pending)

    "increase the quorum when adding a new server" is (pending)

    "decrease the quorum when deleting a partition" is (pending)



  }

}

