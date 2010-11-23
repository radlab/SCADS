package edu.berkeley.cs.scads.test

import org.scalatest.{ BeforeAndAfterAll, Spec }
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import org.junit.runner.RunWith

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.comm._

@RunWith(classOf[JUnitRunner])
class NamespaceSpec extends Spec with ShouldMatchers with BeforeAndAfterAll {

  val cluster = TestScalaEngine.newScadsCluster(5)

  override def afterAll(): Unit = {
    cluster.shutdownCluster()
  }

  describe("Namespace") {
    it("should properly delete namespaces") {
      val servers = cluster.managedServices 
      val ns = cluster.createNamespace[IntRec, StringRec](
        "deletetest",
        Seq((None, List(servers(0))), // [-inf, 25)
            (Some(IntRec(25)), List(servers(1))), // [25, 50)
            (Some(IntRec(50)), List(servers(2), servers(3))), // [50, 100)
            (Some(IntRec(100)), List(servers(4))))) // [100, inf)

      (0 to 120).foreach(i => ns.put(IntRec(i), StringRec(i.toString)))

      ns.delete()

      val ns1 = cluster.createNamespace[IntRec, StringRec](
        "deletetest",
        Seq((None, servers.toList)))

      (0 to 120).foreach(i => ns1.get(IntRec(i)) match {
        case None => // success
        case Some(k) => fail("Should not have retained data: %s".format(k))
      })

      ns1.delete()
    }
  }

}
