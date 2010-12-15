package edu.berkeley.cs.scads.test

import org.scalatest.{ BeforeAndAfterAll, WordSpec }
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import org.junit.runner.RunWith

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._

@RunWith(classOf[JUnitRunner])
class RoutingTableSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll {
  val client1 = TestScalaEngine.newScadsCluster(10)
  val storageServices = client1.managedServices.toList

  implicit def toOption[A](a: A): Option[A] = Option(a)

  implicit def toIntRec(a : Int) : IntRec = new IntRec(a)

  implicit def toIntRecOption(a : Int) : Option[IntRec] = Some(new IntRec(a))

  implicit def toIntFromIntRec(a : IntRec) : Int = a.f1

  implicit def toStorageService(ra: RemoteActor): StorageService = 
    StorageService(ra.host, ra.port, ra.id)

  override def afterAll(): Unit = {
    client1.shutdownCluster()
  }

  "A Routing Table" should {

    "route requests" in {
      val ns : SpecificNamespace[IntRec, IntRec] = client1.createNamespace[IntRec, IntRec]("routingtest", List(
            (None,storageServices.slice(0,3)),
            (50,storageServices.slice(3,6)),
            (100 ,storageServices.slice(6,9))))
      (1 to 150 by 5).foreach(i => ns.put(IntRec(i), IntRec(i)))
      ns.getRange(None, None).map(_._1.f1) should equal (1 to 150 by 5)
      val ranges = ns.getAllRangeVersions(None, None).map(_._1.storageService)
      for( (orig, received) <- storageServices.zip(ranges))
        orig  should equal (received)
    }

    "add servers" in {
      val ns : SpecificNamespace[IntRec, IntRec] = client1.createNamespace[IntRec, IntRec]("addservertest", List(
            (None,storageServices.slice(0,3)),
            (50,storageServices.slice(4,7)),
            (100 ,storageServices.slice(7,10))))
      (1 to 150 by 5).foreach(i => ns.put(IntRec(i), IntRec(i)))
      val partitionToClone = ns.partitions.ranges(1).values(0)
      ns.replicatePartitions(List((partitionToClone, storageServices(3))))
      ns.getRange(None, None).map(_._1.f1) should equal (1 to 150 by 5)
      val ranges = ns.getAllRangeVersions(None, None).map(_._1.storageService)
      for( (orig, received) <- storageServices.zip(ranges))
        orig  should equal (received)
    }

    "delete servers" in {
      val ns : SpecificNamespace[IntRec, IntRec] = client1.createNamespace[IntRec, IntRec]("deleteservertest", List(
            (None,storageServices.slice(0,3)),
            (50,storageServices.slice(3,7)),
            (100 ,storageServices.slice(7,10))))
      (1 to 150 by 5).foreach(i => ns.put(IntRec(i), IntRec(i)))
      val partitionToDelete = ns.partitions.ranges(1).values(0)
      println("Going to delete partition: %s".format(partitionToDelete))
      ns.deletePartitions(List(partitionToDelete))
      println("DONE deleting partition")
      ns.getRange(None, None).map(_._1.f1) should equal (1 to 150 by 5)
      for(x <- (1 to 150 by 5)){
        ns.get(IntRec(x)).getOrElse(IntRec(-1)).f1 should equal (x)
      }
      val ranges = ns.getAllRangeVersions(None, None).map(_._1.storageService)
      val storageServ = storageServices.slice(0, 3) ::: storageServices.slice(4, 10)
      for( (orig, received) <- storageServ.zip(ranges))
        orig  should equal (received)
      
    }

    "split partitions" in {
       val ns : SpecificNamespace[IntRec, IntRec] = client1.createNamespace[IntRec, IntRec]("splitparttest", List(
            (None,storageServices.slice(0,3)),
            (100 ,storageServices.slice(3,6))))
       (1 to 150 by 5).foreach(i => ns.put(IntRec(i), IntRec(i)))
       ns.splitPartition(List(50, 150))
       ns.getRange(None, None).map(_._1.f1) should equal (1 to 150 by 5)
       val ranges = ns.getAllRangeVersions(None, None).map(_._1.storageService)
       ranges should have size (12)
    }

    "merge partitions" in {
      val ns : SpecificNamespace[IntRec, IntRec] = client1.createNamespace[IntRec, IntRec]("mergeparttest", List(
            (None,storageServices.slice(0,3)),
            (50 ,storageServices.slice(0,3)),
            (100 ,storageServices.slice(3,6)),
            (150 ,storageServices.slice(3,6))))
       (1 to 150 by 5).foreach(i => ns.put(IntRec(i), IntRec(i)))
       //println("Before routing table " + ns.routingTable)
       println("Before all range versions" + ns.getAllRangeVersions(None, None))
       Thread.sleep(1000) 
       ns.mergePartitions(List(IntRec(50), IntRec(150)))
       Thread.sleep(1000)
       //println("After Routing table " + ns.routingTable)
       println("After all range versions " + ns.getAllRangeVersions(None, None))
       ns.getRange(None, None).map(_._1.f1) should equal (1 to 150 by 5)
       val ranges = ns.getAllRangeVersions(None, None).map(_._1.storageService)
       ranges should have size (6)
    }

  }
}


