package edu.berkeley.cs.scads.storage.examples
import edu.berkeley.cs.scads.storage.transactions._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.{AvroRecord, AvroPair}

import scala.actors.Actor
import scala.actors.Actor._

import edu.berkeley.cs.scads.storage.transactions.FieldAnnotations._

import java.util.concurrent.Semaphore

case class DataRecordActor(var id: Int) extends AvroPair {
  var s: String = _
  @FieldGE(0)
  @FieldLT(200)
  var a: Int = _
  @FieldGE(0)
  @FieldLT(200)
  var b: Long = _
  var c: Float = _

  override def toString = "DataRecord(" + id + ", " + s + ", " + a + ", " + b + ", " + c + ")"
}

class Client(nsPair: PairNamespace[DataRecordActor] with PairTransactions[DataRecordActor], sema: Semaphore, useLogical: Boolean = false) extends Actor {
  def act() {
    for (i <- 0 until 3) {
      println("" + this.hashCode() + " Starting update")
      new Tx(1000) ({
        if (!useLogical) {
          val dr = nsPair.getRecord(DataRecordActor(1)).get
          dr.a = dr.a - 1
          nsPair.put(dr)
        } else {
          val dr = DataRecordActor(1)
          dr.s = ""; dr.a = -1; dr.b = 0; dr.c = 0
          nsPair.putLogical(dr)
        }
      }).Execute()
    }

    sema.release
  }
}

class TestTxActors {
  def run() {
    val cluster = TestScalaEngine.newScadsCluster(4)

    val nsPair = cluster.getNamespace[DataRecordActor]("testnsPair", NSTxProtocolMDCC())
    nsPair.setPartitionScheme(List((None, cluster.getAvailableServers)))
    Thread.sleep(1000)
    var dr = DataRecordActor(1)
    dr.s = "a"; dr.a = 10; dr.b = 100; dr.c = 1.0.floatValue
    nsPair.put(dr)
    dr.id = 2
    nsPair.put(dr)

    val numClients = 5
    val sema = new Semaphore(0)
    val clients = List.fill(numClients)(new Client(nsPair, sema, true))

    // Start client actors.
    clients.foreach(_.start)

    // Wait for actors to be done.
    clients.foreach(x => sema.acquire)

    // Sleep for a little bit to wait for the commits.
    Thread.sleep(2000)
    println("result: ")
    nsPair.getRange(None, None).foreach(x => println(x))
  }
}

object TestTxActors {
  def main(args: Array[String]) {
    val test = new TestTxActors()
    test.run()
    println("Done with test")
    Thread.sleep(100000)
    println("Exiting...")
    System.exit(0)
  }
}
