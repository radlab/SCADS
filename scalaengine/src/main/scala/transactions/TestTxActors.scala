package edu.berkeley.cs.scads.storage.examples
import edu.berkeley.cs.scads.storage.transactions._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker.{AvroRecord, AvroPair}
import edu.berkeley.cs.scads.util.Logger

import scala.actors.Actor
import scala.actors.Actor._

import edu.berkeley.cs.scads.storage.transactions.FieldAnnotations._

import java.util.concurrent.Semaphore
import scala.util.Random
import scala.collection.mutable.HashSet
import _root_.org.fusesource.hawtdispatch._
import java.util.concurrent.atomic.AtomicInteger

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

object Config {
  val PRODUCTS = 5
  val TRX_SIZE = 3
  val CLIENTS = 1
  val ROUNDS = 15
  val LOGICAL = true
  val STOCK = 1
}

object Client {
  @volatile var ready = true
  var committed =  new AtomicInteger(0)
  var aborted =  new AtomicInteger(0)
}

class Client(id : Int, nsPair: PairNamespace[DataRecordActor] with PairTransactions[DataRecordActor], sema: Semaphore) extends Actor {
  import Config._
  import Client._

  monitor_hawtdispatch()


  def monitor_hawtdispatch() :Unit = {

    import java.util.concurrent.TimeUnit._

    // do the actual check in 1 second..
    getGlobalQueue().after(5, SECONDS) {
      println("Stats: Total Trx: " + (committed.intValue()  + aborted.intValue() ) + " Aborted:" + aborted.intValue()  + " Committed: " + committed.intValue())
      committed.getAndSet(0)
      aborted.getAndSet(0)
      // to check again...
      monitor_hawtdispatch
    }
  }

  private val logger = Logger(classOf[Client])

  def act() {
    for (i <- 0 until ROUNDS) {
      if(!Client.ready) assert(false, "A Trx was unsuccesfull")
      logger.info("%s Starting update", this.hashCode())
      val s = System.currentTimeMillis
      val tx = new Tx(10000) ({
        val items = HashSet[Int]()
        do{
          items += Random.nextInt(PRODUCTS)
        }while(items.size < TRX_SIZE)
        if (LOGICAL) {
          items.foreach( x=> {
            val dr = DataRecordActor(x)
            dr.s = ""; dr.a = -1; dr.b = 0; dr.c = 0
            println("Putting " + dr)
            nsPair.putLogical(dr)
          })
        } else {
          items.foreach( x=> {
            val dr = nsPair.getRecord(DataRecordActor(x)).get
            dr.a = dr.a - 1
            nsPair.put(dr)
          })
        }
      })
      val status = tx.Execute()
      status match {
        case UNKNOWN => {
          println("#####################  UNKNOWN ######################")
          Client.ready =false
        }
        case COMMITTED => committed.getAndIncrement()
        case ABORTED => aborted.getAndIncrement()
      }

      logger.info("%s Finished the Trx in %s", this.hashCode(), (System.currentTimeMillis() - s))
      Thread.sleep(1000)
      //Thread.sleep(1000)
      println("XXXXXXXXXXXXXXXXXXXXXXXXXXXX Finished Trx " + i + " XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
      Thread.sleep(1000)
    }

    sema.release
  }
}

class TestTxActors {
  import Config._

  def run() {
    val cluster = TestScalaEngine.newScadsCluster(5)

    val nsPair = cluster.getNamespace[DataRecordActor]("testnsPair", NSTxProtocolMDCC())
    nsPair.setPartitionScheme(List((None, cluster.getAvailableServers)))
    Thread.sleep(1000)
    var dr = DataRecordActor(0)
    dr.s = "a"; dr.a = STOCK ; dr.b = 100; dr.c = 1.0.floatValue
    nsPair.put(dr)
    (1 to PRODUCTS).foreach( x => {
      dr.id = x
      nsPair.put(dr)
    })

    val sema = new Semaphore(0)
    val clients = (1 to CLIENTS).map(x => new Client(x, nsPair, sema))

    // Start client actors.
    clients.foreach(_.start)

    // Wait for actors to be done.
    clients.foreach(x => sema.acquire)

    // Sleep for a little bit to wait for the commits.
    Thread.sleep(2000)
    println("We are done \n result: ")
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
