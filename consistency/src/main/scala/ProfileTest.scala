package edu.berkeley.cs
package scads
package consistency

import deploylib._
import deploylib.ec2._
import deploylib.mesos._
import avro.marker._
import comm._
import config._
import storage._

import net.lag.logging.Logger

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.storage.transactions._
import edu.berkeley.cs.scads.storage.transactions.FieldAnnotations._
import edu.berkeley.cs.scads.perf._
import edu.berkeley.cs.scads.piql.tpcw._

import scala.actors.Future
import scala.actors.Futures._

import edu.berkeley.cs.scads.config.Config

import tpcw._

import edu.berkeley.cs.scads.storage.transactions.mdcc._


object ProfileTest extends ExperimentBase {
  val logger = Logger()

  def run(): Unit = {

    Config.config.setBool("scads.mdcc.onEC2", false)
    Config.config.setBool("scads.mdcc.fastDefault", true)
    Config.config.setLong("scads.mdcc.DefaultRounds", 1)

    val numPartitions = 1
    val numClusters = 1
    val cluster = TestScalaEngine.newScadsClusters(numPartitions, numClusters)
    val loaderTask = new StressLoaderTask(cluster.root.canonicalAddress, numPartitions * numClusters, 1, 1000, numClusters, NSTxProtocolMDCC())
    loaderTask.run()

    var expStartTime: String = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date)

    val microItems = cluster.getNamespace[MicroItem]("microItems", NSTxProtocolMDCC())

    println("*************************************************************")
    println("*************************************************************")
    println("***********************STARTING******************************")
    println("*************************************************************")
    println("*************************************************************")

    val startT = System.nanoTime / 1000000

    val key: Array[Byte] = Array(1, 2, 3)
    val cstruct = CStruct(None, Nil)
    val md = MDCCMetadata(null, Nil, true, true)

    (0 until 50).map(_ => future {
      (0 until 1000).foreach(_ => {
        var handler = new MDCCRecordHandler(key, cstruct, null, Nil,  true, Nil, microItems.conflictResolver, null)
      })
    }).map(_())

    val endT = System.nanoTime / 1000000

    println("time: " + (endT - startT))

    println("*************************************************************")
    println("*************************************************************")
    println("***************************DONE******************************")
    println("*************************************************************")
    println("*************************************************************")
  }

  def main(args: Array[String]) {
    ProfileTest.run()
    println("Exiting...")
    System.exit(0)
  }
}
