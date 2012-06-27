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

object StressTest extends ExperimentBase {
  val logger = Logger()

  // TODO(gpang): make the options do something.
  def run(protocol: NSTxProtocol = NSTxProtocolMDCC(),
          useLogicalUpdates: Boolean = true,
          useFast: Boolean = true,
          classicDemarcation: Boolean = true,
          localMasterPercentage: Int = 20): Unit = {
    println("starting stress test")

    Config.config.setBool("scads.mdcc.onEC2", false)

    if (useFast) {
      Config.config.setBool("scads.mdcc.fastDefault", true)
      Config.config.setLong("scads.mdcc.DefaultRounds", 1)
    } else {
      Config.config.setBool("scads.mdcc.fastDefault", false)
      Config.config.setLong("scads.mdcc.DefaultRounds", 9999999999L)
    }

    Config.config.setBool("scads.mdcc.classicDemarcation", classicDemarcation)
    Config.config.setLong("scads.mdcc.localMasterPercentage", localMasterPercentage)

    val numPartitions = 2
    val numClusters = 3
    val cluster = TestScalaEngine.newScadsClusters(numPartitions, numClusters)
    val loaderTask = new StressLoaderTask(cluster.root.canonicalAddress, numPartitions * numClusters, 150, 1000, numClusters, protocol)
    loaderTask.run()

    var expStartTime: String = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date)

    val stressTasks = StressWorkflowTask(
      numClients=2,
      executorClass="edu.berkeley.cs.scads.piql.exec.SimpleExecutor",
      numThreads=2,
      iterations=1,
      runLengthMin=1,
      startTime=expStartTime,
      useLogical=useLogicalUpdates,
      note="")
    println("starting txs")

    val t = stressTasks.testLocally(cluster)
    t.foreach(_.join)

//    cluster.shutdown

    println("stress test done")
  }

  def main(args: Array[String]) {
    StressTest.run()
    println("Exiting...")
    System.exit(0)
  }
}
