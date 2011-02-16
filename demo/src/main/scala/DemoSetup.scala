package edu.berkeley.cs
package radlab
package demo

import avro.marker._
import avro.runtime._

import scads.comm._
import deploylib.mesos._
import deploylib.ec2._

object ServiceSchedulerDaemon extends optional.Application {
  import DemoConfig._

  def main(mesosMaster: Option[String]): Unit = {
    System.loadLibrary("mesos")
    val scheduler = new ServiceScheduler(
      mesosMaster.getOrElse(DemoConfig.localMesosMasterPid),
      javaExecutorPath
    )
    serviceSchedulerNode.data = scheduler.remoteHandle.toBytes
  }
}


//TODO: Move to web app scheduler.
case class WebAppSchedulerTask(var name: String, var mesosMaster: String, var executor: String, var warFile: S3CachedJar, var zkWebServerListRoot: String, var properties: Map[String, String]) extends AvroTask with AvroRecord {
  import DemoConfig._

  def run(): Unit = {
    System.loadLibrary("mesos")
    val scheduler = new WebAppScheduler(
      name,
      mesosMaster,
      executor,
      warFile,
      properties,
      zkWebServerListRoot,
      serverCapacity=appServerCapacity, /*req/sec*/
      minServers=5,
      statsServer=dashboardDb)

    scheduler.registerActionListener(DemoScadr.post)
    scheduler.monitoringThread.join()
  }
}

object IntKeyScaleScheduler extends optional.Application {
  import DemoConfig._

  def main(name: String, mesosMaster: String, executor: String, cp: String): Unit = {
    System.loadLibrary("mesos")
    implicit val scheduler = LocalExperimentScheduler(name, mesosMaster, executor)
    implicit val classpath = cp.split("\\|").map(S3CachedJar(_)).toSeq
    implicit val zookeeper = zooKeeperRoot.getOrCreate("scads/perf") 

    import scads.perf.intkey._
    val cluster = DataLoader(1,1).newCluster
    RandomLoadClient(1, 1).schedule(cluster)
  }
}

object RepTestScheduler extends optional.Application {
  import DemoConfig._

  def main(name: String, mesosMaster: String, executor: String, cp: String, numKeys: Int): Unit = {
    System.loadLibrary("mesos")
    implicit val scheduler = LocalExperimentScheduler(name, mesosMaster, executor)
    implicit val classpath = cp.split("\\|").map(S3CachedJar(_)).toSeq
    implicit val zookeeper = zooKeeperRoot.getOrCreate("scads/perf") 

    import scads.perf.reptest._
    val cluster = DataLoader(1, numKeys).newCluster
    RepClient(3).schedule(cluster)

    cluster.root.awaitChild("expFinished")

    println("--- Printing results for exp with %d keys ---".format(numKeys))
    val loadResults = cluster.getNamespace[RepResultKey, RepResultValue]("repResults")
    println("iteration\telasped time (ms)")
    loadResults.getRange(None, None).foreach { case (k, v) => 
      println("%d\t%d".format(k.iteration, v.endTime - v.startTime))
    }

    //System.exit(0)
  }
}
