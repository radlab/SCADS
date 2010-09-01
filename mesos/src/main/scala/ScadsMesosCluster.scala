package edu.berkeley.cs.scads.mesos

import mesos._
import java.io.File
import org.apache.log4j.Logger

import org.apache.zookeeper.CreateMode

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.ScadsClusterManager

import scala.collection.JavaConversions._

object ScadsMesosCluster {
  System.loadLibrary("mesos")

  def apply()(implicit zooRoot: ZooKeeperProxy#ZooKeeperNode): ScadsMesosCluster = {
    new ScadsMesosCluster(zooRoot.createChild("scads-mesos", mode = CreateMode.PERSISTENT_SEQUENTIAL))
  }
}

class ScadsMesosCluster(val root: ZooKeeperProxy#ZooKeeperNode) extends Scheduler with ScadsClusterManager {
  /* Self reference for use in the driver thread */
  self =>

  /* Invoke this to ensure the mesos libraries are loaded.  Is there a cleaner way? */
  ScadsMesosCluster
  val logger = Logger.getLogger("scads.mesos.scheduler")
  var taskId = 0

  val thread = new Thread() {
    override def run(): Unit = new MesosSchedulerDriver(self, "1@169.229.48.70:5050").run();
  }
  thread.start()

  override def getFrameworkName(d: SchedulerDriver): String = "SCADS Cluster " + root.path
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = new ExecutorInfo("/work/marmbrus/mesos/scads_executor", Array[Byte]())
  override def registered(d: SchedulerDriver, fid: String): Unit = logger.info("Registered SCADS Framework.  Fid: " + fid)

  override def resourceOffer(d: SchedulerDriver, oid: String, offers: java.util.List[SlaveOffer]) = {
    logger.info("Got offer: " + offers)

    val tasks = new java.util.LinkedList[TaskDescription]
    val taskParams = new java.util.TreeMap[String, String]
    offers.foreach(offer => {
      if(taskId < 10) {
        taskParams.put("cpus", offer.getParams.get("cpus"))
        taskParams.put("mem", offer.getParams.get("mem"))
        tasks.add(new TaskDescription(taskId, offer.getSlaveId(), "task" + taskId, taskParams, JvmProcess("/work/marmbrus/mesos/mesos-2.1.0-SNAPSHOT-jar-with-dependencies.jar", "edu.berkeley.cs.scads.storage.ScalaEngine", "--zooKeeper" :: "169.229.48.70:2181" :: "--zooBase" :: root.name :: "--verbose" :: Nil ).toBytes))
        taskId += 1
      }
    })

    val params = new java.util.TreeMap[String, String]
    d.replyToOffer(oid, tasks, params)
  }

  override def statusUpdate(d: SchedulerDriver, status: TaskStatus): Unit = {
    logger.info("Status Update: " + status.getTaskId + " " + status.getState)
    logger.debug(new String(status.getData))
  }

}
