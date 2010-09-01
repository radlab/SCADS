package edu.berkeley.cs.scads.mesos

import mesos._
import java.io.File
import org.apache.log4j.Logger

import edu.berkeley.cs.scads.comm._

import scala.collection.JavaConversions._

object ScadsScheduler {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("mesos")
    org.apache.log4j.BasicConfigurator.configure()
    new MesosSchedulerDriver(new ScadsScheduler(), "1@169.229.48.70:5050").run();
  }

}

class ScadsScheduler extends Scheduler {
  val logger = Logger.getLogger("scads.mesos.scheduler")
  var taskId = 0
  override def getFrameworkName(d: SchedulerDriver): String = "SCADS Framework"
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
        tasks.add(new TaskDescription(taskId, offer.getSlaveId(), "task" + taskId, taskParams, JvmProcess("/work/marmbrus/mesos/mesos-2.1.0-SNAPSHOT-jar-with-dependencies.jar", "edu.berkeley.cs.scads.storage.ScalaEngine", "--zooKeeper" :: "169.229.48.70:2181" :: "--verbose" :: Nil ).toBytes))
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
