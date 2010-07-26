package edu.berkeley.cs.scads.mesos

import nexus._
import java.io.File
import org.apache.log4j.Logger

object Test {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("nexus")
    org.apache.log4j.BasicConfigurator.configure()
    new NexusSchedulerDriver(new ScadsScheduler(), "1@169.229.48.70:9999").run();
  }
}

class ScadsScheduler extends Scheduler {
  val logger = Logger.getLogger("scads.mesos.scheduler")
  var taskId = 0
  override def getFrameworkName(d: SchedulerDriver): String = "SCADS Framework"
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = new ExecutorInfo("/home/eecs/marmbrus/scads_executor", Array[Byte]())
  override def registered(d: SchedulerDriver, fid: String): Unit = logger.info("Registered SCADS Framework.  Fid: " + fid)

  override def resourceOffer(d: SchedulerDriver, oid: String, offers: SlaveOfferVector) = {
    logger.info("Got offer: " + offers)

    val tasks = new TaskDescriptionVector()
    val taskParams = new StringMap()
    val offer = offers.get(0)
    taskParams.set("cpus", "1")
    taskParams.set("mem", "134217728")
    tasks.add(new TaskDescription(taskId, offer.getSlaveId(), "task" + taskId, taskParams, Array[Byte]()))
    taskId += 1

    val params = new StringMap()
    params.set("timeout", "1")
    d.replyToOffer(oid, tasks, params)

  }

  def statusUpdate(d: SchedulerDriver, code: Int, message: String): Unit = {
    logger.info("Status Update: " + code + " " + message)
  }
}
