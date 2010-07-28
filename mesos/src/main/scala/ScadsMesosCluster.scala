package edu.berkeley.cs.scads.mesos

import mesos._
import java.io.File
import org.apache.log4j.Logger

object ScadsScheduler {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("mesos")
    org.apache.log4j.BasicConfigurator.configure()
    new MesosSchedulerDriver(new ScadsScheduler(), "1@169.229.48.70:9999").run();
  }

}

class ScadsScheduler extends Scheduler {
  val logger = Logger.getLogger("scads.mesos.scheduler")
  var taskId = 0
  override def getFrameworkName(d: SchedulerDriver): String = "SCADS Framework"
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = new ExecutorInfo("/work/marmbrus/mesos/scads_executor", Array[Byte]())
  override def registered(d: SchedulerDriver, fid: String): Unit = logger.info("Registered SCADS Framework.  Fid: " + fid)

  override def resourceOffer(d: SchedulerDriver, oid: String, offers: SlaveOfferVector) = {
    logger.info("Got offer: " + offers)

    val tasks = new TaskDescriptionVector()
    val taskParams = new StringMap()
    val offer = offers.get(0)
    taskParams.set("cpus", "1")
    taskParams.set("mem", "134217728")
    tasks.add(new TaskDescription(taskId, offer.getSlaveId(), "task" + taskId, taskParams, "".getBytes))
    taskId += 1

    val params = new StringMap()
    params.set("timeout", "1")
    d.replyToOffer(oid, tasks, params)

  }

  def statusUpdate(d: SchedulerDriver, code: Int, message: String): Unit = {
    logger.info("Status Update: " + code + " " + message)
  }

}

object ScadsExecutor {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("mesos")
    val driver = new MesosExecutorDriver(new ScadsExecutor())
    driver.run()
  }
}

class ScadsExecutor extends Executor {
  System.loadLibrary("mesos")

  override def launchTask(d: ExecutorDriver, task: TaskDescription): Unit = {
    println("Starting storage handler" + task.getTaskId())
    println("Current Directory: " + System.getProperty("user.dir"))
    edu.berkeley.cs.scads.storage.ScalaEngine.main(Some("scads"), Some("r2:2181"), None, None, true)
  }
}
