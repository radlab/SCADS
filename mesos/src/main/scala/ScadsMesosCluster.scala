package edu.berkeley.cs.scads.mesos

import mesos._
import java.io.File
import org.apache.log4j.Logger

import edu.berkeley.cs.scads.comm._

object ScadsScheduler {
  def main(args: Array[String]): Unit = {
    System.loadLibrary("mesos")
    org.apache.log4j.BasicConfigurator.configure()
    new MesosSchedulerDriver(new ScadsScheduler(), "1@169.229.48.74:5050").run();
  }

}

class ScadsScheduler extends Scheduler {
  val logger = Logger.getLogger("scads.mesos.scheduler")
  var taskId = 0
  override def getFrameworkName(d: SchedulerDriver): String = "SCADS Framework"
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = new ExecutorInfo("/work/marmbrus/mesos/scads_executor", Array[Byte]())
  override def registered(d: SchedulerDriver, fid: String): Unit = logger.info("Registered SCADS Framework.  Fid: " + fid)

  override def resourceOffer(d: SchedulerDriver, oid: String, offs: SlaveOfferVector) = {
    val offers = (0 to offs.capacity.toInt - 1).map(offs.get)
    logger.info("Got offer: " + offers)

    val tasks = new TaskDescriptionVector()
    val taskParams = new StringMap()
    offers.foreach(offer => {
      taskParams.set("cpus", offer.getParams.get("cpus"))
      taskParams.set("mem", offer.getParams.get("mem"))
      tasks.add(new TaskDescription(taskId, offer.getSlaveId(), "task" + taskId, taskParams, JvmProcess("/work/marmbrus/mesos/mesos-2.1.0-SNAPSHOT-jar-with-dependencies.jar", "edu.berkeley.cs.scads.storage.ScalaEngine", "--zooKeeper r6:2181" :: Nil ).toBytes))
      taskId += 1
    })

    val params = new StringMap()
    d.replyToOffer(oid, tasks, params)
  }

  def statusUpdate(d: SchedulerDriver, code: Int, message: String): Unit = {
    logger.info("Status Update: " + code + " " + message)
  }

}
