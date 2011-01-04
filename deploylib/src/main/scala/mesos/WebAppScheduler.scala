package deploylib
package mesos

import _root_.mesos._
import edu.berkeley.cs.scads.comm._
import net.lag.logging.Logger
import scala.collection.JavaConversions._

class WebAppScheduler protected (name: String, mesosMaster: String, executor: String, warFile: ClassSource) extends Scheduler {
  val logger = Logger()
  var driver = new MesosSchedulerDriver(this, mesosMaster)
  val driverThread = new Thread("ExperimentScheduler Mesos Driver Thread") { override def run(): Unit = driver.run() }
  driverThread.start()

  override def getFrameworkName(d: SchedulerDriver): String = ": " + name
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = new ExecutorInfo(executor, Array[Byte]())
  override def registered(d: SchedulerDriver, fid: String): Unit = logger.info("Registered Framework.  Fid: " + fid)

  val webAppTask = JvmWebAppTask(warFile)
  var runningAppServers = 0
  var maxAppServers = 1

  override def resourceOffer(driver: SchedulerDriver, oid: String, offers: java.util.List[SlaveOffer]): Unit = {

    val tasks = offers.flatMap(offer => {
      if(runningAppServers < maxAppServers) {
        val taskParams = Map(List("mem", "cpus").map(k => k -> offer.getParams.get(k)):_*)

	runningAppServers += 1
	new TaskDescription(runningAppServers, offer.getSlaveId, webAppTask.toString, taskParams, JvmTask(webAppTask)) :: Nil
      }
      else
	Nil
    })

    driver.replyToOffer(oid, tasks, Map[String,String]())
  }


  override def statusUpdate(d: SchedulerDriver, status: TaskStatus): Unit = {
    if(status.getState == TaskState.TASK_FAILED || status.getState == TaskState.TASK_LOST || status.getState == TaskState.TASK_KILLED) {
      logger.warning("Status Update for Task %d: %s", status.getTaskId, status.getState)
      //TODO: restarted failed app servers
    }
    else {
      logger.debug("Status Update: " + status.getTaskId + " " + status.getState)
      logger.ifDebug(new String(status.getData))
    }
  }

  def stopDriver = driver.stop
}
