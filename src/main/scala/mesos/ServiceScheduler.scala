package deploylib
package mesos

import _root_.mesos._
import java.io.File
import net.lag.logging.Logger

import scala.collection.JavaConversions._

object ExperimentScheduler {
  System.loadLibrary("mesos")

  def apply(name: String, mesosMaster: String = "1@" + java.net.InetAddress.getLocalHost.getHostAddress + ":5050") = new ExperimentScheduler(name, mesosMaster)
}

class ExperimentScheduler protected (name: String, mesosMaster: String) extends Scheduler {
  val logger = Logger()
  var taskId = 0
  var driver = new MesosSchedulerDriver(this, mesosMaster)

  val driverThread = new Thread("ExperimentScheduler Mesos Driver Thread") { override def run(): Unit = driver.run() }
  driverThread.start()

  type ProcessList = IndexedSeq[JvmProcess]
  var outstandingExperiments = List[ProcessList]()

  def scheduleExperiment(processes: ProcessList): Unit = synchronized {
    outstandingExperiments ::= processes
  }

  override def getFrameworkName(d: SchedulerDriver): String = "SCADS Service Framework: " + name
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = new ExecutorInfo("/root/java_executor", Array[Byte]())
  override def registered(d: SchedulerDriver, fid: String): Unit = logger.info("Registered SCADS Framework.  Fid: " + fid)

  override def resourceOffer(d: SchedulerDriver, oid: String, offers: java.util.List[SlaveOffer]) = {
    val tasks = new java.util.LinkedList[TaskDescription]

    var remainingExperiments = List[ProcessList]()
    synchronized {
      for(procList <- outstandingExperiments) {
        if(procList.size <= offers.size) {
          procList.foreach(proc => {
            val offer = offers.remove(0)
            val taskParams = Map(List("mem", "cpus").map(k => k -> offer.getParams.get(k)):_*)
            val task = new TaskDescription(taskId, offer.getSlaveId, proc.mainclass, taskParams, proc.toBytes)
            taskId += 1

            logger.info("Scheduling experiment: %s", procList)
            tasks add task
          })
        }
        else {
          logger.debug("Not enough resources to schedule experiment of size %d", procList.size)
          remainingExperiments ::= procList
        }
      }
      outstandingExperiments = remainingExperiments
    }

    d.replyToOffer(oid, tasks, Map[String,String]())
  }

  override def statusUpdate(d: SchedulerDriver, status: TaskStatus): Unit = {
    if(status.getState == TaskState.TASK_FAILED || status.getState == TaskState.TASK_LOST) {
      logger.warning("Status Update for Task %d: %s", status.getTaskId, status.getState)
      logger.ifWarning(new String(status.getData))
    }
    else {
      logger.info("Status Update: " + status.getTaskId + " " + status.getState)
      logger.ifDebug(new String(status.getData))
    }
  }

  def stop = driver.stop
}
