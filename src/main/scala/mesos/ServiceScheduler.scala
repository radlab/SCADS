package deploylib
package mesos

import _root_.mesos._
import java.io.File
import net.lag.logging.Logger

import scala.collection.JavaConversions._

object ServiceScheduler {
  System.loadLibrary("mesos")

  def apply(name: String, mesosMaster: String = "1@" + java.net.InetAddress.getLocalHost.getHostAddress + ":5050") = new ServiceScheduler(name, mesosMaster)
}

class ServiceScheduler protected (name: String, mesosMaster: String) extends Scheduler {
  /* Self reference for use in the driver thread */
  self =>

  val logger = Logger()
  var taskId = 0

  val thread = new Thread() {
    override def run(): Unit = new MesosSchedulerDriver(self, mesosMaster).run();
  }
  thread.start()

  case class ServiceDescription(mem: Int, cpus: Int, desc: JvmProcess)
  var outstandingTasks = List[ServiceDescription]()

  override def getFrameworkName(d: SchedulerDriver): String = "SCADS Service Framework: " + name
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = new ExecutorInfo("/root/java_executor", Array[Byte]())
  override def registered(d: SchedulerDriver, fid: String): Unit = logger.info("Registered SCADS Framework.  Fid: " + fid)

  override def resourceOffer(d: SchedulerDriver, oid: String, offers: java.util.List[SlaveOffer]) = {
    val tasks = new java.util.LinkedList[TaskDescription]
    val taskParams = new java.util.TreeMap[String, String]

    synchronized {
     outstandingTasks = outstandingTasks.filter(task => {
        val slotIndex = offers.findIndexOf(o => o.getParams.get("cpus").toInt >= task.cpus && o.getParams.get("mem").toInt >= task.mem)

        if(slotIndex != -1) {
          val offer = offers.get(slotIndex)
          offers.remove(slotIndex)
          tasks.add(new TaskDescription(taskId, offer.getSlaveId(), task.desc.mainclass, Map("mem" -> task.mem.toString, "cpus" -> task.cpus.toString), task.desc.toBytes))
          taskId += 1
          false
        }
        else {
          logger.info("Unable to schedule: " + task)
          true
        }
      })
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

  def runService(mem: Int, cores: Int, desc: JvmProcess): Unit = synchronized {
    logger.debug("Running Service %s with %d cores and %dMb RAM", desc, cores, mem)
    outstandingTasks ::= ServiceDescription(mem, cores, desc)
  }
}
