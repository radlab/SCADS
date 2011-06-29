package deploylib
package mesos

import org.apache.mesos._
import org.apache.mesos.Protos._
import com.google.protobuf.ByteString

import edu.berkeley.cs.scads.comm._

import java.io.File
import net.lag.logging.Logger

import scala.collection.mutable.{Buffer, ListBuffer}
import scala.collection.JavaConversions._

object LocalExperimentScheduler {
  System.loadLibrary("mesos")

  def apply(name: String, mesosMaster: String = "1@" + java.net.InetAddress.getLocalHost.getHostAddress + ":5050", executor: String = "/usr/local/mesos/frameworks/deploylib/java_executor") =
    new LocalExperimentScheduler(name, mesosMaster, executor)
}

abstract trait ExperimentScheduler {
  def scheduleExperiment(processes: Seq[JvmTask]): Unit
}

class LocalExperimentScheduler protected (name: String, mesosMaster: String, executor: String) extends Scheduler with ExperimentScheduler {
  val logger = Logger()
  var taskId = 0
  var driver = new MesosSchedulerDriver(this, mesosMaster)

  case class Experiment(var processes: Seq[JvmTask])
  var outstandingExperiments = new java.util.concurrent.ConcurrentLinkedQueue[Experiment]
  var awaitingSiblings = List[JvmTask]()
  var taskIds = List[TaskID]()
  var scheduledExperiments = List[List[TaskID]]()

  val driverThread = new Thread("ExperimentScheduler Mesos Driver Thread") { override def run(): Unit = driver.run() }
  driverThread.start()

  def scheduleExperiment(processes: Seq[JvmTask]): Unit = synchronized {
    outstandingExperiments.add(new Experiment(processes))
  }

  override def getFrameworkName(d: SchedulerDriver): String = "SCADS Service Framework: " + name
  val executorId = ExecutorID.newBuilder().setValue("javaExecutor")
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = ExecutorInfo.newBuilder().setUri(executor).setExecutorId(executorId).build()
  override def registered(d: SchedulerDriver, fid: FrameworkID): Unit = logger.info("Registered SCADS Framework.  Fid: " + fid)

  protected def taskDescription(task: JvmTask): String = task match {
    case j:JvmMainTask => j.mainclass + " " + j.args
    case j:JvmWebAppTask => j.warFile.toString
  }

  override def resourceOffer(d: SchedulerDriver, oid: OfferID, offers: java.util.List[SlaveOffer]) = awaitingSiblings.synchronized {
    val tasks = new java.util.LinkedList[TaskDescription]

    while(offers.size > 0 && outstandingExperiments.peek() != null) {
      val currentExperiment = outstandingExperiments.peek()
      val scheduleNow = currentExperiment.processes.take(offers.size)
      scheduleNow.take(offers.size).foreach(proc => {
        val offer = offers.remove(0)
	      val taskDesc = taskDescription(proc)
        val curId = TaskID.newBuilder().setValue(taskId.toString).build
        val task = TaskDescription.newBuilder()
                                  .setTaskId(curId)
                                  .setSlaveId(offer.getSlaveId)
                                  .addAllResources(offer.getResourcesList)
                                  .setName(taskDesc)
                                  .setData(ByteString.copyFrom(JvmTask(proc)))
                                  .build()

        logger.info("Scheduling task %d: %s", taskId, taskDesc)
        taskIds ::= curId
        logger.info("Assigning task %d to slave %s on %s", taskId, offer.getSlaveId, offer.getHostname)
        taskId += 1
        tasks.add(task)

        awaitingSiblings ::= proc
      })

      currentExperiment.processes = currentExperiment.processes.drop(scheduleNow.size)
      if(currentExperiment.processes.size == 0) {
        outstandingExperiments.poll()
        logger.info("Experiment Scheduled. Size: %d", awaitingSiblings.size)
        scheduledExperiments ::= taskIds
        taskIds = List[TaskID]()
        awaitingSiblings = List[JvmTask]()
      }
      else {
        logger.info("Scheduled %d of %d processes", awaitingSiblings.size, awaitingSiblings.size + currentExperiment.processes.size)
      }
    }

    d.replyToOffer(oid, tasks, Map[String,String]())
  }

  override def statusUpdate(d: SchedulerDriver, status: TaskStatus): Unit = {
	if(status == null) {
      logger.warning("Null status.")
	}
    else if(status.getState == TaskState.TASK_FAILED) {
      logger.warning("Status Update for Task %d: %s", status.getTaskId, status.getState)
      logger.ifWarning(new String(status.getData.toByteArray))

      val siblings = scheduledExperiments.find(_ contains status.getTaskId).getOrElse {
        logger.debug("Failed to locate siblings for task %d, can't kill stranded processes", status.getTaskId)
        return
      }
      siblings.foreach(d.killTask)
      logger.info("Killing Failed Experiment Siblings: %s", siblings)
      scheduledExperiments = scheduledExperiments.filterNot(_ equals siblings)
    }
    else {
      logger.debug("Status Update: " + status.getTaskId + " " + status.getState)
      logger.ifDebug(new String(status.getData.toByteArray))
    }
  }

  def stopDriver = driver.stop

  override def error(driver: SchedulerDriver, code: Int, message: String): Unit = logger.fatal("MESOS ERROR %d: %s", code, message)
  override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID): Unit = logger.fatal("SLAVE LOST: %s", slaveId)
  override def frameworkMessage(driver: SchedulerDriver, slaveId: SlaveID, executor: ExecutorID, data: Array[Byte]): Unit = logger.info("FW Message: %s %s %s", slaveId, executor, new String(data))
  override def offerRescinded(driver: SchedulerDriver, oid: OfferID): Unit = logger.fatal("Offer Rescinded: %s", oid)
}
