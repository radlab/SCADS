package edu.berkeley.cs.scads.director

import deploylib.ec2._
import deploylib.mesos._

import _root_.mesos._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage.{ScadsCluster, ScalaEngineTask}

import java.io.File
import net.lag.logging.Logger

import scala.collection.mutable.{ Buffer, ListBuffer }
import scala.collection.JavaConversions._

/**
 * Factory for when running scheduler
 */
object ScadsServerScheduler {
  System.loadLibrary("mesos")

  def apply(name: String, mesosMaster: String, zookeep: String)(implicit classsource: Seq[ClassSource]) = new ScadsServerScheduler(name, mesosMaster, zookeep)
  //def apply(name: String, mesosMaster: String = "1@" + java.net.InetAddress.getLocalHost.getHostAddress + ":5050",mesosMaster: String = "1@" + java.net.InetAddress.getLocalHost.getHostAddress + ":5050") = new ScadsServerScheduler(name, mesosMaster,zookeep)
}

class ScadsServerScheduler protected (name: String, mesosMaster: String, zookeeperCanonical: String)(implicit val classsource: Seq[ClassSource]) extends Scheduler {
  val logger = Logger("scheduler")
  var taskId = 0
  var driver = new MesosSchedulerDriver(this, mesosMaster/*,existing_framework_id*/)
  val cluster = new ScadsCluster(ZooKeeperNode(zookeeperCanonical))

  //   val serverJvmProcess = 
  // JvmProcess(List(ServerSideJar("/mnt/director-2.1.0-SNAPSHOT-jar-with-dependencies.jar")),
  // "edu.berkeley.cs.scads.storage.ScalaEngine","--clusterAddress" :: zookeeperCanonical :: Nil)

  var serversToAdd = new java.util.concurrent.ConcurrentLinkedQueue[JvmMainTask]()
  var taskIds = List[Int]() // pending tasks
  var scheduledServers = List[Int]() // list of taskids

  val driverThread = new Thread("ScadsServerScheduler Mesos Driver Thread") { override def run(): Unit = driver.run() }
  driverThread.start()

  def serverProcess(name: Option[String]): JvmMainTask =
    ScalaEngineTask(clusterAddress= zookeeperCanonical,
		    name = name).toJvmTask

  def addServers(num: Int): Unit = synchronized { (0 until num).toList.foreach(s => serversToAdd.add(serverProcess(None))) }
  def addServers(servers: Iterable[String]): Unit = synchronized { servers.foreach(s => serversToAdd.add(serverProcess(Some(s)))) }
  def stopDriver = driver.stop

  override def getFrameworkName(d: SchedulerDriver): String = "SCADS Service Framework: " + name
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = new ExecutorInfo("/usr/local/mesos/frameworks/deploylib/java_executor", Array[Byte]())
  override def registered(d: SchedulerDriver, fid: String): Unit = logger.info("Registered SCADS Framework.  Fid: " + fid)

  override def resourceOffer(d: SchedulerDriver, oid: String, offers: java.util.List[SlaveOffer]) = {
    val tasks = new java.util.LinkedList[TaskDescription]

    while (offers.size > 0 && serversToAdd.peek() != null) {
      val scheduleNow = (0 until offers.size).toList.map(_ => serversToAdd.poll).filter(_ != null)
      scheduleNow.foreach(proc => {
        val offer = offers.remove(0)
        val taskParams = Map(List("mem", "cpus").map(k => k -> offer.getParams.get(k)): _*)
        val task = new TaskDescription(taskId, offer.getSlaveId, proc.mainclass, taskParams, JvmTask(proc))
        logger.debug("Scheduling task %d: %s", taskId, proc)
        scheduledServers ::= taskId
        logger.info("Assigning task %d to slave %s on %s", taskId, offer.getSlaveId, offer.getHost)
        taskId += 1
        tasks.add(task)
      })
    }
    d.replyToOffer(oid, tasks, Map[String, String]())
  }

  override def statusUpdate(d: SchedulerDriver, status: TaskStatus): Unit = {
    if (status.getState == TaskState.TASK_FAILED || status.getState == TaskState.TASK_LOST) {
      logger.warning("Status Update for Task %d: %s", status.getTaskId, status.getState)
      logger.ifWarning(new String(status.getData))

      synchronized {
        val failedServers = scheduledServers.filter(_ equals status.getTaskId)
        if (failedServers.size > 0) {
          failedServers.foreach(d.killTask(_))
          logger.info("Killing Failed Server task: %s", failedServers)
          scheduledServers = scheduledServers.filterNot(_ equals status.getTaskId)
        } else logger.debug("Failed to locate servers, can't kill stranded process")
      }
    } else {
      logger.info("Status Update: " + status.getTaskId + " " + status.getState)
      logger.ifDebug(new String(status.getData))
    }
  }

}
