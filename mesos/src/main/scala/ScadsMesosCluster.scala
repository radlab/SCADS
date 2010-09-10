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

  def apply(initialSize: Int = 1)(implicit zooRoot: ZooKeeperProxy#ZooKeeperNode): ScadsMesosCluster = {
    new ScadsMesosCluster(zooRoot.createChild("scads-mesos", mode = CreateMode.PERSISTENT_SEQUENTIAL), initialSize)
  }
}

class ServiceScheduler(name: String) extends Scheduler {
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

  case class ServiceDescription(mem: Int, cpus: Int, desc: JvmProcess)
  var outstandingTasks = List[ServiceDescription]()

  override def getFrameworkName(d: SchedulerDriver): String = "SCADS Service Framework: " + name
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = new ExecutorInfo("/work/marmbrus/mesos/scads_executor", Array[Byte]())
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
    logger.info("Status Update: " + status.getTaskId + " " + status.getState)
    logger.debug(new String(status.getData))
  }

  def runService(mem: Int, cores: Int, desc: JvmProcess): Unit = synchronized {
    outstandingTasks ::= ServiceDescription(mem, cores, desc)
  }
}

class ScadsMesosCluster(val root: ZooKeeperProxy#ZooKeeperNode, initialSize: Int) extends ScadsClusterManager {
  val scheduler = new ServiceScheduler("ScadsCluster " + root)
  val procDesc = JvmProcess("/work/marmbrus/mesos/mesos-scads-2.1.0-SNAPSHOT-jar-with-dependencies.jar", "edu.berkeley.cs.scads.storage.ScalaEngine", "--zooKeeper" :: "169.229.48.70:2181" :: "--zooBase" :: root.name :: "--verbose" :: Nil )

  (1 to initialSize).foreach(i => scheduler.runService(2048, 3, procDesc))
}
