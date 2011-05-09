package deploylib
package mesos

import java.net.URL
import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import java.util.Date

import org.apache.mesos._
import org.apache.mesos.Protos._
import com.google.protobuf.ByteString


import edu.berkeley.cs.scads.comm._
import net.lag.logging.Logger
import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._
import org.apache.commons.httpclient.params._

object WebAppScheduler {
  System.loadLibrary("mesos")
}

/* serverCapacity is the number of requests per second that a single application server can handle */
class WebAppScheduler protected(name: String, mesosMaster: String, executor: String, warFile: ClassSource, properties: Map[String, String], zkWebServerListRoot: String, serverCapacity: Int, var minServers: Int, statsServer: Option[String] = None) extends Scheduler {
  scheduler =>

  val logger = Logger()
  var listeners: List[String => Unit] = Nil
  var driver = new MesosSchedulerDriver(this, mesosMaster)
  val driverThread = new Thread("ExperimentScheduler Mesos Driver Thread") {
    override def run(): Unit = driver.run()
  }

  //set up mysql connection for statistics
  val statement = statsServer.map(connString => {
    try {
      val conn = DriverManager.getConnection(connString)
      Some(conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE))
    } catch {
      case e: SQLException => logger.warning("Connection to SQL Database failed with connection string %s.".format(connString))
      None
    }
  })

  def registerActionListener(func: String => Unit): Unit = {
    listeners ::= func
  }

  protected def recordAction(action: String): Unit = listeners.foreach(_(action))

  val port = 8080
  val zkWebServerList = ZooKeeperNode(zkWebServerListRoot)
  var runMonitorThread = true
  var numToKill = 0
  var killTimer = 0
  @volatile var targetNumServers: Int = minServers
  var smoothedWorkload = 0.0
  // The smoothed version of the aggregate workload
  var smoothedUtilization = 0.0
  var servers = new HashMap[TaskID, String]()
  var pendingServers = new HashMap[TaskID, String]()
  var unresponsiveServers = new HashMap[TaskID, Int]()

  /**
   * Periodically query all of the webapp servers currently thought
   * to be alive (i.e. in the servers HashMap), calculate the aggregate
   * request rate to the application, and then derive the target number
   * of webapp servers necessary to satisfy that request rate. Kill some
   * webapp server mesos tasks if the app is currently over provisioned.
   * If more are needed, they will be acquired by resourceOffer().
   *
   * @param period - frequency with which targetNumServers recalculated
   * @param unresponsiveRequestLimit - Wait to kill a webapp server until you have failed to retrieve its request rate this many times.
   * @param rampUpWorkloadWeight - Smooth adding webapp servers by weighing in history. Must be geq 0.0 and lt 1.0.
   * @param rampDownWorkloadWeight - Smooth killing webapp servers by weighing in history. Must be geq 0.0 and lt 1.0.
   */
  class monitorThread(period: Int = 1000 * 30,
                      unresponsiveRequestLimit: Int = 3,
                      rampUpWorkloadWeight: Double = 0.9,
                      rampDownWorkloadWeight: Double = 0.01,
                      rampUpUtilizationWeight: Double = 0.9,
                      rampDownUtilizationWeight: Double = 0.05,
                      utilizationThreshold: Double = 0.40
                       ) extends Runnable {
    private val params = new HttpClientParams()
    params.setSoTimeout(5000)
    private val httpClient = new HttpClient(params)

    override def run() = {
      while (runMonitorThread) {
        // Calculate the average web server request rate
        logger.info("Beginging collection of workload statistics")
        val stats = for ((taskID, slaveHostname) <- servers.toSeq) yield {
          val slaveUrl = "http://%s:%d/stats/".format(slaveHostname, port)
          val method = new GetMethod(slaveUrl)
          logger.debug("Getting status from %s.", slaveUrl)
          try {
            httpClient.executeMethod(method)
            val xml = scala.xml.XML.loadString(method.getResponseBodyAsString)
            unresponsiveServers -= taskID
            ((xml \ "RequestRate").text.toDouble, (xml \ "CpuUtilization").text.toDouble)
          } catch {
            case e =>
              unresponsiveServers += taskID -> (unresponsiveServers.get(taskID).getOrElse(0) + 1)
              logger.warning("Couldn't get RequestRate from %s for %d iterations..", slaveUrl, unresponsiveServers(taskID))
              if (unresponsiveServers(taskID) > unresponsiveRequestLimit) {
                driver.killTask(taskID)
                servers -= taskID
                recordAction("Webapp Director killed an unresponsive webapp server of %s's. Weakness will not be tolerated!".format(name))
              }
              (0.0, 0.0)
          }
        }

        logger.info("Current Workload: %s", stats)
        val aggregateReqRate = stats.map(_._1).sum
        val totalUtilization = stats.map(_._2).sum
        val averageUtilization = totalUtilization / servers.size

        /*
          if (aggregateReqRate > smoothedWorkload) { // ramping up
            smoothedWorkload = smoothedWorkload + (aggregateReqRate - smoothedWorkload) * rampUpWorkloadWeight
          } else { // ramping down
            smoothedWorkload = smoothedWorkload + (aggregateReqRate - smoothedWorkload) * rampDownWorkloadWeight
          }
          targetNumServers = math.max(minServers, math.ceil(smoothedWorkload / serverCapacity).toInt)
    */

        if (totalUtilization > smoothedUtilization) {
          smoothedUtilization = smoothedUtilization + (totalUtilization - smoothedUtilization) * rampUpUtilizationWeight
        } else {
          // ramping down
          smoothedUtilization = smoothedUtilization + (totalUtilization - smoothedUtilization) * rampDownUtilizationWeight
        }
        targetNumServers = math.max(minServers, math.ceil(smoothedUtilization / utilizationThreshold).toInt)

        logger.info("Current Aggregate Workload: %f req/sec (%f smoothed), targetServers=%d", aggregateReqRate, smoothedWorkload, targetNumServers)

        //TODO: Error Handling
        statement.foreach(s => {
          val now = new Date
          val sqlInsertCmd = "INSERT INTO appReqRate (timestamp, webAppID, aggRequestRate, targetNumServers, actualNumServers, averageUtilization)" +
            "VALUES (%d, '%s', %f, %d, %d, %f)".format(now.getTime, name, aggregateReqRate, targetNumServers.toInt, servers.size, averageUtilization)
          try {
            val numResults = s.map(_.executeUpdate(sqlInsertCmd))
            if (numResults.getOrElse(0) != 1)
              logger.warning("SQL INSERT statment failed.")
          } catch {
            case e: SQLException => logger.warning(e, "SQL INSERT statement failed: %s.".format(sqlInsertCmd))
          }
        })

        logger.debug("aggregateReqRate is " + aggregateReqRate + ", targetNumServers is " + targetNumServers)

        // if necessary, kill backends
        val numToKill = servers.size - targetNumServers.ceil.toInt
        if (numToKill > 0) {
          killTasks(numToKill)
        }

        Thread.sleep(period)
      }
    }
  }

  def killTasks(numServers: Int) = {
    logger.info("Calling driver.kill() for %d webservers.", numToKill)
    val toKill = servers.take(numToKill)
    toKill.map {
      case (victimID, victimHost) => {
        logger.info("Killing task %d on node %s.", victimID, victimHost)
        driver.killTask(victimID)
        logger.info("Removing webserver task " + victimID + " on host " + victimHost + " from hashmap of servers.")
        recordAction("Webapp Director killed an unecessary webapp server of %s's. Inefficiencies will not be tolerated!".format(name))
        servers -= victimID
      }
    }
    updateZooWebServerList();
  }

  val monitoringThread = new Thread(new monitorThread(), "WebAppScheduler Monitoring Thread")
  monitoringThread.start()
  driverThread.start()

  /**
   * Periodically sends an http GET request to a server until it responds
   * with a status 200 or until a timeout. If the timeout is reached, kills
   * the mesos task so that it doesn't become a zombie task.
   *
   * @param taskId - the mesos taskId associated with this web app server.
   * @param serverHostname - the hostname of the web app server that was started.
   * @param sleepPeriod - how often, in ms, to check for a successful server response. Default is 10 seconds.
   * @param timeout - how long, in ms, to spend checking for an http 200 status response before giving up and killing the mesos task associated with the server. Default is 5 minutes.
   */
  class WebAppPrimerThread(taskId: TaskID, serverHostname: String, numSuccessRequired: Int = 100, sleepPeriod: Int = 10 * 1000, timeout: Int = 5 * 60 * 1000) extends Runnable {
    val httpClient = new HttpClient()

    override def run() = {
      var timeoutCountdown = timeout
      var numSuccesses = 0
      var serverIsUp = false
      var exitThread = false
      while (!exitThread) {
        Thread.sleep(sleepPeriod)
        timeoutCountdown -= sleepPeriod
        if (timeoutCountdown <= 0) {
          logger.warning("The webserver on host %s never came up, killing the mesos task associated with it now.", serverHostname)
          driver.killTask(taskId)
          pendingServers -= taskId
          recordAction("The webserver for app %s killed webapp server task on host %s because it never responded, it was weak and deserved to be killed.".format(name, taskId))
          exitThread = true
        } else {
          val appUrl = "http://%s:%d/".format(serverHostname, port)
          val method = new GetMethod(appUrl)
          do {
            logger.info("Sending a priming HTTP request to %s.", appUrl)
            try {
              val status = httpClient.executeMethod(method)
              if (status == 200) {
                serverIsUp = true
                logger.info("Server %s contacted successfully", appUrl)
                numSuccesses += 1
                if (numSuccesses >= numSuccessRequired) {
                  exitThread = true
                  scheduler.synchronized {
                    servers += taskId -> serverHostname
                    pendingServers -= taskId
                  }
                  updateZooWebServerList()
                  recordAction("Webapp Director added a webapp server for %s. Good luck young server!".format(name))
                }
              }
            } catch {
              case e =>
                logger.info("priming GET request to %s failed.", appUrl)
            }
          } while (serverIsUp && !exitThread)
        }
      }
    }
  }

  override def getFrameworkName(d: SchedulerDriver): String = "WebAppScheduler: " + name

  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = ExecutorInfo.newBuilder.setUri(executor).build()

  override def registered(d: SchedulerDriver, fid: FrameworkID): Unit = logger.info("Registered Framework.  Fid: " + fid)

  val webAppTask = JvmWebAppTask(warFile, properties)
  var taskIdCounter = 0

  override def resourceOffer(driver: SchedulerDriver, oid: OfferID, offers: java.util.List[SlaveOffer]): Unit = {

    val tasks = offers.flatMap(offer => {
      logger.debug("In resourceOffer, taskIdCounter is " + taskIdCounter + ", targetNumServers is " + targetNumServers)
      logger.debug("servers.size is %d.", servers.size)
      logger.debug("pendingServers.size is %d.", pendingServers.size)
      if (servers.size + pendingServers.size < targetNumServers) {
        logger.info("adding server %s as task %d", offer.getHostname, taskIdCounter)
        val newTaskId = TaskID.newBuilder.setValue(taskIdCounter.toString).build()
        taskIdCounter += 1

        scheduler.synchronized {
          pendingServers += newTaskId -> offer.getHostname
        }

        new Thread(new WebAppPrimerThread(newTaskId, offer.getHostname)).start()

        var td = TaskDescription.newBuilder
                                .setTaskId(newTaskId)
                                .setSlaveId(offer.getSlaveId)
                                .setName(webAppTask.toString)
                                .addAllResources(offer.getResourcesList)
                                .setData(ByteString.copyFrom(JvmTask(webAppTask)))
                                .build() :: Nil
        td
      }
      else
        Nil
    })

    driver.replyToOffer(oid, tasks, Map[String, String]())
  }


  override def statusUpdate(d: SchedulerDriver, status: TaskStatus): Unit = {
    if (status.getState == TaskState.TASK_FAILED || status.getState == TaskState.TASK_LOST || status.getState == TaskState.TASK_KILLED) {
      logger.warning("Status Update for Task %d: %s", status.getTaskId, status.getState)
      //TODO: restarted failed app servers
    }
    else {
      logger.debug("Status Update: " + status.getTaskId + " " + status.getState)
      logger.ifDebug(new String(status.getData.toByteArray))
    }
  }

  def updateZooWebServerList() = {
    var serverList = servers.values.mkString("\n")
    logger.debug("Updating zookeeper list of webservers to %s", serverList)
    zkWebServerList.data = serverList.getBytes
  }

  def kill = {
    driver.stop
    runMonitorThread = false
  }

  override def error(driver: SchedulerDriver, code: Int, message: String): Unit = logger.fatal("MESOS ERROR %d: %s", code, message)
  override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID): Unit = logger.fatal("SLAVE LOST: %s", slaveId)
  override def frameworkMessage(driver: SchedulerDriver, slaveId: SlaveID, executor: ExecutorID, data: Array[Byte]): Unit = logger.info("FW Message: %s %s %s", slaveId, executor, new String(data))
  override def offerRescinded(driver: SchedulerDriver, oid: OfferID): Unit = logger.fatal("Offer Rescinded: %s", oid)
}
