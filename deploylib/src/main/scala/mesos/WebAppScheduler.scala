package deploylib
package mesos

import java.net.URL
import java.sql.{Connection, DriverManager, ResultSet, SQLException}
import java.util.Date

import _root_.mesos._
import edu.berkeley.cs.scads.comm._
import net.lag.logging.Logger
import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

import org.apache.commons.httpclient._
import org.apache.commons.httpclient.methods._

object WebAppScheduler {
  System.loadLibrary("mesos")
}

/* serverCapacity is the number of requests per second that a single application server can handle */
class WebAppScheduler protected (name: String, mesosMaster: String, executor: String, warFile: ClassSource, properties: Map[String, String], zkWebServerListRoot:String, serverCapacity: Int, var minServers: Int, statsServer: Option[String] = None) extends Scheduler {
  scheduler =>

  val logger = Logger()
  var driver = new MesosSchedulerDriver(this, mesosMaster)
  val driverThread = new Thread("ExperimentScheduler Mesos Driver Thread") { override def run(): Unit = driver.run() }

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

  val port = 8080
  val zkWebServerList = ZooKeeperNode(zkWebServerListRoot)
  var runMonitorThread = true
  var numToKill = 0
  var killTimer = 0
  @volatile var targetNumServers: Int = minServers
  val monitorThreadPeriod = 1000 * 30 // Recalculate targetNumServers every 10 seconds
  val rampUpWorkloadWeight = 0.9 //Smooth adding webapp servers by weighing in history. Must be >=0.0 and <1.0.
  val rampDownWorkloadWeight = 0.01 //Smooth killing webapp servers by weighing in history. Must be >=0.0 and <1.0.
  var smoothedWorkload = 0.0 // the smoothed version of the aggregate workload
  var servers =  new HashMap[Int, String]()
  var pendingServers =  new HashMap[Int, String]()
  val monitorThread = new Thread("StatsThread") {
    private val httpClient = new HttpClient()
    override def run() = {
      while(runMonitorThread) {
        // Calculate the average web server request rate
	logger.info("Beginging collection of workload statistics")
        val requestsPerSec = for(s <- servers.values) yield {
          val slaveUrl = "http://%s:%d/stats/".format(s, port)
          val method = new GetMethod(slaveUrl)
          logger.debug("Getting status from %s.", slaveUrl)
          try {
            httpClient.executeMethod(method)
            val xml = scala.xml.XML.loadString(method.getResponseBodyAsString)
            (xml \ "RequestRate").text.toFloat
          } catch {
            case e =>
              logger.warning("Couldn't get RequestRate from %s.", slaveUrl)
              0.0
          }
        }

        logger.info("Current Workload: %s", requestsPerSec)
        val aggregateReqRate = requestsPerSec.sum
        if (aggregateReqRate > smoothedWorkload) { // ramping up
          smoothedWorkload = smoothedWorkload + (aggregateReqRate - smoothedWorkload) * rampUpWorkloadWeight // newsmooth = oldsmooth + (newraw - oldsmooth)*alpha
        } else { // ramping down
          smoothedWorkload = smoothedWorkload + (aggregateReqRate - smoothedWorkload) * rampDownWorkloadWeight // newsmooth = oldsmooth + (newraw - oldsmooth)*alpha
        }
        targetNumServers = math.max(minServers, math.ceil(smoothedWorkload / serverCapacity).toInt)
        logger.info("Current Aggregate Workload: %f req/sec (%f smoothed), targetServers=%d", aggregateReqRate, smoothedWorkload, targetNumServers)

	//TODO: Error Handling
        statement.foreach(s => {
          val now = new Date
          val sqlInsertCmd = "INSERT INTO appReqRate (timestamp, webAppID, aggRequestRate, targetNumServers)" +
                             "VALUES (%d, '%s', %f, %d)".format(now.getTime, name, aggregateReqRate, targetNumServers.toInt)
          try {
            val numResults = s.map(_.executeUpdate(sqlInsertCmd))
            if (numResults.getOrElse(0) != 1)
              logger.warning("SQL INSERT statment failed.")
          } catch {
            case e: SQLException => logger.warning("SQL INSERT statement failed: %s.".format(sqlInsertCmd))
          }
        })

        logger.debug("aggregateReqRate is " + aggregateReqRate + ", targetNumServers is " + targetNumServers)

        // if necessary, kill backends
        val numToKill = servers.size - targetNumServers.ceil.toInt
        if (numToKill > 0) {
          logger.info("Calling driver.kill() for %d webservers.", numToKill)
          val toKill = servers.take(numToKill)
          toKill.map{case (victimID, victimHost) =>  {
            logger.info("Killing task on node " + victimHost)
            driver.killTask(victimID)
            logger.info("Removing webserver task " + victimID + " on host " + victimHost + " from hashmap of servers.")
            servers -= victimID
          }}
          updateZooWebServerList();
        }
	
	Thread.sleep(monitorThreadPeriod)
      }
    }
  }
  monitorThread.start()
  driverThread.start()

  /**
   * Periodically sends an http GET request to a server until it responds
   * with a status 200 or until a timeout. If the timeout is reached, kills
   * the mesos task so that it doesn't become a zombie task.
   * 
   * @taskId - the mesos taskId associated with this web app server.
   * @serverHostname - the hostname of the web app server that was started.
   * @sleepPeriod - how often, in ms, to check for a successful server response. Default is 10 seconds.
   * @timeout - how long, in ms, to spend checking for an http 200 status response before giving up and killing the mesos task associated with the server. Default is 5 minutes.
   */
  class WebAppPrimerThread(taskId: Int, serverHostname: String, sleepPeriod: Int = 10*1000, timeout: Int = 5*60*1000) extends Runnable {
    val httpClient = new HttpClient()
    override def run() = {
      var timeoutCountdown = timeout
      var exitThread = false
      while(!exitThread) {
        Thread.sleep(sleepPeriod)
        timeoutCountdown -= sleepPeriod
        if (timeoutCountdown < 0) {
          logger.warning("The webserver on host %s never came up, killing the mesos task associated with it now.")
          driver.killTask(taskId)
          pendingServers -= taskId
          exitThread = true
        } else {
          val appUrl = "http://%s:%d/".format(serverHostname, port)
          val method = new GetMethod(appUrl)
          logger.info("Sending a priming HTTP request to %s.", appUrl)
          try {
            val status = httpClient.executeMethod(method)
            if (status == 200) {
	      logger.info("Server %s contacted successfully", appUrl)
	      scheduler.synchronized {
		servers += taskId -> serverHostname
		pendingServers -= taskId
	      }
              updateZooWebServerList()
              exitThread = true
            }
          } catch {
            case e =>
              logger.info("priming GET request to %s failed.", appUrl)
              0.0
          }
        }
      }
    }
  }
  override def getFrameworkName(d: SchedulerDriver): String = "WebAppScheduler: " + name
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = new ExecutorInfo(executor, Array[Byte]())
  override def registered(d: SchedulerDriver, fid: String): Unit = logger.info("Registered Framework.  Fid: " + fid)

  val webAppTask = JvmWebAppTask(warFile, properties)
  var taskIdCounter = 0

  override def resourceOffer(driver: SchedulerDriver, oid: String, offers: java.util.List[SlaveOffer]): Unit = {

    val tasks = offers.flatMap(offer => {
      logger.info("In resourceOffer, taskIdCounter is " + taskIdCounter + ", targetNumServers is " + targetNumServers)
      logger.info("servers.size is %d.", servers.size)
      logger.info("pendingServers.size is %d.", pendingServers.size)
      if(servers.size + pendingServers.size < targetNumServers) {
        val taskParams = Map(List("mem", "cpus").map(k => k -> offer.getParams.get(k)):_*)

	scheduler.synchronized {
          pendingServers += taskIdCounter -> offer.getHost
	}
        new Thread(new WebAppPrimerThread(taskIdCounter,offer.getHost)).start()

        var td = new TaskDescription(taskIdCounter, offer.getSlaveId, webAppTask.toString, taskParams, JvmTask(webAppTask)) :: Nil
        taskIdCounter += 1
        td
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

  def updateZooWebServerList() = {
    var serverList = servers.values.mkString("\n")
    logger.debug("Updating zookeeper list of webservers to %s", serverList)
    zkWebServerList.data = serverList.getBytes
  }

  def kill = {
    driver.stop
    runMonitorThread = false
  }

}
