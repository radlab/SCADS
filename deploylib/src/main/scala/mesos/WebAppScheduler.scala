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
class WebAppScheduler protected (name: String, mesosMaster: String, executor: String, warFile: ClassSource, properties: Map[String, String], serverCapacity: Int, statsServer: Option[String] = None) extends Scheduler {
  val logger = Logger()
  var driver = new MesosSchedulerDriver(this, mesosMaster)
  val driverThread = new Thread("ExperimentScheduler Mesos Driver Thread") { override def run(): Unit = driver.run() }
  driverThread.start()

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

  var runMonitorThread = true
  var numToKill = 0
  var killTimer = 0
  @volatile var targetNumServers = 0
  var servers =  new HashMap[Int, String]()
  var minNumServers = 1
  val monitorThread = new Thread("StatsThread") {
    val httpClient = new HttpClient()
    override def run() = {
      while(runMonitorThread) {
        Thread.sleep(2000)

        // Calculate the average web server request rate
        val requestsPerSec = for(s <- servers.values) yield {
          val slaveUrl = "http://" + s + ":8080/stats"
          val method = new GetMethod(slaveUrl)
          logger.info("Getting status from %s.", slaveUrl)
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

        val aggregateReqRate = requestsPerSec.sum
        targetNumServers = math.max(minNumServers,math.ceil(aggregateReqRate / serverCapacity).toInt)

        statement.foreach(s => {
          val now = new Date
          val sqlInsertCmd = "INSERT INTO appReqRate (timestamp, webAppID, aggRequestRate, targetNumServers) VALUES (%d, '%s', %f, %d)".format(now.getTime, name, aggregateReqRate, targetNumServers)
          try {
            val numResults = s.map(_.executeUpdate(sqlInsertCmd))
            if (numResults.getOrElse(0) != 1)
              logger.warning("SQL INSERT statment failed.")
          } catch {
            case e: SQLException => logger.warning("SQL INSERT statement failed: %s.".format(sqlInsertCmd))
          }
        })

        logger.info("aggregateReqRate is " + aggregateReqRate + ", targetNumServers is " + targetNumServers)

        // if necessary, kill backends
        val numToKill = servers.size - targetNumServers
        if (numToKill > 0) {
          logger.info("Calling driver.kill() for %d backends.", numToKill)
          val toKill = servers.keys.take(numToKill)
          toKill.map(driver.killTask(_))
        }
      }
    }
  }
  monitorThread.start()

  override def getFrameworkName(d: SchedulerDriver): String = ": " + name
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = new ExecutorInfo(executor, Array[Byte]())
  override def registered(d: SchedulerDriver, fid: String): Unit = logger.info("Registered Framework.  Fid: " + fid)

  val webAppTask = JvmWebAppTask(warFile, properties)
  var numAppServers = 0

  override def resourceOffer(driver: SchedulerDriver, oid: String, offers: java.util.List[SlaveOffer]): Unit = {

    val tasks = offers.flatMap(offer => {
      logger.info("In resourceOffer, numAppServers is " + numAppServers + ", targetNumServers is " + targetNumServers)
      if(numAppServers < targetNumServers) {
        val taskParams = Map(List("mem", "cpus").map(k => k -> offer.getParams.get(k)):_*)

        servers += ((numAppServers, offer.getHost()))
        numAppServers += 1
        new TaskDescription(numAppServers, offer.getSlaveId, webAppTask.toString, taskParams, JvmTask(webAppTask)) :: Nil
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

  def kill = {
    driver.stop
    runMonitorThread = false
  }

}
