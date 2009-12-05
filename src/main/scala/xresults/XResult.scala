package deploylib.xresults

import org.xmldb.api.base._
import org.xmldb.api.modules._
import org.xmldb.api._
import org.exist.xmldb.DatabaseInstanceManager
import org.apache.log4j.Logger
import java.net.InetAddress
import scala.xml.{UnprefixedAttribute, Elem, Node, Null, Text, TopScope }


object XResult {
  val logger = Logger.getLogger("deploylib.xresult")
  lazy val collection = getCollection()
  val hostname = localHostname()

  def experimentId(): String = System.getProperty("experimentId")

  def startExperiment():Unit = {
    if(experimentId != null)
      logger.warn("Experiment: " + experimentId + " is already running.  Starting a new one anyway.")
    System.setProperty("experimentId", System.getProperty("user.name") + System.currentTimeMillis())
    logger.info("Begining experiment: " + experimentId)
  }

  def storeXml(elem: Elem):Unit = {
    if(experimentId == null) {
      logger.warn("No experiment running, logging results to console.")
      logger.warn(elem)
    }
    else {
      val doc = collection.createResource(null, "XMLResource").asInstanceOf[XMLResource]
      doc.setContent(elem % new UnprefixedAttribute("experimentId", experimentId, Null))
      logger.debug("Storing result: " + doc.getId)
      collection.storeResource(doc)
    }
  }

  def recordResult(result: Elem): Unit = {
    storeXml(<result hostname={hostname}>{timestamp}{result}</result>)
  }

  def timestamp = <timestamp unit="milliseconds">{System.currentTimeMillis().toString}</timestamp>

  def benchmark(func: => Elem): Elem = {
    val startTime = System.currentTimeMillis()
    val result = func
    val endTime = System.currentTimeMillis()
      <benchmark unit="miliseconds"><startTime>{startTime.toString()}</startTime><endTime>{endTime.toString()}</endTime>{result}</benchmark>
  }

  protected def localHostname(): String = {
    InetAddress.getLocalHost().getHostName()
  }

  protected def getCollection():Collection = {
    val cl = Class.forName("org.exist.xmldb.DatabaseImpl")
    val database = cl.newInstance().asInstanceOf[Database]
    database.setProperty("create-database", "true")
    DatabaseManager.registerDatabase(database)
    DatabaseManager.getCollection("xmldb:exist://scm.millennium.berkeley.edu:8080/exist/xmlrpc/db/results", "experiment", "scads")
  }
}
