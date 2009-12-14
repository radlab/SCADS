package deploylib.xresults

import org.xmldb.api.base._
import org.xmldb.api.modules._
import org.xmldb.api._
import org.exist.xmldb.DatabaseInstanceManager
import org.apache.log4j.Logger
import java.net.InetAddress
import scala.xml.{NodeSeq, UnprefixedAttribute, Elem, Node, Null, Text, TopScope }
import java.io.File


object XResult {
  val logger = Logger.getLogger("deploylib.xresult")
  lazy val collection = getCollection()
	lazy val queryService = collection.getService("XPathQueryService", "1.0").asInstanceOf[XPathQueryService]
  val hostname = localHostname()

  def experimentId(): String = System.getProperty("experimentId")


	def startExperiment(description: String):Unit = startExperiment(<description>{description}</description>)
  def startExperiment(experimentData: NodeSeq):Unit = {
    if(experimentId != null)
      logger.warn("Experiment: " + experimentId + " is already running.  Starting a new one anyway.")
    System.setProperty("experimentId", System.getProperty("user.name") + System.currentTimeMillis())
    logger.info("Begining experiment: " + experimentId)
    storeXml(
      <experiment user={System.getProperty("user.name")}>
				{experimentData}
      </experiment>)
  }

	def storeUnrelatedXml(elem: Elem): Unit = {
		val doc = collection.createResource(null, "XMLResource").asInstanceOf[XMLResource]
		doc.setContent(elem)
		logger.debug("Storing result: " + doc.getId)
		collection.storeResource(doc)
	}

  def storeXml(elem: Elem):Unit = {
    if(experimentId == null) {
      logger.warn("No experiment running, logging results to console.")
      logger.warn(elem)
    }
    else {
      val taggedResult = new Elem(
        elem.prefix,
        elem.label,
        elem.attributes.append(new UnprefixedAttribute("experimentId", experimentId, Null)).append(new UnprefixedAttribute("timestamp", System.currentTimeMillis().toString, Null)),
        elem.scope,
        elem.child:_*)
      storeUnrelatedXml(taggedResult)
    }
  }

  def recordResult(result: NodeSeq): Unit = {
    storeXml(<result hostname={hostname}>{result}</result>)
  }

  def benchmark(func: => Elem): Elem = {
    val startTime = System.currentTimeMillis()
    val result = func
    val endTime = System.currentTimeMillis()
      <benchmark type="open" unit="miliseconds" startTime={startTime.toString()} endTime={endTime.toString()}>
				{result}
			</benchmark>
  }

	def timeLimitBenchmark(seconds: Int, iterationsPerCheck: Int, data: Elem)(func: => Boolean): Elem = {
		val startTime = System.currentTimeMillis()
		var lastTime = startTime
		var endTime = startTime
		val hist = new Histogram(50, 100)
		var totalIterations = 1
		var success = 0
		var failure = 0

		while((endTime - startTime) / 1000 < seconds) {
			if(func)
				success += 1
			else
				failure += 1

			totalIterations += 1

			if(totalIterations % iterationsPerCheck == 0) {
				endTime = System.currentTimeMillis()
				hist.add((endTime - lastTime).toInt)
				lastTime = endTime
			}
		}

		<benchmark type="timeLimited" startTime={startTime.toString()} endTime={endTime.toString()} iterations={totalIterations.toString} successfulIteration={success.toString} failedIterations={failure.toString} checkInterval={iterationsPerCheck.toString}>
			{hist.toXml}
			{data}
		</benchmark>
	}

	def recordException[ReturnType](func: => ReturnType): ReturnType = {
		try {
			func
		}
		catch {
			case e: Exception => {
				storeXml(
					<exception hostname={hostname} name={e.toString}>
						{e.getStackTrace.map(l => <line>{l}</line>)}
					</exception>)
				logger.debug("Exception stored to result database")
				throw e
			}
		}
	}

	def captureDirectory(target: RemoteMachine, directory: File): Unit = {
		val files = target.ls(directory).map(r => {
				<file name={r.name} owner={r.owner} permissions={r.permissions} modDate={r.modDate} size={r.size}/>
				})
		storeXml(
				<directory host={target.hostname} path={directory.toString}>
					{files}
				</directory>)
	}

	def execQuery(query: String): Iterator[Elem] = {
		class ResultIterator(ri: ResourceIterator) extends Iterator[Elem] {
			def hasNext: Boolean = ri.hasMoreResources()
			def next: Elem = scala.xml.XML.loadString(ri.nextResource().getContent().asInstanceOf[String])
		}
		new ResultIterator(queryService.query(query).getIterator)
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
