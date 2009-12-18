package deploylib.xresults

import java.net.InetAddress
import scala.xml.{NodeSeq, UnprefixedAttribute, Elem, Node, Null, Text, TopScope }
import java.io.File

object XResult extends XmlCollection("xmldb:exist://scm.millennium.berkeley.edu:8080/exist/xmlrpc/db/results", "experiment", "scads") {
  val hostname = localHostname()

  def experimentId(): String = System.getProperty("experimentId")

	def startExperiment(description: String):Unit = startExperiment(<description>{description}</description>)
  def startExperiment(experimentData: NodeSeq):Unit = {
    if(experimentId != null)
      logger.warn("Experiment: " + experimentId + " is already running.  Starting a new one anyway.")
    System.setProperty("experimentId", System.getProperty("user.name") + System.currentTimeMillis())
    logger.info("Begining experiment: " + experimentId)
    storeUnrelatedXml(experimentId,
      <experiment experimentId={experimentId} user={System.getProperty("user.name")} timestamp={System.currentTimeMillis.toString}>
				{experimentData}
      </experiment>)
  }


  def storeXml(elem: Elem):Unit = {
    if(experimentId == null) {
      logger.warn("No experiment running, logging results to console.")
      logger.warn(elem)
    }
    else {
			val updateCmd =
				<xupdate:modifications version="1.0" xmlns:xupdate="http://www.xmldb.org/xupdate">
					<xupdate:append select={"/experiment[@experimentId = '" + experimentId + "']"}>
     				<xupdate:element name={elem.label}>
							<xupdate:attribute name="timestamp">{System.currentTimeMillis}</xupdate:attribute>
							{elem.attributes.elements.map(a => <xupdate:attribute name={a.key}>{a.value}</xupdate:attribute>)}
							{elem.child}
						</xupdate:element>
					</xupdate:append>
				</xupdate:modifications>

			Util.retry(10) {
    		updateService.update(updateCmd.toString)
			}
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


  protected def localHostname(): String = {
    InetAddress.getLocalHost().getHostName()
  }

}
