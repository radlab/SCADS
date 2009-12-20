package deploylib.xresults

import java.net.InetAddress
import scala.xml.{NodeSeq, UnprefixedAttribute, Elem, Node, Null, Text, TopScope }
import java.io.File

/**
 * XResult is a specific XmlCollection stored at http://scm.millennium.berkeley.edu:8080/exist intended for storing the results of RADLab performance experiments.
 * Any XML fragment stored using XResult will be grouped by the actively running <code>experimentId</code>.
 * An <code>experimentId</code> is generated by calling startExperiment and is automatically propagated to other processes that are configured using the config framework.
 */
object XResult extends XmlCollection("xmldb:exist://scm.millennium.berkeley.edu:8080/exist/xmlrpc/db/results", "experiment", "scads") {
  val hostname = localHostname()

	/**
	 * Returns the currently running experimentId for this JVM or null if no experiment is running.
	 * An experimentId is an auto-generated identifier that is used to correlate seperate XML fragments from differerent functions/threads/machines into one complete experiment XML document
	 */
  def experimentId(): String = System.getProperty("experimentId")

	/**
   * Generates a new experimentId with the given description, and set it as the active experiment.
   */
	def startExperiment(description: String):Unit = startExperiment(<description>{description}</description>)

	/**
   * Generates a new experimentId with following experiment data, in the form of an XML {@link NodeSeq} Generates a new experimentId with following experiment data, in the form of an XML {@link NodeSeq}, and sets it as the active experiment.
	 * @param experimentData an xml fragment describing the experiment, which should contain at minimum a description tag.
   */
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

	/**
	 * Stores the given XML fragment associated with the currently running experiment.  If no experiment is running, the fragement is logged to the console.
	 */
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

	/**
	 * Records a result element tagged with the current hostname containing the provided XML fragment.
	 */
  def recordResult(result: NodeSeq): Unit = {
    storeXml(<result hostname={hostname}>{result}</result>)
  }

	/**
	 * Records the execution time of the provided function to the active experiment, along with the XML fragment that is returned by the function.
	 */
  def benchmark(func: => Elem): Elem = {
    val startTime = System.currentTimeMillis()
    val result = func
    val endTime = System.currentTimeMillis()
      <benchmark type="open" unit="miliseconds" startTime={startTime.toString()} endTime={endTime.toString()}>
				{result}
			</benchmark>
  }

	/**
	 * Repeatedly calls the provided function for up to the specified number of seconds.
	 * The total execution time, number of succesful and failed iterations and the XML data is recorded to the actively running experiment, along with a histogram of time for each iteration set.
	 * @param seconds continue running the benchmark until this many seconds has elapsed
	 * @param iterationsPerCheck the number of times to called the function before checking to see if the time is up.  This is also the granularity that will be used to record execution time in the stored histogram.
	 * @param data an XML fragment describing the benchmark parameters.
	 */
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

	/**
	 * Calls the provided function and returns its result.  If an exception is thrown by the function it will be recorded to the active experiment and rethrown.
	 */
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

	/**
	 * Records the contents of <code>directory</code> on the {@link RemoteMachine} <code>target</code> to the active experiment
	 */
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
