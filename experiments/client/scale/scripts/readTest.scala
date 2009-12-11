import scaletest._
import deploylib.rcluster._
import deploylib.xresults._
import edu.berkeley.cs.scads.thrift._
import org.apache.log4j.Logger
import deploylib.Util
import scala.collection.jcl.Conversions._

val allKeys = RangedPolicy.convert((null, null)).apply(0)
settings.maxPrintString = 1000000

	//logger.info("Cleaning up running services")
	//nodes.foreach(_.stopWatches)
	//nodes.flatMap(_.services).foreach(s => {s.stop; s.blockTillDown; s.exit; s.clearFailures; s.clearLog})

	//val cluster = ScadsDeployment.deployScadsCluster(nodes, false)

class ReadExp(serverNodes: List[RunitManager], clientNodes: List[RunitManager], threads: Int) {
	val logger = Logger.getLogger("script")

	XResult.startExperiment("Read Experiment " + serverNodes + clientNodes + threads)

	val cluster = ScadsDeployment.recoverScadsDeployment(serverNodes)
	ScadsDeployment.captureScadsDeployment(cluster)

//	clientNodes.foreach(_.clearAll)
	clientNodes.foreach(_.stopWatches)

	val loadServices = clientNodes.map(n => {
		ScadsDeployment.deployLoadClient(n, "RandomReader", Map("zookeeper" -> cluster.zooUri, "length" -> (5*60).toString, "threads" -> threads.toString))
	})
	loadServices.foreach(_.watchFailures)
	loadServices.foreach(_.once)
}
