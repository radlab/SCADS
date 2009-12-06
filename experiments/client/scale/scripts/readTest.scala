import scaletest._
import deploylib.rcluster._
import deploylib.xresults._
import edu.berkeley.cs.scads.thrift._
import org.apache.log4j.Logger
import deploylib.Util

settings.maxPrintString = 1000000

class ReadExp {
	val logger = Logger.getLogger("script")

	val nodes = List(r27, r10, r9, r32)

	logger.info("Cleaning up running services")
	nodes.foreach(_.stopWatches)
	nodes.flatMap(_.services).foreach(s => {s.stop; s.blockTillDown; s.exit; s.clearFailures; s.clearLog})

	val cluster = ScadsDeployment.deployScadsCluster(nodes, false)
}
