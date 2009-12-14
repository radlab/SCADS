package scaletest

import deploylib._
import deploylib.rcluster._
import deploylib.xresults._
import deploylib.ec2._
import deploylib.runit._
import edu.berkeley.cs.scads.thrift._
import org.apache.log4j.Logger
import java.io.File

class LoadExp(nodes: List[RunitManager], threads: Int, bulkLoad: Boolean, testSize: Int) {
	val logger = Logger.getLogger("script")

	val partitions = IntTestDeployment.createPartitions(testSize, nodes.size)

	XResult.startExperiment("Threaded Load Experiment: " + threads + " " + bulkLoad + " " + testSize)

	logger.info("Cleaning up")
	nodes.foreach(_.clearAll)
	nodes.foreach(_.executeCommand("killall java"))
	nodes.foreach(_.stopWatches)

	val cluster = ScadsDeployment.deployScadsCluster(nodes, bulkLoad)

	val loadServices = partitions.zip(nodes).map(p => {
		logger.info("Setting range policy for: " + p)
		val n = StorageNode(p._2.hostname, ScadsDeployment.storageEnginePort)
		Util.retry(5) {
			n.useConnection(_.set_responsibility_policy("intKeys", RangedPolicy.convert((p._1.start, p._1.end))))
		}

		ScadsDeployment.deployLoadClient(p._2, "ThreadedLoader", Map("zookeeper" -> cluster.zooUri, "startKey" -> p._1.start, "endKey" -> p._1.end, "threads" -> threads.toString))
	})
	loadServices.foreach(_.watchFailures)
	loadServices.foreach(_.once)

	val postTestCollection = Future {
		loadServices.foreach(_.blockTillDown)
		logger.info("Begining Post-test collection")
		ScadsDeployment.captureScadsDeployment(cluster)
		(cluster.storageServices ++ loadServices).foreach(_.captureLog)
		cluster.storageServices.foreach(s => XResult.captureDirectory(s.manager, new File(s.serviceDir, "db")))
		logger.info("Post test collection complete")
	}
}

class ReadExp(serverNodes: List[RunitManager], clientNodes: List[RunitManager], threads: Int) {
	val logger = Logger.getLogger("script")

	XResult.startExperiment("Read Experiment " + serverNodes + clientNodes + threads)

	val cluster = ScadsDeployment.recoverScadsDeployment(serverNodes)
	ScadsDeployment.captureScadsDeployment(cluster)

	clientNodes.foreach(_.stopWatches)

	val loadServices = clientNodes.map(n => {
		ScadsDeployment.deployLoadClient(n, "RandomReader", Map("zookeeper" -> cluster.zooUri, "length" -> (5*60).toString, "threads" -> threads.toString))
	})
	loadServices.foreach(_.clearFailures)
	loadServices.foreach(_.watchFailures)
	loadServices.foreach(_.once)
}
