import scaletest._
import deploylib.rcluster._
import edu.berkeley.cs.scads.thrift._
import org.apache.log4j.Logger
import deploylib.Util

settings.maxPrintString = 1000000

object Exp {
	val logger = Logger.getLogger("script")

	val nodes = List(r26, r16, r31, r30)
	val testSize = 100000
	val partitions = IntTestDeployment.createPartitions(testSize, nodes.size)

	logger.info("Cleaning up")
	nodes.foreach(_.clearAll)
	nodes.foreach(_.executeCommand("killall java"))
	nodes.foreach(_.setupRunit)

	logger.info("Configuring zookeeper")
	val zooNode = nodes(0)
	val zooService = ScadsDeployment.deployZooKeeperServer(zooNode)
	zooService.watchFailures
	zooService.start
	zooService.blockTillUpFor(5)
	zooNode.blockTillPortOpen(ScadsDeployment.zookeeperPort)


	logger.info("Configuring storage engines")
	val storageServices = nodes.map(ScadsDeployment.deployStorageEngine(_, zooNode))
	storageServices.foreach(_.watchFailures)
	storageServices.foreach(_.start)
	storageServices.foreach(_.blockTillUpFor(5))
	nodes.foreach(_.blockTillPortOpen(ScadsDeployment.storageEnginePort))

	val loadServices = partitions.zip(nodes).map(p => {
		logger.info("Setting range policy for: " + p)
		val n = StorageNode(p._2.hostname, ScadsDeployment.storageEnginePort)
		Util.retry(5)(() => {
			n.useConnection(_.set_responsibility_policy("intKeys", RangedPolicy.convert((p._1.start, p._1.end))))
		})
		IntTestDeployment.deployIntKeyLoader(p._2, "SingleConnectionPoolLoader", p._1.start, p._1.end, zooNode.hostname + ":" + ScadsDeployment.zookeeperPort)
	})
	loadServices.foreach(_.watchFailures)
	loadServices.foreach(_.once)
}
