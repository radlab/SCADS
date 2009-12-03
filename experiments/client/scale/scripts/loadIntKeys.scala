import scaletest._
import deploylib.rcluster._
import edu.berkeley.cs.scads.thrift._

object Exp {
	val nodes = List(r6,r8,r10,r11,r12)
	val testSize = 1000
	val partitions = IntTestDeployment.createPartitions(testSize, nodes)

	nodes.foreach(_.clearAll)
	nodes.foreach(_.executeCommand("killall java"))
	nodes.foreach(_.setupRunit)

	val zooNode = nodes(0)
	val zooService = ScadsDeployment.deployZooKeeperServer(zooNode)
	zooService.watchLog
	zooService.start
	zooService.blockTillUpFor(5)

	val storageServices = nodes.map(ScadsDeployment.deployStorageEngine(_, zooNode))
	storageServices.foreach(_.watchLog)
	storageServices.foreach(_.start)
	storageServices.foreach(_.blockTillUpFor(5))

	val loadServices = partitions.map(p => {
		val n = StorageNode(p.node.hostname, ScadsDeployment.storageEnginePort)
		n.useConnection(_.set_responsibility_policy("intKeys", RangedPolicy.convert((p.start, p.end))))

		IntTestDeployment.deployIntKeyLoader(p.node, p.start, p.end, zooNode.hostname + ":" + ScadsDeployment.zookeeperPort)
	})

	loadServices.foreach(_.once)

}
