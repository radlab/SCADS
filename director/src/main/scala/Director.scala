package edu.berkeley.cs.scads.director

import edu.berkeley.cs.scads.comm.{PartitionService,StorageService,ZooKeeperProxy, ZooKeeperNode}
import edu.berkeley.cs.scads.storage.{GenericNamespace,ScadsCluster}
import net.lag.logging.Logger

object Director {
	private val rnd = new java.util.Random(7)
	val basedir = "/tmp"
	val bootupTimes = new BootupTimes()
	var cluster:ScadsCluster = null
	
	def nextRndInt(n:Int):Int = rnd.nextInt(n)

	def nextRndDouble():Double = rnd.nextDouble()
}

case class Director(var numClients: Int, namespaceString:String) {
	val period = 20*1000

	class Controller() extends Runnable() {
		
		def run():Unit = {}
	}
	
	def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode) = {
		//val node = ZooKeeperNode(zookeepCanonical)
		Director.cluster = new ScadsCluster(clusterRoot)
		val namespace:GenericNamespace = Director.cluster.getNamespace(namespaceString)

		val predictor = SimpleHysteresis(0.9,0.1,0.0)
		val policy = new BestFitPolicy(null,100,100,0.99,true,100,10*60*1000,predictor,true,true,1,1)
		val stateHistory = StateHistory(period,namespace,policy)
		val executor = new TestGroupingExecutor(namespace)//new GroupingExecutor(namespace)
		
		// when clients are ready to start, start everything
		val coordination = clusterRoot.getOrCreate("coordination") // TODO
	}
	
	

}


	// instantiate with zookeeper info
	// create a service scheduler (in scads mesos)
	
	/*
	val node = ZooKeeperNode("zk://r2:2181/myscads/") // within this is List(availableServers, namespaces, clients)
	val c = new ScadsCluster(node)
	val namespace:GenericNamespace = c.getNamespace(namespaceString)
	*/
	
	
	// x use service scheduler to create ScadsMesosCluster
	
