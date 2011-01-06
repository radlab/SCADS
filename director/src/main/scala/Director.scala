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

case class Director(var numClients: Int, namespaceString:String, val scheduler:ScadsServerScheduler) {
	val period = 20*1000
	var runnerThread:Controller = null

	class Controller(policy:Policy, executor:ActionExecutor, stateHistory:StateHistory) extends Runnable() {
		var running = true
		def stop = running = false
		def run():Unit = {
			while (running) {
				val latestState = stateHistory.getMostRecentState
				policy.perform(if (latestState != null) ScadsState.refresh(stateHistory.namespace,latestState.workloadRaw,latestState.time) else null, executor)
				Thread.sleep(period)
			}
		}
	}
	
	def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode) = {
		//val node = ZooKeeperNode(zookeepCanonical)
		if (Director.cluster == null) Director.cluster = new ScadsCluster(clusterRoot)
		var namespace:GenericNamespace = Director.cluster.getNamespace(namespaceString)
		
		// update start time for existing servers
		val now = new java.util.Date().getTime
		Director.cluster.getAvailableServers.foreach(s => if (Director.bootupTimes.getBootupTime(s) == None) Director.bootupTimes.setBootupTime(s,now) )

		val predictor = SimpleHysteresis(0.9,0.1,0.0)
		predictor.initialize
		val policy = new BestFitPolicy(null,100,100,0.99,true,20*1000,10*1000,predictor,true,true,1,1)
		val stateHistory = StateHistory(period,namespace,policy)
		stateHistory.startUpdating
		val executor = /*new TestGroupingExecutor(namespace)*/new GroupingExecutor(namespace,scheduler)
		executor.start
		
		// when clients are ready to start, start everything
		//val coordination = clusterRoot.getOrCreate("coordination") // TODO
		Thread.sleep(10*1000)
		runnerThread = new Controller(policy,executor,stateHistory)
		(new Thread(runnerThread)).start
		
	}
	def stop = {
		runnerThread.stop
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
	
