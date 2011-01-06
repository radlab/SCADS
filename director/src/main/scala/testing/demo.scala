import edu.berkeley.cs.scads.director._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import net.lag.logging.Logger

/**
* assumptions: mesos master and slaves are running, zookeeper running
* to run: init() to connect to cluster and create namespaces
*/
object multiNSdirectorTest {
	org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)
	val partitionsPerNamespace = 10
	var maxkey = System.getProperty("maxkey","10000").toInt
	val nsPrefix = "directortest"
	var sched:ScadsServerScheduler = null
	
	/**
	* connect to zookeep, create cluster, set up namespaces with all partitions on one server
	*/
	def init(zoopath:String, mesospath:String, numNamespaces:Int):Seq[GenericNamespace] = {
		// connect to mesos and zookeeper
		val node = ZooKeeperNode(zoopath)
		sched = ScadsServerScheduler("director",mesospath,node.canonicalAddress)
		sched.addServers( (0 until numNamespaces).toList.map(i => "init"+i) )
		
		// wait until have enough servers for one per namespace
		Thread.sleep(10*1000)
		Director.cluster = new ScadsCluster(node)
		var storageServers = Director.cluster.getAvailableServers
		while (storageServers.size < numNamespaces) {
			Thread.sleep(3*1000)
			Director.cluster = new ScadsCluster(node)
			storageServers = Director.cluster.getAvailableServers
		}
		
		// create each namespace, and put all partitions on one server
		val slice = maxkey/partitionsPerNamespace
		(0 until numNamespaces).toList.map( i => {
			Director.cluster.createNamespace[IntRec, StringRec](nsPrefix+i, 
				(None, storageServers.slice(i,i+1)) :: 
				(1 until partitionsPerNamespace).toList.map(p => (Some(IntRec(p*slice)), storageServers.slice(i,i+1)))
			).genericNamespace
		})
		
	}
	
	def initTest(numNamespaces:Int):Seq[GenericNamespace] = {
		Director.cluster = new ManagedScadsCluster(TestScalaEngine.newScadsCluster(0).root)
		(0 until numNamespaces).foreach(i => Director.cluster match {case m:ManagedScadsCluster => m.addNamedNode(nsPrefix+i+"first") })
		
		// create each namespace, and put all partitions on one server
		val slice = maxkey/partitionsPerNamespace
		(0 until numNamespaces).toList.map( i => {
			var storageServers = Director.cluster.getAvailableServers(nsPrefix+i)
			Director.cluster.createNamespace[IntRec, StringRec](nsPrefix+i, 
				(None, storageServers.slice(0,1)) :: 
				(1 until partitionsPerNamespace).toList.map(p => (Some(IntRec(p*slice)), storageServers.slice(0,1)))
			).genericNamespace
		})
		
	}
	
	def start(namespaces:Seq[GenericNamespace], zoopath:String) = {
		val directors = (0 until namespaces.size).toList.map(i => Director(1,nsPrefix+i,sched))
		directors.foreach(_.run( ZooKeeperNode(zoopath) ))
	}
}

object directorTest {
	//Logger("scheduler").setLevel(java.util.logging.Level.FINEST)
	org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)
	var sched:ScadsServerScheduler = null
	
	def init4(zoopath:String,mesospath:String):Unit ={
		val node = ZooKeeperNode(zoopath)
		sched = ScadsServerScheduler("director",mesospath,node.canonicalAddress)
		sched.addServers(List("init"))
		Thread.sleep(10*1000)
		Director.cluster = new ScadsCluster(node)
		var storageServers = Director.cluster.getAvailableServers
		while (storageServers.isEmpty) {
			Thread.sleep(1*1000)
			Director.cluster = new ScadsCluster(node)
			storageServers = Director.cluster.getAvailableServers
		}
		val ns = Director.cluster.createNamespace[IntRec, StringRec]("getputtest", List(
		(None, storageServers.slice(0,1)), 
		(Some(IntRec(1000)), storageServers.slice(0,1)),
		(Some(IntRec(2000)), storageServers.slice(0,1)),
		(Some(IntRec(3000)), storageServers.slice(0,1))
		))
	}
	def init4x16(zoopath:String,mesospath:String):Unit ={
		val node = ZooKeeperNode(zoopath)
		sched = ScadsServerScheduler("director",mesospath,node.canonicalAddress)
		sched.addServers(List("init1","init2","init3","init4"))
		Thread.sleep(10*1000)
		Director.cluster = new ScadsCluster(node)
		var storageServers = Director.cluster.getAvailableServers
		while (storageServers.size < 4) {
			Thread.sleep(1*1000)
			Director.cluster = new ScadsCluster(node)
			storageServers = Director.cluster.getAvailableServers
		}
		val ns = Director.cluster.createNamespace[IntRec, StringRec]("getputtest", List(
		(None, storageServers.slice(0,1)), 
		(Some(IntRec(1000)), storageServers.slice(0,1)),
		(Some(IntRec(2000)), storageServers.slice(0,1)),
		(Some(IntRec(3000)), storageServers.slice(0,1)),
		(Some(IntRec(4000)), storageServers.slice(1,2)),
		(Some(IntRec(5000)), storageServers.slice(1,2)),
		(Some(IntRec(6000)), storageServers.slice(1,2)),
		(Some(IntRec(7000)), storageServers.slice(1,2)),
		(Some(IntRec(8000)), storageServers.slice(2,3)),
		(Some(IntRec(9000)), storageServers.slice(2,3)),
		(Some(IntRec(10000)), storageServers.slice(2,3)),
		(Some(IntRec(11000)), storageServers.slice(2,3)),
		(Some(IntRec(12000)), storageServers.slice(3,4)),
		(Some(IntRec(13000)), storageServers.slice(3,4)),
		(Some(IntRec(14000)), storageServers.slice(3,4)),
		(Some(IntRec(15000)), storageServers.slice(3,4))
		))
	}
	def initTest():Unit ={
		
		Director.cluster = new ScadsCluster(TestScalaEngine.newScadsCluster(/*1*/4).root)
		var storageServers = Director.cluster.getAvailableServers
		// val ns = Director.cluster.createNamespace[IntRec, StringRec]("getputtest", List(
		// (None, storageServers.slice(0,1)), 
		// (Some(IntRec(10000)), storageServers.slice(0,1)),
		// (Some(IntRec(20000)), storageServers.slice(0,1)),
		// (Some(IntRec(30000)), storageServers.slice(0,1))
		// ))
		val ns = Director.cluster.createNamespace[IntRec, StringRec]("getputtest", List(
		(None, storageServers.slice(0,1)), 
		(Some(IntRec(1000)), storageServers.slice(0,1)),
		(Some(IntRec(2000)), storageServers.slice(0,1)),
		(Some(IntRec(3000)), storageServers.slice(0,1)),
		(Some(IntRec(4000)), storageServers.slice(1,2)),
		(Some(IntRec(5000)), storageServers.slice(1,2)),
		(Some(IntRec(6000)), storageServers.slice(1,2)),
		(Some(IntRec(7000)), storageServers.slice(1,2)),
		(Some(IntRec(8000)), storageServers.slice(2,3)),
		(Some(IntRec(9000)), storageServers.slice(2,3)),
		(Some(IntRec(10000)), storageServers.slice(2,3)),
		(Some(IntRec(11000)), storageServers.slice(2,3)),
		(Some(IntRec(12000)), storageServers.slice(3,4)),
		(Some(IntRec(13000)), storageServers.slice(3,4)),
		(Some(IntRec(14000)), storageServers.slice(3,4)),
		(Some(IntRec(15000)), storageServers.slice(3,4))
		))
		
	}
	def start(zoopath:String):Unit = {
		//Logger("policy").setLevel(java.util.logging.Level.FINEST)
		val namespaceString = "getputtest"
		val director = Director(1,namespaceString,sched)
		director.run( ZooKeeperNode(zoopath) )
	}
}

object runClient {
	org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)
	var maxkey = System.getProperty("maxkey","10000").toInt
	
	def start(namespaceString:String, zoopath:String):Unit = {
		val workload1 = WorkloadGenerators.flatWorkloadRates(1.0, 0, 0, maxkey, 500, 1)
		val workload2 = WorkloadGenerators.flatWorkloadRates(1.0, 0, 0, maxkey, 3500, 2)
		val workload3 = WorkloadGenerators.flatWorkloadRates(1.0, 0, 0, maxkey, 500, 3)
		val workload = new WorkloadDescription(workload1.thinkTimeMean,workload1.workload ++ workload2.workload ++ workload3.workload)

		workload.serialize("/tmp/workload.ser")

		val client = AsyncIntClient(1,1,namespaceString,"/tmp/workload.ser",maxkey,2,0.1)
		client.run( ZooKeeperNode(zoopath) )
	}
	
	def start2(namespaceString:String, zoopath:String, numclients:Int):Unit = {
		val workload1 = WorkloadGenerators.linearWorkloadRates(1.0, 0.0,0, maxkey, 500, 1600, 20, 10*1000)
		val workload = new WorkloadDescription(workload1.thinkTimeMean,workload1.workload)// ++ workload1.workload.reverse)
		
		workload.serialize("/tmp/workload.ser")

		val client = AsyncIntClient(1,numclients,namespaceString,"/tmp/workload.ser",maxkey,2,0.1)
		client.run( ZooKeeperNode(zoopath) )
	}
	
	def startflat(namespaceString:String, zoopath:String, numclients:Int, totalflat:Int):Unit = {
		val workload = WorkloadGenerators.flatWorkloadRates(1.0, 0, 0, maxkey, totalflat, 3) // run for 3 min
		workload.serialize("/tmp/workload.ser")

		val client = AsyncIntClient(1,numclients,namespaceString,"/tmp/workload.ser",maxkey,2,0.1)
		client.run( ZooKeeperNode(zoopath) )
	}
}
