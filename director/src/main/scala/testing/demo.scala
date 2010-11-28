import edu.berkeley.cs.scads.director._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import net.lag.logging.Logger

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
		
		Director.cluster = new ScadsCluster(TestScalaEngine.getTestHandler(/*1*/4).head.root)
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
	
	def start(namespaceString:String, zoopath:String):Unit = {
		val maxkey = 40000
		val workload1 = WorkloadGenerators.flatWorkloadRates(1.0, 0, 0, maxkey, 500, 1)
		val workload2 = WorkloadGenerators.flatWorkloadRates(1.0, 0, 0, maxkey, 3500, 2)
		val workload3 = WorkloadGenerators.flatWorkloadRates(1.0, 0, 0, maxkey, 500, 3)
		val workload = new WorkloadDescription(workload1.thinkTimeMean,workload1.workload ++ workload2.workload ++ workload3.workload)

		workload.serialize("/tmp/workload.ser")

		val client = AsyncIntClient(1,1,namespaceString,"/tmp/workload.ser",maxkey,2,0.1)
		client.run( ZooKeeperNode(zoopath) )
	}
	
	def start2(namespaceString:String, zoopath:String, numclients:Int):Unit = {
		val maxkey = 16000
		val workload1 = WorkloadGenerators.linearWorkloadRates(1.0, 0.0,0, maxkey, 3000, 14000, 360, 10*1000)
		val workload = new WorkloadDescription(workload1.thinkTimeMean,workload1.workload)// ++ workload1.workload.reverse)
		
		workload.serialize("/tmp/workload.ser")

		val client = AsyncIntClient(1,numclients,namespaceString,"/tmp/workload.ser",maxkey,2,0.1)
		client.run( ZooKeeperNode(zoopath) )
	}
}
