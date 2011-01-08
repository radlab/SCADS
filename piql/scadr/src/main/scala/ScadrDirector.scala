import edu.berkeley.cs.scads.director._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import net.lag.logging.Logger

object ScadrDirectorTest {
  org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)
  val partitionsPerNamespace = 1
  var sched:ScadsServerScheduler = null

  def init(zoopath:String, mesospath:String):Seq[GenericNamespace] = {
    // connect to mesos and zookeeper
  	val node = ZooKeeperNode(zoopath)
  	ScadsServerScheduler
  	sched = ScadsServerScheduler("director",mesospath,node.canonicalAddress)
  	sched.addServers( List("User","Thought","Subscription").map(ns => ns+"node0") )
  	
  	// wait until have enough servers for one per namespace
		Thread.sleep(10*1000)
		Director.cluster = new ScadsCluster(node)
		while (Director.cluster.getAvailableServers.size < 3) {
			Thread.sleep(3*1000)
			Director.cluster = new ScadsCluster(node)
		}

		List( ("User",classOf[edu.berkeley.cs.scads.piql.scadr.User].newInstance),("Thought",classOf[edu.berkeley.cs.scads.piql.scadr.Thought].newInstance),("Subscription",classOf[edu.berkeley.cs.scads.piql.scadr.Subscription].newInstance) ).map(entry => {
		  Director.cluster.createNamespace(entry._1, entry._2.key.getSchema, entry._2.value.getSchema, List((None,Director.cluster.getAvailableServers(entry._1))))
		})
		
  }
  
  def initTest():Seq[GenericNamespace] = {
    	Director.cluster = new ManagedScadsCluster(TestScalaEngine.newScadsCluster(0).root)
  		List("User","Thought","Subscription").foreach(ns => Director.cluster match {case m:ManagedScadsCluster => m.addNamedNode(ns+"first") })
  		
  		List( ("User",classOf[edu.berkeley.cs.scads.piql.scadr.User].newInstance),("Thought",classOf[edu.berkeley.cs.scads.piql.scadr.Thought].newInstance),("Subscription",classOf[edu.berkeley.cs.scads.piql.scadr.Subscription].newInstance) ).map(entry => {
  		  Director.cluster.createNamespace(entry._1, entry._2.key.getSchema, entry._2.value.getSchema, List((None,Director.cluster.getAvailableServers(entry._1))))
  		})
  }
  
  def start(namespaceStrings:Seq[String], zoopath:String):Seq[Director] = {
    System.setProperty("doEmpty","true")
		val directors = namespaceStrings.map(ns => Director(1,ns,sched))
		directors.foreach(_.run( ZooKeeperNode(zoopath) ))
		directors
	}
}