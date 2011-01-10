import edu.berkeley.cs.scads.director._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import net.lag.logging.Logger

object ScadrDirectorTest {
  org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)
  var sched:ScadsServerScheduler = null
  val namespaces = List("user","thought","subscription")

  /**
  * Start a cluster with one storage engine per namespace using mesos slaves
  * creates the scadr namespaces
  */
  def init(zoopath:String, mesospath:String):Seq[GenericNamespace] = {
    // connect to mesos and zookeeper
  	val node = ZooKeeperNode(zoopath)
  	ScadsServerScheduler
  	sched = ScadsServerScheduler("director",mesospath,node.canonicalAddress)
  	sched.addServers( namespaces.map(ns => ns+"node0") )
  	
  	// wait until have enough servers for one per namespace
		Thread.sleep(10*1000)
		Director.cluster = new ScadsCluster(node)
		while (Director.cluster.getAvailableServers.size < 3) {
			Thread.sleep(3*1000)
			Director.cluster = new ScadsCluster(node)
		}

		List( (namespaces(0),classOf[edu.berkeley.cs.scads.piql.scadr.User].newInstance),(namespaces(1),classOf[edu.berkeley.cs.scads.piql.scadr.Thought].newInstance),(namespaces(2),classOf[edu.berkeley.cs.scads.piql.scadr.Subscription].newInstance) ).map(entry => {
		  Director.cluster.createNamespace(entry._1, entry._2.key.getSchema, entry._2.value.getSchema, List((None,Director.cluster.getAvailableServers(entry._1))))
		})
		
  }
  
  /**
  * Start a local test cluster with one storage engine per namespace
  * creates the scadr namespaces
  */
  def initTest():Seq[GenericNamespace] = {
    	Director.cluster = new ManagedScadsCluster(TestScalaEngine.newScadsCluster(0).root)
  		namespaces.foreach(ns => Director.cluster match {case m:ManagedScadsCluster => m.addNamedNode(ns+"first") })
  		
  		List( (namespaces(0),classOf[edu.berkeley.cs.scads.piql.scadr.User].newInstance),(namespaces(1),classOf[edu.berkeley.cs.scads.piql.scadr.Thought].newInstance),(namespaces(2),classOf[edu.berkeley.cs.scads.piql.scadr.Subscription].newInstance) ).map(entry => {
  		  Director.cluster.createNamespace(entry._1, entry._2.key.getSchema, entry._2.value.getSchema, List((None,Director.cluster.getAvailableServers(entry._1))))
  		})
  }
  
  /**
  * given the namespaces and zookeeper path, create and start a director per namespace
  */
  def start(namespaceStrings:Seq[String], zoopath:String):Seq[Director] = {
    System.setProperty("doEmpty","true") // run the empty policy, which does nothing but observe workload stats
		val directors = namespaceStrings.map(ns => Director(1,ns,sched))
		directors.foreach(_.run( ZooKeeperNode(zoopath) ))
		directors
	}
}