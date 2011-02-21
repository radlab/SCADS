package edu.berkeley.cs.scads.director

import edu.berkeley.cs.scads.comm.{ PartitionService, StorageService, ZooKeeperProxy, ZooKeeperNode, RemoteActorProxy }
import edu.berkeley.cs.scads.storage.{ GenericNamespace, ScadsCluster }
import net.lag.logging.Logger

object Director {
  //Logger("policy").setLevel(java.util.logging.Level.FINEST)
  private val rnd = new java.util.Random(7)
  val basedir = "/tmp"
  val bootupTimes = new BootupTimes()
  var cluster: ScadsCluster = null

  def nextRndInt(n: Int): Int = rnd.nextInt(n)

  def nextRndDouble(): Double = rnd.nextDouble()
}

case class Director(var numClients: Int, namespaceString: String, val scheduler: RemoteActorProxy) {
  val period = 20 * 1000
  var controller: Controller = null
  var thread: Thread = null
  val listeners = new collection.mutable.ArrayBuffer[String => Unit]()

  class Controller(policy: Policy, executor: ActionExecutor, stateHistory: StateHistory) extends Runnable() {
    @volatile var running = true
    def stop = running = false
    def run(): Unit = {
      while (running) {
        val latestState = stateHistory.getMostRecentState
        policy.perform(if (latestState != null) ScadsState.refresh(stateHistory.namespace, latestState.workloadRaw, latestState.time) else null, executor)
        Thread.sleep(period)
      }
    }
  }

  def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode) = {
    //val node = ZooKeeperNode(zookeepCanonical)
    if (Director.cluster == null) Director.cluster = new ScadsCluster(clusterRoot)
    var namespace: GenericNamespace = Director.cluster.getNamespace(namespaceString)
    val splitQueue = new java.util.concurrent.LinkedBlockingQueue[(Option[org.apache.avro.generic.GenericRecord],Seq[Option[org.apache.avro.generic.GenericRecord]])]()
    val mergeQueue = new java.util.concurrent.LinkedBlockingQueue[(Seq[Option[org.apache.avro.generic.GenericRecord]],Option[org.apache.avro.generic.GenericRecord])]()
    

    // update start time for existing servers
    val now = new java.util.Date().getTime
    Director.cluster.getAvailableServers.foreach(s => if (Director.bootupTimes.getBootupTime(s) == None) Director.bootupTimes.setBootupTime(s, now))

    val predictor = SimpleHysteresis(0.9, 0.1, 0.0)
    predictor.initialize
    val policy = if (System.getProperty("doEmpty", "false").toBoolean) new EmptyPolicy(predictor) else new BestFitPolicySplitting(null, 100, 100, 0.99, true, 20 * 1000, 10 * 1000, predictor, true, true, 1, 1, splitQueue, mergeQueue)
    val stateHistory = StateHistory(period, namespace, policy)
    stateHistory.startUpdating
    val executor = /*new TestGroupingExecutor(namespace, splitQueue, mergeQueue)*/new SplittingGroupingExecutor(namespace, splitQueue, mergeQueue, scheduler)// new GroupingExecutor(namespace, scheduler)
    listeners.foreach(executor.registerListener(_))
    executor.start

    // when clients are ready to start, start everything
    //val coordination = clusterRoot.getOrCreate("coordination") // TODO
    //Thread.sleep(10 * 1000)
    controller = new Controller(policy, executor, stateHistory)
    thread = new Thread(controller, "Controller:"+namespace.namespace)
    thread.start

  }
  def stop = {
    controller.stop
  }

  def registerActionListener(func: String => Unit): Unit = {
    listeners.append(func)
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

