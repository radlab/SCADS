package edu.berkeley.cs
package radlab
package demo

import deploylib.mesos._
import avro.marker._
import scads.director._
import scads.comm._
import scads.storage._
import scads.perf._

import net.lag.logging.Logger


case class ScadrDirectorTask(var clusterAddress: String, var mesosMaster: String) extends AvroTask with AvroRecord {
  import DemoConfig._

  def run() = {
    val namespaces = Map("users" -> classOf[edu.berkeley.cs.scads.piql.scadr.User],
		       "thoughts" -> classOf[edu.berkeley.cs.scads.piql.scadr.Thought],
		       "subscriptions" -> classOf[edu.berkeley.cs.scads.piql.scadr.Subscription])
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val scheduler = ScadsServerScheduler("SCADr Storage Director",
					 mesosMaster,
					 clusterRoot.canonicalAddress)
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    logger.info("Adding servers to cluster for each namespace")
    scheduler.addServers(namespaces.keys.map(_ + "node0"))
    cluster.blockUntilReady(namespaces.size)
    Director.cluster = cluster

    namespaces.foreach {
      case (name, entityType) => {
	logger.info("Creating namespace %s", name)
	val entity = entityType.newInstance
	val keySchema = entity.key.getSchema
	val valueSchema = entity.value.getSchema
	val initialPartitions = (None, cluster.getAvailableServers(name)) :: Nil

	Director.cluster.createNamespace(name, keySchema, valueSchema, initialPartitions)
      }
    }

    /* run the empty policy, which does nothing but observe workload stats */
    //TODO: make this an argument to director instead of global property
    System.setProperty("doEmpty", "true")

    logger.info("Starting Directors")
    val directors = namespaces.keys.map(ns => Director(1, ns, scheduler))
    //TODO: maybe we should pass the zookeeper address upon creation
    directors.foreach(_.run(clusterRoot))

    //TODO: Join with director threads instead of just sleeping forever
    directors.foreach(_.thread.join())
  }
}
