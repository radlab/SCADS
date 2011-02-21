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

case class InitScadrClusterTask(var clusterAddress:String) extends AvroTask with AvroRecord {
  import DemoConfig._

  def run() = {
    DemoConfig.initScadrCluster(clusterAddress)
    System.exit(0)
  }
}

case class InitGraditClusterTask(var clusterAddress:String) extends AvroTask with AvroRecord {
  import DemoConfig._

  def run() = {
    DemoConfig.initGraditCluster(clusterAddress)
    System.exit(0)
  }
}

case class InitComradesClusterTask(var clusterAddress:String) extends AvroTask with AvroRecord {
  import DemoConfig._

  def run() = {
    DemoConfig.initComradesCluster(clusterAddress)
    System.exit(0)
  }
}

case class ScadrDirectorTask(var clusterAddress: String, var mesosMaster: String) extends AvroTask with AvroRecord {
  import DemoConfig._

  def run() = {
    val namespaces = Map("users" -> classOf[edu.berkeley.cs.scads.piql.scadr.User],
              "thoughts" -> classOf[edu.berkeley.cs.scads.piql.scadr.Thought],
               "subscriptions" -> classOf[edu.berkeley.cs.scads.piql.scadr.Subscription])
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val scheduler = /*ScadsServerScheduler("SCADr Storage Director",
					 mesosMaster,
					 clusterRoot.canonicalAddress)*/DemoConfig.serviceScheduler
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    //logger.info("Adding servers to cluster for each namespace")
    //scheduler.addServers(namespaces.keys.map(_ + "node0"))
    cluster.blockUntilReady(namespaces.size)
    Director.cluster = cluster

  //     namespaces.foreach {
  //       case (name, entityType) => {
  // logger.info("Creating namespace %s", name)
  // val entity = entityType.newInstance
  // val keySchema = entity.key.getSchema
  // val valueSchema = entity.value.getSchema
  // val initialPartitions = (None, cluster.getAvailableServers(name)) :: Nil
  //
  // Director.cluster.createNamespace(name, keySchema, valueSchema, initialPartitions)
  //       }
  //     }

    /* run the empty policy, which does nothing but observe workload stats */
    //TODO: make this an argument to director instead of global property
    //System.setProperty("doEmpty", "true")

    logger.info("Starting Directors")
    val directors = namespaces.keys.map(ns => Director(1, ns, scheduler,"SCADr"))
    //TODO: maybe we should pass the zookeeper address upon creation
    directors.foreach(_.registerActionListener(DemoScadr.post))
    directors.foreach(_.run(clusterRoot))
    directors.foreach(_.thread.join())
  }
}

case class GraditDirectorTask(var clusterAddress: String, var mesosMaster: String) extends AvroTask with AvroRecord {
  import DemoConfig._

  def run() = {
    val namespaces = Map("words" -> classOf[edu.berkeley.cs.scads.piql.gradit.Word],
           "books" -> classOf[edu.berkeley.cs.scads.piql.gradit.Book],
           "wordcontexts" -> classOf[edu.berkeley.cs.scads.piql.gradit.WordContext],
           "wordlists" -> classOf[edu.berkeley.cs.scads.piql.gradit.WordList],
           "games" -> classOf[edu.berkeley.cs.scads.piql.gradit.Game],
           "gameplayers" -> classOf[edu.berkeley.cs.scads.piql.gradit.GamePlayer],
           "users" -> classOf[edu.berkeley.cs.scads.piql.gradit.User],
           "challenges" -> classOf[edu.berkeley.cs.scads.piql.gradit.Challenge])
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val scheduler = DemoConfig.serviceScheduler
    val cluster = new ExperimentalScadsCluster(clusterRoot)
    val indexes = List("challenges_(user2)", "gameplayers_(score)", "gameplayers_(gameid)", "challenges_(user1)", "wordlists_(login)", "words_(wordlist)", "challenges_(game2)", "words_(word)", "challenges_(game1)")

    cluster.blockUntilReady(namespaces.size + indexes.size)
    Director.cluster = cluster

    logger.info("Starting Gradit Directors")
    val directors = (namespaces.keys ++ indexes).map(ns => Director(1, ns, scheduler,"gRADit"))

    directors.foreach(_.registerActionListener(DemoScadr.post))
    //TODO: maybe we should pass the zookeeper address upon creation
    directors.foreach(_.run(clusterRoot))

    directors.foreach(_.thread.join())
  }
}

case class ComradesDirectorTask(var clusterAddress: String, var mesosMaster: String) extends AvroTask with AvroRecord {
  import DemoConfig._

  def run() = {
    val namespaces = Map("candidates" -> classOf[edu.berkeley.cs.scads.piql.comrades.Candidate],
           "interviews" -> classOf[edu.berkeley.cs.scads.piql.comrades.Interview])
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val scheduler = DemoConfig.serviceScheduler
    val cluster = new ExperimentalScadsCluster(clusterRoot)
    val indexes = List("candidates_(name)", "interviews_(researchArea,interviewedAt,createdAt)", "interviews_(researchArea,score,status,interviewedAt)")

    cluster.blockUntilReady(namespaces.size + indexes.size)
    Director.cluster = cluster

    logger.info("Starting comRADes Directors")
    val directors = (namespaces.keys ++ indexes).map(ns => Director(1, ns, scheduler,"comRADes"))

    directors.foreach(_.registerActionListener(DemoScadr.post))
    //TODO: maybe we should pass the zookeeper address upon creation
    directors.foreach(_.run(clusterRoot))

    directors.foreach(_.thread.join())
  }
}
