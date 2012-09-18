package edu.berkeley.cs
package scads
package piql
package mviews

import scala.util.Random
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

import deploylib._
import deploylib.mesos._
import avro.marker._
import comm._
import config._
import exec._
import opt._
import perf._
import plans._
import storage._
import storage.client.index._

import net.lag.logging.Logger

object TimelineExperiment extends ExperimentBase {
  def newClient(): TimelineClient = {
    val cluster = TestScalaEngine.newScadsCluster(3)
    new TimelineClient(cluster, new ParallelExecutor, numStripes = 10)
  }

  def stripeByUser(user: String, numStripes: Int): Int = {
    (ExperimentKeyspace.invert(user) * numStripes + .00001).intValuet
  }

  def go(replicas: Int = 1,
         partitions: Int = 1,
         nClients: Int = 1,
         iterationMinutes: Double = 1.0,
         usersPerMachine: Int = 50000,
         numStripes: Int = 10,
         comment: String = "",
         local: Boolean = false,
         threadCount: Int = 4,
         samplingFraction: Double = 1.0,
         keepStorageRunning: Boolean = false) (implicit cluster: deploylib.mesos.Cluster, classSource: Seq[ClassSource]): Unit = {
    val experiment = new TimelineExperimentTask(
        replicas=replicas,
        partitions=partitions,
        samplingFraction=samplingFraction,
        numStripes=numStripes,
        nClients=nClients,
        usersPerMachine=usersPerMachine,
        threadCount=threadCount,
        comment=comment,
        iterationMinutes=iterationMinutes,
        local=local,
        keepStorageRunning=keepStorageRunning)
    if (local) {
      experiment.run()
    } else {
      experiment.schedule(resultClusterAddress)
    }
  }
}

/**
 * Client that supports the striping of Subscriptions across partitions.
 */
class TimelineClient(val cluster: ScadsCluster,
                     implicit val executor: QueryExecutor,
                     val numStripes: Int,
                     val limit: Int = 20) {
  val posts = cluster.getNamespace[Post]("posts")
  val subscr = cluster.getNamespace[Subscription]("subscr")
  val timelineUnopt =
    posts.as("p")
      .join(subscr.as("s"))
      .where("p.topicId".a === "s.topicId".a)
      .where("s.userId".a === (0.?))
      .sort(List("p.timestamp".a))
      .paginate(limit)
      .select("p.text".a, "p.topicId".a)

  val timelineQuery = timelineUnopt.toPiqlWithView("timelineQuery", detectStripedIndex = true)

  val selectSubscribersForStripe =
    subscr.where("_stripe".a === (0.?))
          .where("topicId".a === (1.?))
          .dataLimit(1024)
          .toPiql("selectSubscribersForStripe")

  val selectUsers =
    subscr.where("userId".a === (0.?))
          .dataLimit(1024)
          .toPiql("selectUsers")

  def postToTopic(topic: String, text: String): Unit = {
    val p = Post(topic, System.currentTimeMillis)
    p.text = text
    posts.put(p)
  }

  def addSubscription(user: String, topic: String): Unit = {
    val s = Subscription(user, topic)
    s._stripe = TimelineExperiment.stripeByUser(user, numStripes)
    s
  }

  def countSubscribers(topic: String): Int = {
    (0 until numStripes).pmap(selectSubscribersForStripe(_, topic).length).sum
  }
}

// Task to run timeline queries on EC2.
case class TimelineExperimentTask(var replicas: Int = 1,
                                  var partitions: Int = 1,
                                  var nClients: Int = 1,
                                  var iterations: Int = 2,
                                  var iterationMinutes: Double = 1.0,
                                  var usersPerMachine: Int = 10000,
                                  var numStripes: Int = 10,
                                  var samplingFraction: Double = 0.1,
                                  var maxSubscriptionsPerUser: Int = 50,
                                  var meanSubscriptionsPerUser: Int = 10,
                                  var readFrac: Double = 0.5,
                                  var threadCount: Int = 4,
                                  var comment: String = "",
                                  var zipfExponent: Double = 0.7,
                                  var uniqueTopics: Int = 2000,
                                  var local: Boolean = true,
                                  var keepStorageRunning: Boolean = false)
    extends AvroTask with AvroRecord with TaskBase {
  
  var resultClusterAddress: String = _
  var clusterAddress: String = _

  def schedule(resultClusterAddress: String)(implicit cluster: deploylib.mesos.Cluster,
                                             classSource: Seq[ClassSource]): Unit = {
    var extra = java.lang.Math.sqrt(replicas * partitions / 5).intValue
    val preallocSize = 1L << 31 // 1L << 32 for 4GiB
    val scadsCluster = newScadsCluster(replicas * partitions + extra,
                                       preallocSize = preallocSize, noSync = true)
    clusterAddress = scadsCluster.root.canonicalAddress
    this.resultClusterAddress = resultClusterAddress
    val task = this.toJvmTask
    (1 to nClients).foreach {
      i => cluster.serviceScheduler.scheduleExperiment(task :: Nil)
    }
  }

  def setupPartitions(cluster: ScadsCluster, logger: Logger) = {
    implicit val rnd = new Random()
    val numServers: Int = replicas * partitions
    val available = cluster.getAvailableServers
    assert (available.size >= numServers)

    // Partitioning Scheme for Posts

    val posts = cluster.getNamespace[Post]("posts")
    val groups = available.take(numServers).grouped(replicas).toSeq
    var partitionRanges: List[(Option[Array[Byte]], Seq[StorageService])] = List()
    for (servers <- groups) {
      if (partitionRanges.length == 0) {
        partitionRanges ::= (None, servers)
      } else {
        val keyBytes = posts.keyToBytes(Post(ExperimentKeyspace.lookup(partitionRanges.length, partitions), 0))
        partitionRanges ::= (keyBytes, servers)
      }
    }
    partitionRanges = partitionRanges.reverse
    logger.info("Uniform partition scheme for posts: " + partitionRanges)

    posts.setPartitionScheme(partitionRanges)
    posts.setReadWriteQuorum(.001, 1.00)

    // Partitioning Scheme for Subscriptions

    val subscr = cluster.getNamespace[Subscription]("subscr")
    partitionRanges = List()
    for (servers <- groups) {
      if (partitionRanges.length == 0) {
        partitionRanges ::= (None, servers)
      } else {
        val keyBytes = subscr.keyToBytes(Subscription(ExperimentKeyspace.lookup(partitionRanges.length, partitions), ""))
        partitionRanges ::= (keyBytes, servers)
      }
    }
    partitionRanges = partitionRanges.reverse
    logger.info("Partitioning scheme for subscriptions: " + partitionRanges)

    subscr.setPartitionScheme(partitionRanges)
    subscr.setReadWriteQuorum(.001, 1.00)

    // Partitioning Scheme for Striped Subscription Index

    val subscribersByTopic = subscr.getOrCreateIndex(AttributeIndex("_stripe") :: AttributeIndex("topicId") :: Nil)
    partitionRanges = List()
    for (servers <- groups) {
      if (partitionRanges.length == 0) {
        partitionRanges ::= (None, servers)
      } else {
        val keyBytes = subscribersByTopic.keyToBytes(SubscrIndexType(partitionRanges.length / partitions * numStripes, "", ""))
        partitionRanges ::= (keyBytes, servers)
      }
    }
    partitionRanges = partitionRanges.reverse
    logger.info("Partitioning scheme for striped index: " + partitionRanges)

    subscribersByTopic.setPartitionScheme(partitionRanges)
    subscribersByTopic.setReadWriteQuorum(.001, 1.00)
  }

  def createStorageCluster(capacity: Int) = {
    if (local) {
      TestScalaEngine.newScadsCluster(2 + capacity)
    } else {
      assert (clusterAddress != null)
      val c = new ExperimentalScadsCluster(ZooKeeperNode(clusterAddress))
      c.blockUntilReady(capacity)
      c
    }
  }

  def createResultsCluster() = {
    if (local) {
      TestScalaEngine.newScadsCluster(1)
    } else {
      assert (resultClusterAddress != null)
      new ScadsCluster(ZooKeeperNode(resultClusterAddress))
    }
  }

  def populateSubscriptionsForSegment(totalUsers: Int, segment: Int,
                                      numSegments: Int, cluster: ScadsCluster,
                                      logger: Logger) = {
    val subscriptionsToGenerate = totalUsers * meanSubscriptionsPerUser

    assert (segment >= 0 && segment < numSegments)
    assert (maxSubscriptionsPerUser * totalUsers > subscriptionsToGenerate)
    assert (uniqueTopics > maxSubscriptionsPerUser)
    logger.info("Loading segment " + segment)

    implicit val rnd = new Random()
    val topicsOf = new HashMap[String,HashSet[String]]()
    var bulk = List[Tuple2[String,String]]()

    for (i <- Range(0, totalUsers)) {
      if (ExperimentKeyspace.inSegment(i, segment, numSegments)) {
        val user = ExperimentKeyspace.lookup(i, totalUsers)
        topicsOf(user) = new HashSet[String]()
      }
    }

    def chooseRandomTopic() =
      ExperimentKeyspace.lookup(ZipfDistribution.sample(uniqueTopics, zipfExponent), uniqueTopics)

    def randomUserInSegment(segment: Int, ns: Int)(implicit rnd: Random) = {
      def adjust(i: Int): Int = segment + i - (i % ns)
      var a = adjust(rnd.nextInt(totalUsers))

      /* re-randomize in rare edge cases*/
      while (!ExperimentKeyspace.inSegment(a, segment, ns)) {
        a = adjust(rnd.nextInt(totalUsers))
      }

      ExperimentKeyspace.lookup(a, totalUsers)
    }

    /* only generate for users in our segment */
    for (i <- 1 to (subscriptionsToGenerate/numSegments)) {
      var user = randomUserInSegment(segment, numSegments)
      var topic = chooseRandomTopic
      while (topicsOf(user).size > maxSubscriptionsPerUser) {
        user = randomUserInSegment(segment, numSegments)
      }
      while (topicsOf(user).contains(topic)) {
        topic = chooseRandomTopic
      }
      topicsOf(user).add(topic)
      bulk ::= (user, topic)
    }

    topicsOf.clear
    val subscr = cluster.getNamespace[Subscription]("subscr")
    logger.info("Bulk loading subscriptions", bulk.length)
    subscr ++= bulk.map(t => {
      val s = Subscription(t._1, t._2)
      s._stripe = TimelineExperiment.stripeByUser(t._1, numStripes)
      s
    })

    var preloaded = List[Post]()
    for (i <- Range(0, uniqueTopics)) {
      if (ExperimentKeyspace.inSegment(i, segment, numSegments)) {
        val topic = ExperimentKeyspace.lookup(i, uniqueTopics)
        val p = Post(topic, 12345)
        p.text = "some dummy placeholder text"
        preloaded ::= p
      }
    }

    val posts = cluster.getNamespace[Post]("posts")
    logger.info("Bulk loading posts", preloaded.length)
    posts ++= preloaded
  }

  def run(): Unit = {
    val logger = Logger()
    logger.info("Starting.")

    assert (numStripes >= partitions)

//    val resultCluster = createResultsCluster
//    val results = resultCluster.getNamespace[TimelineResult]("TimelineResult")
//
//    logger.info("Created results cluster.")
//
//    val canary = new TimelineResult(1234567890)
//    results.put(canary, None)
//
//    logger.info("Results cluster is up.")

    val cluster = createStorageCluster(replicas * partitions)
    val hostname = java.net.InetAddress.getLocalHost.getHostName
    val coordination = cluster.root.getOrCreate("coordination/timeline")

    logger.info("Storage cluster is up.")

    val clientNumber = coordination.registerAndAwait("clientsStart", nClients)

    logger.info("Clients are up.")

    val client = new TimelineClient(cluster, new ParallelExecutor, numStripes)

    logger.info("Created PIQL client.")

    coordination.registerAndAwait("clientsStarted", nClients)
    val clientId = client.getClass.getSimpleName

    if (clientNumber == 0) {
      logger.info("Client %d preparing partitions...", clientNumber)
      setupPartitions(cluster, logger)
    }
    coordination.registerAndAwait("partitionsReady", nClients)

    /* distributed data load */
    val loadStartMs = System.currentTimeMillis
    val totalUsers = usersPerMachine * partitions
    populateSubscriptionsForSegment(
        totalUsers, clientNumber,
        nClients, cluster, logger)
    coordination.registerAndAwait("dataReady", nClients)
    val loadTimeMs = System.currentTimeMillis - loadStartMs
    logger.info("Data load wait: %d ms", loadTimeMs)

    (1 to iterations).foreach(iteration => {
      logger.info("Beginning iteration %d", iteration)
      coordination.registerAndAwait("it:" + iteration + ",th:" + threadCount, nClients)
      val iterationStartMs = System.currentTimeMillis
      val failures = new java.util.concurrent.atomic.AtomicInteger()
      val ops = new java.util.concurrent.atomic.AtomicInteger()
      (0 until threadCount).pmap(tid => {
        implicit val rnd = new Random()
        val tfrac: Double = tid.doubleValue / threadCount.doubleValue

        while (System.currentTimeMillis - iterationStartMs < iteration*iterationMinutes*60*1000) {
          try {
            if (tfrac < readFrac) {
              val randomUser = ExperimentKeyspace.lookup(rnd.nextInt(totalUsers), totalUsers)
              val timeline = client.timelineQuery(randomUser)
            } else {
              val topic = ExperimentKeyspace.lookup(ZipfDistribution.sample(uniqueTopics, zipfExponent), uniqueTopics)
              val numSubscribers = client.countSubscribers(topic)
              val start = System.currentTimeMillis
              client.postToTopic(topic, "New post written at time=" + start)
              if (rnd.nextDouble() < samplingFraction) {
                logger.info(
                    "post_time=" + start + "," +
                    "num_subscribers=" + numSubscribers + "," +
                    "dt=" + (System.currentTimeMillis - start))
              }
            }
          } catch {
            case e => 
              logger.error(e.getMessage)
              failures.getAndAdd(1)
          }
        }
      })
    })

    coordination.registerAndAwait("experimentDone", nClients)
    if (clientNumber == 0 && !keepStorageRunning) {
      cluster.shutdown
    }
  }
}
