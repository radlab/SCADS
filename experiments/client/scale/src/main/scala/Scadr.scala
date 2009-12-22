package scaletest

import java.io.File
import scala.collection.mutable.HashSet

import deploylib._
import deploylib.ec2._
import deploylib.runit._
import deploylib.xresults._
import deploylib.ParallelConversions._
import edu.berkeley.cs.scads.thrift._
import org.apache.log4j.Logger

import piql._

object UserCountTest {
	val nodes = EC2Instance.myInstances
	var le: ScadrLoadExp = null
	var re: ScadrReadExp = null

	var test: Future[Unit] = null

	def run() = test = Future {
		List(10, 100, 1000, 10000, 50000, 100000, 500000).foreach(users => {
			le = new ScadrLoadExp(nodes, users, 20, 10)
			le.postTestCollection()
			re = new ScadrReadExp(nodes, users, 5)
			re.postTestCollection()
		})
	}
}

class ScadrLoadExp(nodes: List[RunitManager], users: Int, thoughts: Int, following: Int) {
	val logger = Logger.getLogger("script")
	val partitions = IntTestDeployment.createPartitions(users, nodes.size)

	logger.info("Cleaning up")
	nodes.pforeach(n => {n.clearAll; n.executeCommand("killall java"); n.stopWatches})
	val cluster = ScadsDeployment.deployScadsCluster(nodes, true)
	val loadServices = partitions.zip(nodes).pmap(p => {
		logger.info("Setting range policy for: " + p)
		val n = StorageNode(p._2.hostname, ScadsDeployment.storageEnginePort)
//		Util.retry(5) {n.useConnection(_.set_responsibility_policy("intKeys", RangedPolicy.convert((p._1.start, p._1.end))))}
		Util.retry(5) {Queries.configureStorageEngine(n)}

		ScadsDeployment.deployLoadClient(p._2, "CreateScadrUsers", Map("zookeeper" -> cluster.zooUri, "startuser" -> p._1.start, "enduser" -> p._1.end, "thoughts" -> thoughts.toString, "following" -> following.toString, "maxuser" -> users.toString))
	})

	loadServices.foreach(_.watchFailures)
	loadServices.foreach(_.once)

	val postTestCollection = Future {
		loadServices.foreach(_.blockTillDown)
		logger.info("Begining Post-test collection")
		ScadsDeployment.captureScadsDeployment(cluster)
		cluster.zooService.captureLog
		(cluster.storageServices ++ loadServices).pforeach(_.captureLog)
		cluster.storageServices.pforeach(s => XResult.captureDirectory(s.manager, new File(s.serviceDir, "db")))
		logger.info("Post test collection complete")
	}
}

class ScadrReadExp(nodes: List[RunitManager], users: Int, threads: Int) {
	val logger = Logger.getLogger("script")

	logger.info("Capturing Scads Cluster State")
	val cluster = ScadsDeployment.recoverScadsDeployment(nodes)
	ScadsDeployment.captureScadsDeployment(cluster)
	nodes.pforeach(_.stopWatches)

	cluster.storageServices.foreach(s => {s.clearFailures; s.watchFailures})
	logger.info("Creating load clients")
	val loadServices = nodes.pmap(n => {
		ScadsDeployment.deployLoadClient(n, "ScadrThoughtstreamQuery", Map("zookeeper" -> cluster.zooUri, "maxuser" -> users.toString, "threads" -> threads.toString))
	})
	loadServices.foreach(_.clearFailures)
	loadServices.foreach(_.watchFailures)
	logger.info("Begining read test")
	loadServices.pforeach(_.once)
	logger.info("Load services started")

	val postTestCollection = Future {
		loadServices.foreach(_.blockTillDown)
		logger.info("Begining Post-Test Collection")
		cluster.storageServices.pforeach(_.captureLog)
		cluster.zooService.captureLog
		loadServices.pforeach(_.captureLog)
		logger.info("Post test collection Complete")
	}
}


abstract class ScadrTest extends ScadsTest {
	val seed = (XResult.hostname + Thread.currentThread().getName + System.currentTimeMillis).hashCode
	val rand = new Random(seed)

	lazy val maxUser = getIntOption("maxuser")
	options.addOption("m", "maxuser", true, "the highest user in the system")

	def makeUsername(id: Int): String = "user%010d".format(id)
	def randomUsername(): String = makeUsername(rand.nextInt(maxUser) + 1)
}


object CreateScadrUsers extends ScadrTest {
	options.addOption("s", "startuser", true, "the userId to start with")
	options.addOption("e", "enduser", true, "the userId to end with")
	options.addOption("t", "thoughts", true, "the number of thoughts per user")
	options.addOption("f", "following", true, "the number of following entries to create per user")

	def runExperiment(): Unit = {
		val startuser = getIntOption("startuser")
		val enduser = getIntOption("enduser")
		val thoughts = getIntOption("thoughts")
		val following = getIntOption("following")

		XResult.recordResult(
			XResult.benchmark {
				(startuser to (enduser - 1)).foreach(id => {
					val u = new user
					u.name(makeUsername(id))
					u.password("pass" + u)
					Util.retry(5) {XResult.recordException{u.save}}

					(1 to thoughts).foreach(tId => {
						val t = new thought
						t.owner(u)
						t.timestamp(tId)
						t.thought("User " + id + "is thinking:" + tId)
						Util.retry(5) {XResult.recordException{t.save}}
					})

					val followedUsers = new HashSet[String]()
					while(followedUsers.size < following) {
						val friend = randomUsername()
						if(!followedUsers.contains(friend)) {
							followedUsers += friend
							val s = new subscription
							s.owner(u)
							s.target(friend)
							s.approved(true)
							Util.retry(5) {XResult.recordException{s.save}}
						}
					}

				})
				<sequentialUserCreate startUser={getIntOption("startuser").toString} endUser={getIntOption("enduser").toString}/>
			}
		)
	}
}

object ScadrThoughtstreamQuery extends ScadrTest {
	options.addOption("threads", "t", true, "the number of request threads to make")

	def runExperiment(): Unit = {
		val results = (1 to getIntOption("threads")).toList.map(i => Future {
			XResult.timeLimitBenchmark(60*5, 1, <thoughtstreamQuery users={maxUser.toString}/>) {
				val u = Queries.userByName(randomUsername()).last
				u.thoughtstream(5).size == 5
			}
		})

		XResult.recordResult(results.map(r => <thread>{r()}</thread>))
	}
}
