package scaletest

import deploylib._
import deploylib.config._
import deploylib.runit._
import deploylib.xresults._
import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.model.{Environment, TrivialSession, TrivialExecutor, ZooKeptCluster}
import scala.collection.jcl.Conversions._

import org.apache.commons.cli.Options
import org.apache.commons.cli.GnuParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter

import scala.xml._

import org.apache.log4j.Logger
import java.io.File

case class ScadsDeployment(zooService: RunitService, zooUri: String, storageServices: List[RunitService])

object ScadsDeployment extends ConfigurationActions {
	val storageEnginePort = 9090
	val zookeeperPort = 2181
	val allKeys = RangedPolicy.convert((null, null)).get(0)

	def deployScadsCluster(nodes: List[RunitManager], bulkLoad: Boolean): ScadsDeployment = {
		logger.info("Configuring zookeeper")
		val zooNode = nodes(0)
		val zooService = ScadsDeployment.deployZooKeeperServer(zooNode)
		zooService.watchFailures
		zooService.start
		zooService.blockTillUpFor(5)
		zooNode.blockTillPortOpen(ScadsDeployment.zookeeperPort)

		logger.info("Configuring storage engines")
		val storageServices = nodes.map(ScadsDeployment.deployStorageEngine(_, zooNode, bulkLoad))
		storageServices.foreach(_.watchFailures)
		storageServices.foreach(_.start)
		storageServices.foreach(_.blockTillUpFor(5))
		nodes.foreach(_.blockTillPortOpen(ScadsDeployment.storageEnginePort))

		ScadsDeployment(zooService, zooNode.hostname + ":" + zookeeperPort, storageServices)
	}

	def recoverScadsDeployment(nodes: List[RunitManager]): ScadsDeployment = {
		val services = nodes.flatMap(_.services)
		val zooService = services.filter(_.name == "org.apache.zookeeper.server.quorum.QuorumPeerMain")(0)
		val storageServices = services.filter(_.name == "edu.berkeley.cs.scads.storage.JavaEngine")

		ScadsDeployment(zooService, zooService.manager.hostname + ":" + zookeeperPort, storageServices)
	}

	def captureScadsDeployment(dep: ScadsDeployment):Unit = {
		val nodes = dep.storageServices.map(s => new StorageNode(s.manager.hostname, storageEnginePort))
		val engineData = nodes.map(n => {
			val namespaces = Util.retry(5) {
				n.useConnection(_.get_set("resp_policies", allKeys)).map(_.key)
			}

			<scadsEngine>
				<host>{n.host}</host>
				{namespaces.map(ns => {
					<namespace name={ns}>
						{val policies = Util.retry(5)(RangedPolicy.convert(n.useConnection(_.get_responsibility_policy(ns))))
							policies.map(p => {
							<responsibilityPolicy>
								<startKey>{p._1}</startKey>
								<endKey>{p._2}</endKey>
							</responsibilityPolicy>
						})}
						<keyCount>{Util.retry(5)(n.useConnection(_.count_set(ns, allKeys)))}</keyCount>
					</namespace>
				})}
			</scadsEngine>
		})

		XResult.storeXml(
			<scadsDeployment>
				{engineData}
			</scadsDeployment>
		)
	}

	def deployLoadClient(target: RunitManager, cl: String, args: Map[String, String]):RunitService = {
		val cmdLineArgs = args.map(a => "--" + a._1 + " " + a._2).mkString("", " ", "")

		XResult.storeXml(
			<configuration type="loadClient">
				{args.map(a => <arg name={a._1}>{a._2}</arg>)}
			</configuration>
		)

		createJavaService(target, new File("target/scale-1.0-SNAPSHOT-jar-with-dependencies.jar"),
  		"scaletest." + cl,
  		512,
			cmdLineArgs)
	}

	protected def deployStorageEngine(target: RunitManager, zooServer: RemoteMachine, bulkLoad: Boolean): RunitService = {
    val bulkLoadFlag = if(bulkLoad) " -b " else ""
		createJavaService(target, new File("target/scale-1.0-SNAPSHOT-jar-with-dependencies.jar"),
			"edu.berkeley.cs.scads.storage.JavaEngine",
      2048,
			"-p " + storageEnginePort + " -z " + zooServer.hostname + ":" + zookeeperPort + bulkLoadFlag)
	}

	protected def deployZooKeeperServer(target: RunitManager): RunitService = {
		val zooStorageDir = createDirectory(target, new File(target.rootDirectory, "zookeeperdata"))
		val zooService = createJavaService(target, new File("target/scale-1.0-SNAPSHOT-jar-with-dependencies.jar"),
			"org.apache.zookeeper.server.quorum.QuorumPeerMain",
      1024,
			"zoo.cnf")
		val config = <configuration type="zookeeper">
									<tickTime>10000</tickTime>
									<initLimit>10</initLimit>
									<syncLimit>5</syncLimit>
									<clientPort>{zookeeperPort}</clientPort>
									<dataDir>{zooStorageDir}</dataDir>
								</configuration>

		val zooConfigData = config.descendant.filter(_.getClass == classOf[scala.xml.Elem]).map(e => e.label + "=" + e.text).mkString("", "\n", "\n")
		val zooConfigFile = createFile(target, new File(zooService.serviceDir, "zoo.cnf"), zooConfigData, "644")

		XResult.storeXml(config)

		return zooService
	}
}

abstract class ScadsTest {
	val logger = Logger.getLogger("scads.test")
	implicit val env = new Environment

	val options = new Options()
	options.addOption("z", "zookeeper", true, "The uri of the zookeeper server")
	options.addOption("h", "help",  false, "print usage information");
	var cmd: CommandLine = null

	def main(args: Array[String]): Unit = {
		val parser = new GnuParser();
		cmd = parser.parse( options, args);

		if(cmd.hasOption("help")) {
			printUsage()
			System.exit(1)
		}

		if(!cmd.hasOption("zookeeper")){
			logger.fatal("No zookeeper specified")
		}

		env.placement = new ZooKeptCluster(getStringOption("zookeeper"))
  	env.session = new TrivialSession
  	env.executor = new TrivialExecutor

		XResult.recordResult(
			Util.retry(5) {
				XResult.recordException {
					runExperiment
				}
			}
		)
	}

	def runExperiment(): NodeSeq

	protected def getStringOption(name: String): String = {
		if(!cmd.hasOption(name)) {
			logger.fatal("Required option not specified: " + name)
			printUsage()
			System.exit(1)
		}
		cmd.getOptionValue(name)
	}

	protected def getIntOption(name: String): Int = getStringOption(name).toInt
	protected def printUsage(): Unit = {
		val formatter = new HelpFormatter()
		formatter.printHelp("ScadsTest", options)
	}
}

abstract trait ThreadedScadsExperiment extends ScadsTest {
	options.addOption("t", "threads", true, "number of concurrent threads to run")

	def runExperiment(): NodeSeq = {
		val results = (1 to getIntOption("threads")).toList.map(r => {
			Future {
				runThread()
			}
		})

		results.map(r => <thread>{r()}</thread>)
	}

	def runThread(): NodeSeq
}
