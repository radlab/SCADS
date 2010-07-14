package edu.berkeley.cs.scads.scale.cluster

import deploylib._
import deploylib.ec2._
import deploylib.ParallelConversions._

import deploylib.runit._
import deploylib.config._
import deploylib.xresults._
import org.apache.log4j.Logger
import org.json.JSONObject
import org.json.JSONArray

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.test._
import edu.berkeley.cs.scads.comm.Conversions._
import edu.berkeley.cs.scads.workload._
import org.apache.avro.util.Utf8
import org.apache.zookeeper.CreateMode

case class PolicyRange(val minKey:Int, val maxKey:Int) {
	def contains(needle:Int) = (needle >= minKey && needle < maxKey)
}

object ZooKeeperServer {
	def main(args:Array[String]):Unit = {
		ZooKeep.start(args(0), args(1).toInt).root.getOrCreate("scads")
	}
}

object SetupNodes{
	val logger = Logger.getLogger("cluster.config")
	val commapattern = java.util.regex.Pattern.compile(",")

	def main(args: Array[String]): Unit = {
		val createNS = args(0).toBoolean
		val zoo_dns = args(1)
		val namespace = args(2)
		val cluster = new ScadsCluster(new ZooKeeperProxy(zoo_dns+":2181").root.get("scads"))
		try {
			if (createNS) createNamespace(cluster,namespace)
			else setPartitions(cluster, "/tmp/scads_config.dat","/tmp/scads_policy.dat",namespace)
			System.exit(0)
		} catch { case e => {logger.warn("Got exception "+ e.toString); System.exit(1)}}
	}

	def createNamespace(cluster:ScadsCluster, namespace:String) = {
		val k1 = new IntRec
		val v1 = new StringRec
		cluster.createNamespace(namespace, k1.getSchema(), v1.getSchema())

		// set up partitions for empty and fake clusters
		val minKey = new IntRec
		val maxKey = new IntRec
		val partition = new KeyPartition; partition.minKey = minKey.toBytes; partition.maxKey = maxKey.toBytes
		val policy = new PartitionedPolicy; policy.partitions = List(partition)
		cluster.addPartition(namespace,"empty",policy)

		minKey.f1 = -1
		maxKey.f1 = -1
		partition.minKey = minKey.toBytes; partition.maxKey = maxKey.toBytes
		policy.partitions = List(partition)
		cluster.addPartition(namespace,"fake",policy)
	}
	/**
	* policy_file: partition, min,max
	* server_file: partition,server
	*/
	def setPartitions(cluster:ScadsCluster, server_file:String,policy_file:String,namespace:String) = {
		// tell each server which partition it has
		// and register each server with zookeeper
		val cr = new ConfigureRequest
		cr.namespace = namespace
		var lines = scala.io.Source.fromFile(server_file).getLines
		lines.foreach(line=>{
			val tokens = commapattern.split(line)
			if (tokens.size==2) {
				val part = tokens(0).trim
				logger.info("Setting "+tokens(1).trim+" with partition "+part)
				cr.partition = part
				Sync.makeRequest(RemoteNode(tokens(1).trim,9991), new Utf8("Storage"),cr)
				try {
					if (!part.equals("1")) cluster.addPartition(namespace,part)
				} catch { case e => logger.warn("Got exception when adding partition "+ e.toString) }
				cluster.namespaces.get(namespace+"/partitions/"+part+"/servers").createChild(tokens(1).trim, "", CreateMode.PERSISTENT)
			}
		})

		lines = scala.io.Source.fromFile(policy_file).getLines
		lines.foreach(line=>{
			val tokens = commapattern.split(line)
			if (tokens.size==3) {
				val part = tokens(0).trim
				val minKey = new IntRec; val maxKey = new IntRec; minKey.f1 = tokens(1).trim.toInt; maxKey.f1 = tokens(2).trim.toInt
				logger.info("Setting partition"+part+": "+minKey+" - "+maxKey)
				try {
					val partition = new KeyPartition; partition.minKey = minKey.toBytes; partition.maxKey = maxKey.toBytes
					val policy = new PartitionedPolicy; policy.partitions = List(partition)
					cluster.namespaces.get(namespace+"/partitions/"+part+"/policy").data = policy.toBytes
				} catch { case e => logger.warn("Got exception when adding modifying placement policy "+ e.toString) }
			}
		})

	}
}

object StartRequestHandler {
	//BasicConfigurator.configure()
	val commapattern = java.util.regex.Pattern.compile(",")
	System.setProperty("xtrace_stats","true")
	val xtrace_on = System.getProperty("xtrace_stats","false").toBoolean

	def main(args: Array[String]) {
		val zoo_dns = args(0)
		val namespace = args(1)
		val doWarming = args(2).toBoolean
		val minKey = args(3).toInt
		val maxKey = args(4).toInt
		val replicas = args(5).toInt
		val clients = args(6).toInt

		val requesthandler = if (doWarming) {
			val workload = WorkloadGenerators.warmingWorkload(namespace, minKey, maxKey, java.lang.Math.ceil(((maxKey-minKey)*replicas)/1000.toDouble).toInt+10)
			new DynamicWarmer(namespace, zoo_dns, minKey, maxKey, workload, clients, replicas)
		}
		else {
			val workload = WorkloadGenerators.linearWorkloadRates(1.0, 0.0, 0, maxKey.toString, namespace, 1000, 12000, 12, 10*1000)
			//val workload = WorkloadGenerators.flatWorkloadRates(1.0, 0.0, 0, maxKey.toString, namespace, 8000, 1)
			new DynamicRequestHandler(namespace, zoo_dns, workload, clients)
		}
		/*if (xtrace_on) {
			requesthandler match {
				case rh:LogInXtrace => {
					val xtrace_logger = new RequestLogger(new java.util.concurrent.ArrayBlockingQueue[String](1000))
					requesthandler.xtrace_logger_queue = xtrace_logger.queue
					(new Thread(xtrace_logger)).start
				}
			}
		}*/
		requesthandler.run
	}
}

// clients should be m1.large: ami-e4a2448d
// servers should be m1.small: ami-e7a2448e
object ClusterDeployment extends ConfigurationActions {

	var deploy_jarpath = "target/cluster_scale-1.0-SNAPSHOT-jar-with-dependencies.jar"
	var (namespace,perServerData,minKey,maxKey,jarpath) = ("perfTest256",10000,0,1000000,"target/scalaengine-1.1-SNAPSHOT-jar-with-dependencies.jar")
	val directorInfoFile = "/mnt/directorinfo"

	def setupCluster(scads_nodes:List[EC2Instance], num_fake:Int, replicas:Int):Future[Unit] = {
		Future {
			scads_nodes(0).executeCommand("rm -rf /mnt/zoo_data")
			scads_nodes.pforeach(n => {n.clearAll; n.stopWatches; n.executeCommand("killall java; rm -rf /mnt/scads_data")})
			val zoosvc = createJavaService(scads_nodes(0), new java.io.File(deploy_jarpath), "edu.berkeley.cs.scads.scale.cluster.ZooKeeperServer", 1024, "/mnt/zoo_data 2181")
			zoosvc.once
			Thread.sleep(30*1000)
			scads_nodes(0).blockTillPortOpen(2181)
			val nodeServices = scads_nodes.slice(1,scads_nodes.size).pmap(node=> createJavaService(node, new java.io.File(jarpath), "edu.berkeley.cs.scads.storage.ScalaEngine", 1024,"--port 9991 --zooKeeper "+scads_nodes(0).privateDnsName+":2181 --dbDir /mnt/scads_data") )
			nodeServices.pmap(_.once)
			Thread.sleep(30*1000)
			scads_nodes.slice(1,scads_nodes.size).pforeach(_.blockTillPortOpen(9991))

			val create = createJavaService(scads_nodes(0), new java.io.File(deploy_jarpath), "edu.berkeley.cs.scads.scale.cluster.SetupNodes", 256, "true " +scads_nodes(0).privateDnsName + " "+namespace)
			create.once
			scads_nodes(0).executeCommand("rm -rf /mnt/services/edu.berkeley.cs.scads.scale.cluster.SetupNodes")

			assert( (scads_nodes.size-1)%replicas == 0)
			val parts = (scads_nodes.size - 1)/replicas

			val partscsv = (0 until parts).map(p => {
				val reps = scads_nodes.slice(1+p*replicas,1+p*replicas+replicas)
				reps.map(node => ((p+1)+","+ node.privateDnsName))
			}).toList.flatten(b=>b).mkString("\n")
			scads_nodes(0).createFile(new java.io.File("/tmp/scads_config.dat"), partscsv)
			val policycsv = (1 to parts).toList.zip(createPartitions(minKey,perServerData*parts-1,parts)).map(entry=> entry._1+","+entry._2.minKey+","+entry._2.maxKey).mkString("\n")
			scads_nodes(0).createFile(new java.io.File("/tmp/scads_policy.dat"), policycsv)

			/*
			val partscsv = (1 until scads_nodes.size).pmap(part=> (part+","+scads_nodes(part).privateDnsName)).mkString("\n")
			scads_nodes(0).createFile(new java.io.File("/tmp/scads_config.dat"), partscsv)
			val policycsv = (1 until scads_nodes.size).pmap(part=> (part+","+scads_nodes(part).privateDnsName)).mkString("\n")
			scads_nodes(0).createFile(new java.io.File("/tmp/scads_policy.dat"), partscsv)
			*/
			val partition = createJavaService(scads_nodes(0), new java.io.File(deploy_jarpath), "edu.berkeley.cs.scads.scale.cluster.SetupNodes", 256, "false " +scads_nodes(0).privateDnsName + " "+namespace)
			partition.once
		}
	}

	def setupRatioAssociativeClients(clients_per_server:Int, c:List[RunitManager],s:List[EC2Instance]):Future[Unit] = {
		Future {
			//assert( c.size % s.size == 0, "clients must be multiple of num servers")
			(0 until s.size).foreach(i=>{
				val configcsv = s(i).privateDnsName+","+(i*perServerData)+","+((i+1)*perServerData)
				c.slice(i*clients_per_server,i*clients_per_server+clients_per_server).pmap(_.createFile(new java.io.File("/tmp/mapping.dat"), configcsv))
			})
		}
	}

	def setupFullyAssociatedClients(numReplicas:Int,c:List[RunitManager],s:List[EC2Instance]):Future[Unit] = {
		Future {
			assert(s.size % numReplicas == 0,"num servers must divide evenly into num replicas")
			c.pmap(_.executeCommand("rm /tmp/mapping_full.dat"))
			val configcsv = (0 until s.size/numReplicas).map(i=>{
				val replicas = s.slice(i*numReplicas,i*numReplicas+numReplicas)
				replicas.map(r=> (r.privateDnsName+","+(i*perServerData)+","+((i+1)*perServerData)))
			}).toList.flatten(b=>b).mkString("\n")
			c.pmap(_.createFile(new java.io.File("/tmp/mapping_full.dat"), configcsv))
		}
	}

	def doWarming(scads_nodes:List[EC2Instance],clients:List[RunitManager],replicas:Int,minKey:Int,maxKey:Int) = {
		//val s = scads_nodes.slice(1,scads_nodes.size)
		//val t = setupFullyAssociatedClients(replicas,clients,s)
		//t()
		logger.info("Cleaning up clients and uploading jar")
		clients.pmap(_.executeCommand("rm -rf /mnt/services/edu.berkeley.cs.scads.scale.cluster.StartRequestHandler"))
		clients.pmap(c=> c.services.pmap(_.clearFailures))
		clients.pmap(_.stopWatches)
		val partitions = createPartitions(minKey,maxKey-1, clients.size)
		val loadServices = partitions.zip(clients).pmap(p=> {
			createJavaService(p._2, new java.io.File(deploy_jarpath), "edu.berkeley.cs.scads.scale.cluster.StartRequestHandler", 1024, scads_nodes(0).privateDnsName+":2181 "+namespace+" true " +p._1.minKey+" "+p._1.maxKey+" "+replicas+" "+clients.size)
		})
		logger.info("Uploaded jar, deploying")
		loadServices.foreach(_.watchFailures)
		loadServices.foreach(_.once)
	}

	def runTest(active_clients:List[EC2Instance], zoo_dns:String, iteration:Int, maxKey:Int, kind:String) = {
		active_clients.pmap(_.executeCommand("rm -rf /mnt/services/edu.berkeley.cs.scads.scale.cluster.StartRequestHandler"))
	  val runService = active_clients.pmap(node=> createJavaService(node, new java.io.File(deploy_jarpath), "edu.berkeley.cs.scads.scale.cluster.StartRequestHandler", 1024, zoo_dns+" "+namespace+" false 0 "+maxKey+" 0 "+ active_clients.size))
		runService.pforeach(_.watchFailures)
		Thread.sleep(10*1000)
		println("Starting... "+(new java.util.Date).toString)
	  runService.pmap(_.once)
		println("Waiting for completion... "+(new java.util.Date).toString)
		runService.pforeach(_.blockTillDown)

		active_clients.pmap(n=> n.executeCommand("mv /tmp/requestlogs.csv /tmp/"+active_clients.size+"_"+iteration+"_"+kind+"_"+n.hostname+".csv"))
		active_clients.pmap(c=> c.services.pmap(_.clearFailures))
		active_clients.pmap(_.stopWatches)
	}

	def runFullTest(clients:List[EC2Instance],totalIters:Int,replicas:Int, zoo_dns:String,director:EC2Instance) = {
		val clientsPerServer = 2
		List(100,50,10,2,1).map(_*replicas).foreach(numClients=>{
		//List(50,25,5,1).map(_*replicas).foreach(numClients=>{
			//val t = setupFullyAssociatedClients(replicas,clients.slice(0,numClients),scads_nodes.slice(1,(numClients+1)))
			//t()
			val maxKey = numClients/replicas*perServerData
			Thread.sleep(30*1000)
			(1 to totalIters).foreach(iter=>{
				runTest(clients.slice(0,numClients*clientsPerServer),zoo_dns,iter,maxKey,"full"+replicas)
				Thread.sleep(20*1000)
				if (director != null) director.executeCommand("/usr/local/mysql/bin/mysqldump --databases metrics > /tmp/director/"+numClients+"_"+iter+".sql; /usr/local/mysql/bin/mysql -D metrics -e 'truncate scads;'")
			})
		})
	}


	def deployDirector(machine: RunitManager, cluster:List[EC2Instance],standbys:List[EC2Instance]) {
		val cluster_str = cluster.map(n=> n.privateDnsName.replaceAll(".compute-1.internal","")).mkString("\n")
		val standbys_str = standbys.map(n=> n.privateDnsName.replaceAll(".compute-1.internal","")).mkString("\n")
		val cluster_path = machine.rootDirectory.toString+"/cluster.txt"
		val standby_path = machine.rootDirectory.toString+"/standbys.txt"

		logger.info("Uploading server info to "+cluster_path+" and "+ standby_path)
		machine.createFile(new java.io.File(cluster_path), cluster_str)
		machine.blockTillFileCreated(new java.io.File(cluster_path))
		if (!standbys.isEmpty) {
			machine.createFile(new java.io.File(standby_path), standbys_str)
			machine.blockTillFileCreated(new java.io.File(standby_path))
		}
		else machine.executeCommand("touch "+ standby_path)
		machine.createFile(new java.io.File("/etc/ld.so.conf.d/mysql.conf/mysql.conf"), "/usr/local/mysql/lib/mysql")
		machine.blockTillFileCreated(new java.io.File("/etc/ld.so.conf.d/mysql.conf/mysql.conf"))
		machine.executeCommand("ldconfig")
	}

	def deployMonitoring(machine: RunitManager, experimentsJarURL:String, nBins:Int, minKey:Int, maxKey:Int, aggregationInterval:Long, getSampleProb:Double, putSampleProb:Double) {
		// set up chukwa collector and monitoring stuff
		logger.info("setting up monitoring recipes")
		val recipes = new JSONArray(); recipes.put("scads::monitoring"); recipes.put("chukwa::collector")
		val parserInfo = new JSONObject(); parserInfo.put("nBins",nBins); parserInfo.put("putSamplingProbability",putSampleProb); parserInfo.put("getSamplingProbability",getSampleProb); parserInfo.put("minKey",minKey); parserInfo.put("maxKey",maxKey); parserInfo.put("aggregationInterval",aggregationInterval);
		val metricInfo = new JSONObject(); metricInfo.put("port",6001); metricInfo.put("dbhost","localhost"); metricInfo.put("dbuser","root"); metricInfo.put("dbname","metrics"); metricInfo.put("dbpassword","");
		val monitoringInfo = new JSONObject(); 	monitoringInfo.put("basedir","/mnt/monitoring")
												monitoringInfo.put("experimentsJarURL",experimentsJarURL)
												monitoringInfo.put("metricService",metricInfo)
												monitoringInfo.put("xtraceParser",parserInfo)
		val monitoringCfg = new JSONObject(); monitoringCfg.put("recipes",recipes); monitoringCfg.put("monitoring",monitoringInfo)

		logger.info("Installing chukwa collector and monitoring")
		machine.createFile(new java.io.File("config.js"), monitoringCfg.toString)
		machine.blockTillFileCreated(new java.io.File("config.js"))
		machine.executeCommand("chef-solo -j config.js -r http://cs.berkeley.edu/~trush/cookbooks.tgz")
		logger.info("Waiting for monitoring port to be open")
		machine.blockTillPortOpen(6001) // wait until metric service is listening
	}

	def deployClientMachines(clients: List[RunitManager],chukwa_collector_dns:String) {
		// install chukwa on all machines
		logger.info("Installing chukwa on clients")
		val config = getXtraceIntoConfig(chukwa_collector_dns)
		val serverRecipes = new JSONArray()
		serverRecipes.put("chukwa::default")
		config.put("recipes", serverRecipes)
		clients.pmap(_.createFile(new java.io.File("config.js"), config.toString))
		clients.foreach(_.blockTillFileCreated(new java.io.File("config.js")))
		clients.pmap(_.executeCommand("chef-solo -j config.js"))
		clients.pforeach(_.blockTillPortOpen(7831)) // wait until chukwa is listening for xtrace reports

		logger.info("Writing collector/director dns to file")
		clients.pmap(_.createFile(new java.io.File(directorInfoFile), chukwa_collector_dns))
	}
	def getXtraceIntoConfig(collector_dns:String):JSONObject = {
		val adaptors = Array[String](
		        "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Top 15000 /usr/bin/top -b -n 1 -c 0",
		   "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Df 60000 /bin/df -x nfs -x none 0",
		   "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Sar 1000 /usr/bin/sar -q -r -n ALL 55 0",
		   "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Iostat 1000 /usr/bin/iostat -x -k 55 2 0",
		   "add edu.berkeley.chukwa_xtrace.XtrAdaptor XTrace TcpReportSource 0",
		   "add edu.berkeley.chukwa_xtrace.XtrAdaptor XTrace UdpReportSource 0"
		        )
		val xtrace_adaptors = new JSONArray(adaptors)
		val xtraceConfig = new JSONObject()
		xtraceConfig.put("adaptors",xtrace_adaptors)

		val serverConfig = new JSONObject()
	    val collector = Array[String](collector_dns)
	    val xtrace_collector= new JSONArray(collector)
	   	val addedConfig = xtraceConfig
	   	addedConfig.put("collectors",xtrace_collector)
	    serverConfig.put("chukwa",addedConfig);
	}

	def createPartitions(startKey: Int, endKey: Int, numPartitions: Int):List[PolicyRange] = {
		val partitions = new scala.collection.mutable.ArrayStack[PolicyRange]()
		val numKeys = endKey - startKey + 1

		val last = (startKey to endKey by (numKeys/numPartitions)).toList.reduceLeft((s,e) => {
			partitions.push(PolicyRange(s,e))
			e
		})
		partitions.push(PolicyRange(last, numKeys + startKey))

		return partitions.toList.reverse
	}
}
// val d = Future {new DirectorRunExp(director,"MultiRangePolicyWithQuorum",(5*60*1000).toString)}
// val c = Future{ClusterDeployment.runTest(clients,scads_nodes(0).privateDnsName+":2181",1,20000,"test")}
class DirectorRunExp(director:RunitManager, policy:String, experimentDuration:String) {
	val logger = Logger.getLogger("scads.director")
	val directorArgs = scala.collection.mutable.Map[String,String]()
	val (bootupTime, machineInterval, serverRemoveTime) = (48*1000L,10*60*1000L,120*1000L)			// replaying workload at rate 1:36

	director.stopWatches
	director.executeCommand("rm -rf /mnt/services/scads.director")

	directorArgs += "modelPath" -> "\"/opt/scads/director/scripts/perfmodels/gp_model2_thr.csv\""
	directorArgs += "getSLA" -> "100"
	directorArgs += "putSLA" -> "150"
	directorArgs += "slaInterval" -> (5*60*1000).toString
	directorArgs += "slaCost" -> "100"
	directorArgs += "slaQuantile" -> "0.99"
	directorArgs += "machineInterval" -> machineInterval.toString
	directorArgs += "bootupTime" -> bootupTime.toString
	directorArgs += "serverRemoveTime" -> serverRemoveTime.toString
	directorArgs += "machineCost" -> "1"
	directorArgs += "costSkip" -> (10*60*1000).toString
	directorArgs += "runDirector" -> "true"
	//directorArgs += "actionExecutor" -> "FixedRatePerServerActionExecutor"

	directorArgs += "duration" -> experimentDuration
	directorArgs += "policyName" -> policy
	directorArgs += "hysteresisUp" -> "0.9"
	directorArgs += "hysteresisDown" -> "0.1"
	directorArgs += "overprovisioning" -> System.getProperty("overp","0.0")
	directorArgs += "nHotStandbys" -> System.getProperty("policyHotStandbys","0")
	directorArgs += "blocking" -> System.getProperty("blocking","true")
	directorArgs += "aeRate" -> System.getProperty("aeRate","200")

	val experimentName = System.getenv("AWS_KEY_NAME")+ "_"+policy/*+"_"+ae+"_rate"+aeRate+"_nHS"+policyHotStandbys+"_"+hystup+"_"+hystdown+"_"+overprov+"_"+filename+"_"*/+System.currentTimeMillis
	directorArgs += "deploymentName" -> experimentName
	directorArgs += "experimentName" -> experimentName

	val directorCmd = new StringBuilder("java -server -noverify ")
	directorCmd.append(directorArgs.map( a => "-D"+a._1+"="+a._2 ).mkString(" "))
	directorCmd.append(" -cp /mnt/monitoring/experiments.jar")
	directorCmd.append(" scads.director.RunDirector")
	/*val directorCmd = "java -server -noverify " + directorArgs.map( a => "-D"+a._1+"="+a._2 ).mkString(" ") +
						" -cp /mnt/monitoring/experiments.jar" +
						" scads.director.RunDirector"*/
	val service = ClusterDeployment.createRunitService(director, "scads.director", directorCmd.toString)
	service.watchFailures
	service.once

}
