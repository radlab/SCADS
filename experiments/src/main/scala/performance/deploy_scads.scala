package performance
 
import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._

import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.thrift.{KnobbedDataPlacementServer,DataPlacement, RangeConversion}
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}

object ScadsDP extends RangeConversion {
	assert (Scads.dpclient != null) // need to Scads.run first

	def shift = {
		// grab an assignments to storage nodes
		val dp1 = Scads.dpclient.lookup_node(Scads.namespaces(0),Scads.servers.get(0).privateDnsName,Scads.server_port,Scads.server_sync)

		// copy storage node's data to another node (adjacent in key responsibility)
		println(System.currentTimeMillis())
		Scads.dpclient.move(Scads.namespaces(0),dp1.rset, dp1.node, Scads.server_port,Scads.server_sync,Scads.servers.get(1).privateDnsName, Scads.server_port,Scads.server_sync)
		println(System.currentTimeMillis())
	}
}

object ScadsClients {
	val host:String = Scads.placement.get(0).privateDnsName
	val port:Int = 8000
	var num_clients = 1
	val xtrace_on:Boolean = Scads.xtrace_on
	val namespaces = Scads.namespaces
	var deps:String = null
	
	val xtrace_backend = new JSONObject()
	xtrace_backend.put("backend_host", Scads.reporter_host)
	
	val clientConfig = new JSONObject()
    val clientRecipes = new JSONArray()
    clientRecipes.put("scads::client_library")
	if (Scads.reporter_host != null) { clientConfig.put("xtrace",xtrace_backend); clientRecipes.put("xtrace::reporting"); }
    clientConfig.put("recipes", clientRecipes)
	
	var clients:InstanceGroup = null
	
	def loadState() = {
		clients = DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+Scads.getName+"--client" )
		deps = clients.get(0).exec("cd /opt/scads/experiments; cat cplist").getStdout.replace("\n","") + ":../target/classes"
	}
	
	def init(num_c: Int) = {
		num_clients = num_c
		clients = DataCenter.runInstances(num_clients,"c1.medium")		// start up clients
		clients.waitUntilReady
		
		clients.tagWith( DataCenter.keyName+"--SCADS--"+Scads.getName+"--client" )
		println("Deploying clients.")
		clients.deploy(clientConfig)
	    println("Done deploying clients.")

		// get list of mvn dependencies
		deps = clients.get(0).exec("cd /opt/scads/experiments; cat cplist").getStdout.replace("\n","") + ":../target/classes"
	}
	def warm_cache(ns: String, minK:Int, maxK:Int) = {
		println("Warming server caches.")
		val cmd = "cd /opt/scads/experiments/scripts; scala -cp "+ deps + " warm_cache.scala " + 
					Scads.placement.get(0).privateDnsName + " "+ ns +" " + minK + " " + maxK
		clients.get(0).exec(cmd)
		println("Done warming.")
	}

	def run_workload(read_prob: Double, ns: String, totalUsers: Int, delay: Int, think: Int) {
		val testthread = new Thread(new WorkloadRunner(read_prob,ns, totalUsers, delay, think))
		testthread.start
	}
	case class WorkloadRunner(read_prob: Double, ns: String, totalUsers: Int, delay: Int, think: Int) extends Runnable {
		def run() = {
			// determine ranges to give each client
			assert( totalUsers % clients.size == 0, "deploy_scads: can't evenly divide number of users amongst client instances")

			// set up threads to run clients in parallel with appropriate min and max settings
			var commands = Map[Instance, String]()
			val threads = (0 to clients.size-1).toList.map((id)=>{
				val minUser = id * (totalUsers/clients.size)
				val maxUser = minUser + (totalUsers/clients.size) - 1

				val args = Scads.placement.get(0).privateDnsName +" "+ Scads.xtrace_on + " " + minUser + " " + maxUser + " " +
							read_prob + " " + ns + " " + totalUsers + " " + delay + " " + think
				val cmd = "cd /opt/scads/experiments/scripts; scala -cp "+ deps + " run_workload.scala " + args
				commands += (clients.get(id) -> cmd)
				println("Will run with arguments: "+ args)
				new Thread( new ClientRequest(clients.get(id), cmd) )
			})
			for(thread <- threads) thread.start
			for(thread <- threads) thread.join
			println("Workload test complete.")

			// is this broken? :(
			//clients.parallelMap((c: Instance) => c.exec( commands(c) ))
		}
	}
	case class ClientRequest(client: Instance, cmd: String) extends Runnable {
		override def run() = {
			client.exec(cmd)
		}
	}
	
	def startWorkload(workload:WorkloadDescription, totalUsers:Int) {
		val workloadFile = "/tmp/workload.ser"
		workload.serialize(workloadFile)
		
		// determine ranges to give each client
		assert( totalUsers % clients.size == 0, "deploy_scads: can't evenly divide number of users amongst client instances")

		var commands = Map[Instance, String]()
		val threads = (0 to clients.size-1).toList.map((id)=>{
			val minUser = id * (totalUsers/clients.size)
			val maxUser = minUser + (totalUsers/clients.size) - 1

			val args = Scads.placement.get(0).privateDnsName +" "+ Scads.xtrace_on + " " + minUser + " " + maxUser + " " + workloadFile
			val cmd = "cd /opt/scads/experiments/scripts; scala -cp "+ deps + " startWorkload.scala " + args + " &> /tmp/workload.log"
			commands += (clients.get(id) -> cmd)
			println("Will run with arguments: "+ args)
			println("full cmd: " + cmd )
			new Thread( new StartWorkloadRequest(clients.get(id), workloadFile, cmd) )
		})
		for(thread <- threads) thread.start
		for(thread <- threads) thread.join		
	}
	case class StartWorkloadRequest(client: Instance, workloadFile: String, cmd: String) extends Runnable {
		override def run() = {
			client.upload(Array(workloadFile),"/tmp/")
			val result = client.exec(cmd)
			result
		}
	}
	
	def processLogFiles(experimentName:String) {
		val client0 = clients.get(0)
		val targetIP = client0.privateDnsName
		client0.exec( "mkdir -p /mnt/logs/"+experimentName+"/clients/" )
		
		for (c <- clients) { 
			val f=experimentName+"_"+c.privateDnsName+".log"
			val df="/tmp/"+f
			val cmd="cat /mnt/xtrace/logs/* > "+f+" && scp -o StrictHostKeyChecking=no "+f+" "+targetIP+":/mnt/logs/"+experimentName+"/clients/"+f
			//println(cmd)
			c.exec(cmd) 
			c.exec( "rm -f /mnt/xtrace/logs/*" )
		}
		
		val expDir = "/mnt/logs/"+experimentName
		val sourceF = expDir+"/"+experimentName+".log"
		client0.exec( "cat /mnt/logs/"+experimentName+"/clients/* > "+sourceF)
		client0.exec( "ulimit -n 20000" )
				
		for (i <- List(1,5,10)) {
			client0.exec( "scala /opt/scads/experiments/scripts/parselogs.scala "+sourceF+" "+expDir+"/"+experimentName+"_agg"+i+".csv "+i*1000+" 0.2")
			client0.exec( "echo \"source('/opt/scads/experiments/scripts/process.R'); pdf('"+expDir+"/"+experimentName+"_agg"+i+".pdf',width=10,height=15); plot.stats.for.file('"+expDir+"/"+experimentName+"_agg"+i+".csv') \" | R --vanilla")
		}
		
	}
}

object Scads extends RangeConversion {
	val keyFormat = new java.text.DecimalFormat("000000000000000")
	var dpclient:KnobbedDataPlacementServer.Client = null
	var servers:InstanceGroup = null
	var placement:InstanceGroup = null
	var reporter:InstanceGroup = null
	
	// name of the SCADS instance
	var scadsName = ""
	
	// these should be commnand-line args or system properties or something
	var num_servers = 1
	val num_reporters = 1
	val num_placement = 1
	val xtrace_on:Boolean = false
	var reporter_host:String = null
	val namespaces = List[String]("perfTest200")
	val server_port = 9000
	val server_sync = 9091
	
	def getName(): String = scadsName
	
	def getDataPlacementHandle(h:String, p: Int):KnobbedDataPlacementServer.Client = {
		var haveDPHandle = false		
		while (!haveDPHandle) {
			try {
				val transport = new TFramedTransport(new TSocket(h, p))
		   		val protocol = if (Scads.xtrace_on) {new XtBinaryProtocol(transport)} else {new TBinaryProtocol(transport)}
		   		dpclient = new KnobbedDataPlacementServer.Client(protocol)
				transport.open()
				haveDPHandle = true
			} catch {
				case e: Exception => { println("don't have connection to placement server, waiting 1 second"); Thread.sleep(1000) }
			}
		}
		dpclient
	}
	
	def loadState(name:String) {
		scadsName = name

		reporter = DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+scadsName+"--reporter" )
		servers = DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+scadsName+"--storagenode" )
		placement = DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+scadsName+"--placement" )
		
		reporter_host = if (reporter != null) { reporter.get(0).privateDnsName } else { null }
		
		dpclient = Scads.getDataPlacementHandle(placement.get(0).publicDnsName, 8000)
	}
	
	def run(name:String, num_s: Int, minK: Int, maxK: Int) {
		scadsName = name
		num_servers = num_s
		assert( (maxK-minK)%num_servers==0,"deploy_scads: key space isn't evenly divisible by number of servers" )

		reporter = if (xtrace_on) { DataCenter.runInstances(num_reporters, "m1.small") } else { null } // start up xtrace reporter
		reporter_host = if (reporter != null) { reporter.get(0).privateDnsName } else { null }
		servers = DataCenter.runInstances(num_servers,"m1.small")					// start up servers
		placement = DataCenter.runInstances(num_placement,"m1.small")			// start up data placement
		
		val groups = if (reporter!=null) Array(servers,placement,reporter) else Array(servers,placement)
		new InstanceGroup(groups).waitUntilReady
	
		if (reporter!=null) reporter.tagWith( DataCenter.keyName+"--SCADS--"+scadsName+"--reporter" )
		servers.tagWith( DataCenter.keyName+"--SCADS--"+scadsName+"--storagenode" )
		placement.tagWith( DataCenter.keyName+"--SCADS--"+scadsName+"--placement" )
	
		val xtrace_backend = new JSONObject()
		xtrace_backend.put("backend_host", reporter_host)
		val scads_xtrace = new JSONObject()
		scads_xtrace.put("xtrace","-x")
			
		// set up chef scripts and config objects
		val reporterConfig = new JSONObject()
		val reporterRecipes = new JSONArray()
		reporterRecipes.put("xtrace::reporting")
		reporterConfig.put("recipes",reporterRecipes)
		
		val serverConfig = new JSONObject()
	    val serverRecipes = new JSONArray()
		serverRecipes.put("scads::dbs")
	    serverRecipes.put("scads::storage_engine")
/*		serverRecipes.put("ec2::disk_prep")*/
		if (reporter_host != null) { serverConfig.put("scads",scads_xtrace); serverConfig.put("xtrace",xtrace_backend);; serverRecipes.put("xtrace::reporting"); }
	    serverConfig.put("recipes", serverRecipes)
	
		val placementConfig = new JSONObject()
	    val placementRecipes = new JSONArray()
	    placementRecipes.put("scads::data_placement")
		if (reporter_host != null) { placementConfig.put("scads",scads_xtrace); placementConfig.put("xtrace",xtrace_backend); placementRecipes.put("xtrace::reporting"); }
	    placementConfig.put("recipes", placementRecipes)


		println("deploying services")
		val reporterDeployWait = if (reporter_host!=null) reporter.deployNonBlocking(reporterConfig) else null
		val placementDeployWait = placement.deployNonBlocking(placementConfig)
		val serversDeployWait = servers.deployNonBlocking(serverConfig)
		
		println("waiting for deployment to finish")
		if (reporterDeployWait!=null) reporterDeployWait()
		val placementDeployResult = placementDeployWait()
		val serverDeployResult = serversDeployWait()

		println("deployed!")
		println("placement deploy log: "); placementDeployResult.foreach( (x:ExecuteResponse) => {println(x.getStdout); println(x.getStderr)} )
	    println("server deploy log: "); serverDeployResult.foreach( (x:ExecuteResponse) => {println(x.getStdout); println(x.getStderr)} )


		// set up partitioned storage node servers
		val slice = (maxK-minK)/servers.size
		val list = new java.util.ArrayList[DataPlacement]()
		(0 to servers.size-1).toList.map((id)=>{
			val startnum = id * slice
			val start = new StringKey( Scads.keyFormat.format(startnum) )
			val end =  new StringKey( Scads.keyFormat.format(startnum + slice))
			//val start = if (startnum == minK) { MinKey } else { new StringKey( Scads.keyFormat.format(startnum) ) }
			//val end = if ( (startnum + slice) >= maxK) { MaxKey } else { new StringKey( Scads.keyFormat.format(startnum + slice)) }
			println("Adding server to data placement list: "+ servers.get(id).privateDnsName + ": "+ start +" - "+ end)
			list.add(new DataPlacement(servers.get(id).privateDnsName,server_port,server_sync,KeyRange(start,end)))
		})
		dpclient = Scads.getDataPlacementHandle(placement.get(0).publicDnsName, 8000)
		
		var callSuccessful = false		
		while (!callSuccessful) {
			try {
				namespaces.foreach((ns)=> dpclient.add(ns,list) )
				callSuccessful = true
			} catch {
				case e: Exception => { 
					println("thrift \"add\" call failed, waiting 1 second"); 
					Thread.sleep(1000) 
					dpclient = Scads.getDataPlacementHandle(placement.get(0).publicDnsName, 8000)					
				}
			}
		}
		
		println("SCADS running")
	}
	
	def retry( call: => Any, recovery: => Unit, maxNRetries: Int, delay: Long ) {
		var returnValue: Any = null
		var done = false
		var nRetries = 0
		while (!done && nRetries<maxNRetries) {
			try {
				returnValue = call
				done = true
			} catch {
				case e: Exception => { 
					println("exception; will wait and retry")
					Thread.sleep(delay)
					recovery
					nRetries += 1
				}
			}
		}
		returnValue
	}
}