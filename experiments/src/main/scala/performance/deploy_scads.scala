package performance
 
import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._

import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.thrift.{KnobbedDataPlacementServer,DataPlacement, RangeConversion}
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}

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
	def init(num_c: Int) = {
		num_clients = num_c
		clients = Scads.startNodes(num_clients,"client")		// start up clients
		
		// TODO: wait for machines to come online (FIX)
		Thread.sleep(30 * 1000)
		
		println("Deploying clients.")
		var deployedClients = false		
		while (!deployedClients) {
			try {
				clients.parallelMap(_.deploy(clientConfig))
				deployedClients = true
			} catch {
				case e: Exception => { println("can't deploy to clients, waiting 1 second"); Thread.sleep(1000) }
			}
		}
	    println("Done deploying clients.")

		// get list of mvn dependencies
		deps = clients.get(0).exec("cd /opt/scads/experiments; cat cplist").getStdout.replace("\n","") + ":../target/classes"
	}
	def run_workload(read_prob: Double, ns: String, totalUsers: Int, delay: Int, think: Int) {
		// determine ranges to give each client
		assert( totalUsers % clients.size == 0, "deploy_scads: can't evenly divide number of users amongst client instances")

		// set up threads to run clients in parallel with appropriate min and max settings
		val threads = (0 to clients.size-1).toList.map((id)=>{
			val minUser = id * (totalUsers/clients.size)
			val maxUser = minUser + (totalUsers/clients.size) - 1

			val args = Scads.placement.get(0).privateDnsName +" "+ Scads.xtrace_on + " " + minUser + " " + maxUser + " " +
						read_prob + " " + ns + " " + totalUsers + " " + delay + " " + think
			val cmd = "cd /opt/scads/experiments/scripts; scala -cp "+ deps + " run_workload.scala " + args
			println("Running with arguments: "+ args)
			new Thread( new ClientRequest(clients.get(id), cmd) )
		})
		for(thread <- threads) thread.start
		for(thread <- threads) thread.join

		// TODO: check dependencies string is valid?
		//clients.parallelMap((c: Instance)=>c.exec(cmd))
	}
	case class ClientRequest(client: Instance, cmd: String) extends Runnable {
		override def run() = {
			client.exec(cmd)
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
			println(cmd)
			c.exec(cmd) 
			c.exec( "rm -f /mnt/xtrace/logs/*" )
		}
		
		val expDir = "/mnt/logs/"+experimentName
		val sourceF = expDir+"/"+experimentName+".log"
		client0.exec( "cat /mnt/logs/"+experimentName+"/clients/* > "+sourceF)
		client0.exec( "ulimit -n 20000" )
		client0.exec( "scala /opt/scads/experiments/scripts/parselogs.scala "+sourceF+" "+expDir+"/"+experimentName+"_agg1.csv 1000 0.2")
		client0.exec( "scala /opt/scads/experiments/scripts/parselogs.scala "+sourceF+" "+expDir+"/"+experimentName+"_agg5.csv 5000 0.2")
		client0.exec( "scala /opt/scads/experiments/scripts/parselogs.scala "+sourceF+" "+expDir+"/"+experimentName+"_agg10.csv 10000 0.2")
	}
}

object Scads extends RangeConversion {
	var dpclient:KnobbedDataPlacementServer.Client = null
	var servers:InstanceGroup = null
	var placement:InstanceGroup = null
	var reporter:InstanceGroup = null
	
	// these should be commnand-line args or system properties or something
	val num_servers = 1
	val num_reporters = 1
	val num_placement = 1
	val xtrace_on:Boolean = false
	var reporter_host:String = null
	val namespaces = List[String]("perfTest200")
	val server_port = 9000
	val server_sync = 9091
	
	def runInstances(count: Int, typeString: String): InstanceGroup = {
		val imageId = InstanceType.bits(typeString) match {
			case 32 => "ami-e7a2448e"
       		case 64 => "ami-e4a2448d"
     	}
/*     	val keyName = "trush"
     	val keyPath = "/Users/trush/.ec2/trush.key"
*/		val keyName = "bodikp-keypair"
     	val keyPath = "/Users/bodikp/.ec2/bodikp-keypair"
     	val location = "us-east-1a"

     	DataCenter.runInstances(imageId, count, keyName, keyPath, typeString, location)
 	}
	
	def startNodes(count: Int, name: String): InstanceGroup = {
		println("Requesting "+name+" instances.")
	    val nodes = runInstances(count, "m1.small")
	    println("Instances "+name+" received.")

	    println("Waiting on "+name+" instances to be ready.")
	    nodes.parallelMap((instance) => instance.waitUntilReady)
	    println("Instances "+name+" ready.")
		nodes
	}
	
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
	
	def run() {
		reporter = if (xtrace_on) { startNodes(num_reporters,"reporter") } else { null } // start up xtrace reporter
		reporter_host = if (reporter != null) { reporter.get(0).privateDnsName } else { null }
		servers = startNodes(num_servers,"server")					// start up servers
		placement = startNodes(num_placement,"placement")			// start up data placement	
	
		// TODO: wait for machines to come online (FIX)
		Thread.sleep(30 * 1000)
	
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

		if (reporter_host != null) {
			println("Deploying xtrace reporter.")
			reporter.parallelMap(_.deploy(reporterConfig))
		    println("Done deploying reporter.")
		}
		
		println("Deploying placement.")
		val placementDeployResult = placement.parallelMap(_.deploy(placementConfig))
		placementDeployResult.map( (x:ExecuteResponse) => {println(x.getStdout); println(x.getStderr)} )
	    println("Done deploying placement.")
	
	    println("Deploying servers.")
		val serverDeployResult = servers.parallelMap(_.deploy(serverConfig))
		serverDeployResult.map( (x:ExecuteResponse) => {println(x.getStdout); println(x.getStderr)} )
	    println("Done deploying servers.")

		val list = new java.util.ArrayList[DataPlacement]()
		list.add(new DataPlacement(servers.get(0).privateDnsName,server_port,server_sync,KeyRange(MinKey,MaxKey)))

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