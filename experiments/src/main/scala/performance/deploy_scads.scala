package performance
 
import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._

import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.thrift.{RangeSet,RecordSet,KnobbedDataPlacementServer,DataPlacement, RangeConversion}
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}

object Scads {
	val keyFormat = new java.text.DecimalFormat("000000000000000")
	val server_port = 9000
	val server_sync = 9091
	val dp_port = 8000

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

	def getXtraceConfig = xtraceConfig

	def getDataPlacementHandle(h:String,xtrace_on:Boolean):KnobbedDataPlacementServer.Client = {
		val p = Scads.dp_port
		var haveDPHandle = false
		var dpclient:KnobbedDataPlacementServer.Client = null
		while (!haveDPHandle) {
			try {
				val transport = new TFramedTransport(new TSocket(h, p))
		   		val protocol = if (xtrace_on) {new XtBinaryProtocol(transport)} else {new TBinaryProtocol(transport)}
		   		dpclient = new KnobbedDataPlacementServer.Client(protocol)
				transport.open()
				haveDPHandle = true
			} catch {
				case e: Exception => { println("don't have connection to placement server, waiting 1 second"); Thread.sleep(1000) }
			}
		}
		dpclient
	}

	def getNumericKey(key:String) = {
		key.toInt
	}
}

case class ScadsDP(h:String, xtrace_on: Boolean, namespace: String) extends RangeConversion {
	var dpclient:KnobbedDataPlacementServer.Client = null

	val range = new RangeSet()
	range.setStart_key("'000000000000005'")
	range.setEnd_key("'000000000005000'")
	val rset = new RecordSet(3,range,null,null)

	def refreshHandle = {
		try {
			dpclient.lookup_namespace(namespace)
		} catch {
			case e: Exception => {
				println("dp client handle is shmetted, getting new one")
				dpclient = Scads.getDataPlacementHandle(h,xtrace_on)
			}
		}
	}

	def shift(first:String, second: String):Long = {
		// move storage node's data to another node (adjacent in key responsibility)
		val startms = System.currentTimeMillis()
		dpclient.move(namespace,rset, first, Scads.server_port,Scads.server_sync, second, Scads.server_port,Scads.server_sync)
		System.currentTimeMillis()-startms
	}
	def shift_back(first:String, second: String):Long = {
		// move storage node's data to another node (adjacent in key responsibility)
		val startms = System.currentTimeMillis()
		dpclient.move(namespace,rset, second, Scads.server_port,Scads.server_sync, first, Scads.server_port,Scads.server_sync)
		System.currentTimeMillis()-startms
	}
	def copy_from_one(source_host:String, target_host: String):Long = {
		val range = new RangeSet()
		range.setStart_key("'000000000100000'")
		  range.setEnd_key("'000000000200000'")
		val one_rset = new RecordSet(3,range,null,null)

		//val dpclient = Scads.getDataPlacementHandle(myscads.placement.get(0).publicDnsName,xtrace_on)
		val startms = System.currentTimeMillis()
		dpclient.copy(namespace,one_rset, source_host, Scads.server_port,Scads.server_sync, target_host, Scads.server_port,Scads.server_sync)
		System.currentTimeMillis()-startms
	}
/*
	def copy_from_two(source_host:String, target_host: String):Long = {
		val range1 = new RangeSet()
		range1.setStart_key("'000000000010000'")
		range1.setEnd_key("'000000000025600'")
		val two_rset1 = new RecordSet(3,range1,null,null)
		val range2 = new RangeSet()
		range2.setStart_key("'000000000025600'")
		range2.setEnd_key("'000000000051200'")
		val two_rset2 = new RecordSet(3,range2,null,null)

		val start1 = System.currentTimeMillis()
		Scads.dpclient.copy(Scads.namespaces(0),two_rset1, Scads.servers.get(0).privateDnsName, Scads.server_port,Scads.server_sync,
							target_host, Scads.server_port,Scads.server_sync)

		val start2=System.currentTimeMillis()
		Scads.dpclient.copy(Scads.namespaces(0),two_rset2, Scads.servers.get(1).privateDnsName, Scads.server_port,Scads.server_sync,
							target_host, Scads.server_port,Scads.server_sync)
		println(System.currentTimeMillis()-start2)
		println(start2-start1)
	}
*/
}

case class ScadsClients(scadsName: String,host:String, xtrace_on: Boolean, namespace: String) {
	var deps:String = null
	var clients:InstanceGroup = null
	
	val clientConfig = new JSONObject()
    val clientRecipes = new JSONArray()
    clientRecipes.put("scads::client_library")
	if (xtrace_on) { clientConfig.put("chukwa",Scads.getXtraceConfig); clientRecipes.put("chukwa::default"); }
    clientConfig.put("recipes", clientRecipes)
	
	def getName(): String = scadsName
	
	def loadState() = {
		clients = DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+getName+"--client", true )
		deps = clients.get(0).exec("cd /opt/scads/experiments; cat cplist").getStdout.replace("\n","") + ":../target/classes"
	}
	
	def init(num_clients:Int) = {
		clients = DataCenter.runInstances(num_clients,"c1.medium")		// start up clients
		clients.waitUntilReady
		
		clients.tagWith( DataCenter.keyName+"--SCADS--"+getName+"--client" )
		println("Deploying clients.")
		clients.deploy(clientConfig)
	    println("Done deploying clients.")

		// get list of mvn dependencies
		deps = clients.get(0).exec("cd /opt/scads/experiments; cat cplist").getStdout.replace("\n","") + ":../target/classes"
	}
	def warm_cache(ns: String, minK:Int, maxK:Int) = {
		println("Warming server caches.")
		val cmd = "cd /opt/scads/experiments/scripts; scala -cp "+ deps + " warm_cache.scala " + 
					host + " "+ ns +" " + minK + " " + maxK +" "+xtrace_on
		clients.get(0).exec(cmd)
		println("Done warming.")
	}

	def startWorkload_NB(workload:WorkloadDescription) = {
		val testthread = new Thread(new WorkloadDescRunner(workload))
		testthread.start
	}
	
	case class WorkloadDescRunner(workload:WorkloadDescription) extends Runnable {
		def run() = {
			startWorkload(workload)
			println("Workload test complete.")
		}
	}

	def startWorkload(workload:WorkloadDescription) {
		val totalUsers = workload.getMaxNUsers
		val workloadFile = "/tmp/workload.ser"
		workload.serialize(workloadFile)
		
		// determine ranges to give each client
		assert( totalUsers % clients.size == 0, "deploy_scads: can't evenly divide number of users amongst client instances")

		var commands = Map[Instance, String]()
		val threads = (0 to clients.size-1).toList.map((id)=>{
			val minUser = id * (totalUsers/clients.size)
			val maxUser = minUser + (totalUsers/clients.size) - 1

			val args = host +" "+ xtrace_on + " " + minUser + " " + maxUser + " " + workloadFile
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
			client0.exec( "scala /opt/scads/experiments/scripts/parselogs.scala "+sourceF+" "+expDir+"/"+experimentName+"_agg"+i+".csv "+i*1000+" 1")
			client0.exec( "echo \"source('/opt/scads/experiments/scripts/process.R'); pdf('"+expDir+"/"+experimentName+"_agg"+i+".pdf',width=10,height=15); plot.stats.for.file('"+expDir+"/"+experimentName+"_agg"+i+".csv') \" | R --vanilla")
		}
		
	}
}

case class Scads(scadsName: String, xtrace_on: Boolean, namespace: String) extends RangeConversion {
	var servers:InstanceGroup = new InstanceGroup
	var placement:InstanceGroup = null
	var dpclient:KnobbedDataPlacementServer.Client = null
	
	val scads_xtrace = new JSONObject()
	scads_xtrace.put("xtrace","-x")
	val dataRecipe = new JSONArray()
	dataRecipe.put("scads::dbs")
	val dataConfig = new JSONObject()
	dataConfig.put("recipes", dataRecipe)
	
	def init(num_servers: Int) = {
		servers = DataCenter.runInstances(num_servers,"m1.small")					// start up servers
		placement = DataCenter.runInstances(1,"m1.small")			// start up data placement
		
		val groups =  Array(servers,placement)
		new InstanceGroup(groups).waitUntilReady
	
		servers.tagWith( DataCenter.keyName+"--SCADS--"+scadsName+"--storagenode" )
		placement.tagWith( DataCenter.keyName+"--SCADS--"+scadsName+"--placement" )

		val serverConfig = new JSONObject()
	    val serverRecipes = new JSONArray()
	    serverRecipes.put("scads::storage_engine")
/*		serverRecipes.put("ec2::disk_prep")*/
		if (xtrace_on) { serverConfig.put("scads",scads_xtrace); serverConfig.put("chukwa",Scads.getXtraceConfig); serverRecipes.put("chukwa::default"); }
	    serverConfig.put("recipes", serverRecipes)

		val placementConfig = new JSONObject()
	    val placementRecipes = new JSONArray()
	    placementRecipes.put("scads::data_placement")
		if (xtrace_on) { placementConfig.put("scads",scads_xtrace); placementConfig.put("chukwa",Scads.getXtraceConfig); placementRecipes.put("chukwa::default"); }
	    placementConfig.put("recipes", placementRecipes)

		println("deploying services")
		val placementDeployWait = placement.deployNonBlocking(placementConfig)
		val serversDeployWait = servers.deployNonBlocking(serverConfig)
		
		println("waiting for deployment to finish")
		val placementDeployResult = placementDeployWait()
		val serverDeployResult = serversDeployWait()

		println("deployed!")
		println("placement deploy log: "); placementDeployResult.foreach( (x:ExecuteResponse) => {println(x.getStdout); println(x.getStderr)} )
	    println("server deploy log: "); serverDeployResult.foreach( (x:ExecuteResponse) => {println(x.getStdout); println(x.getStderr)} )
	}

	def shutdownServer(host:String) = { // note: this does not remove from data placement!
		(0 to servers.size-1).toList.map((id)=>{
			if (servers.get(id).privateDnsName == host || servers.get(id).publicDnsName == host) {
				val machine:Instance = servers.get(id)
				println("Shutting down "+host)
				machine.stop
				println("Terminated "+host)
			}
			else println("No machine with hostname "+host+" found.")
		})
	}

	def addServers(num:Int) = {
		val new_servers = DataCenter.runInstances(num,"m1.small")
		new_servers.waitUntilReady
		new_servers.tagWith( DataCenter.keyName+"--SCADS--"+scadsName+"--storagenode" )

		val serverConfig = new JSONObject()
	    val serverRecipes = new JSONArray()
	    serverRecipes.put("scads::storage_engine")
		if (xtrace_on) { serverConfig.put("scads",scads_xtrace); serverConfig.put("chukwa",Scads.getXtraceConfig); serverRecipes.put("chukwa::default"); }
	    serverConfig.put("recipes", serverRecipes)

		println("deploying services")
		val serversDeployWait = new_servers.deployNonBlocking(serverConfig)

		println("waiting for deployment to finish")
		val serverDeployResult = serversDeployWait()
		println("deployed!")
	    println("server deploy log: "); serverDeployResult.foreach( (x:ExecuteResponse) => {println(x.getStdout); println(x.getStderr)} )

		servers.addAll(new_servers)
	}

	def loadState():Boolean = {
		servers = DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+scadsName+"--storagenode", true )
		placement = DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+scadsName+"--placement", true )
		if (placement.size > 0) {
			dpclient = Scads.getDataPlacementHandle(placement.get(0).publicDnsName, xtrace_on)
			true
		}
		else false
	}

	def deployData(server:Instance) = {
		server.exec("sv stop /mnt/services/scads_bdb_storage_engine")
		server.exec("rm -rf /mnt/scads/data")
		println("deploying default data")
		val serversDeployWait = server.deployNonBlocking(dataConfig)
		val serverDeployResult = serversDeployWait.value
		server.exec("sv start /mnt/services/scads_bdb_storage_engine")
		println("deployed!")
		println("server data deploy log: "); println(serverDeployResult.getStdout); println(serverDeployResult.getStderr)
	}

	def replicate(minK: Int, maxK: Int) = {
		// have first server download the data, and assign the range with the placement server
		deployData(servers.get(0))
		val list = new java.util.ArrayList[DataPlacement]()
		val start = new StringKey( Scads.keyFormat.format(minK) )
		val end = new StringKey( Scads.keyFormat.format(maxK) )
		val range = KeyRange(start,end)
		list.add(new DataPlacement(servers.get(0).privateDnsName,Scads.server_port,Scads.server_sync,range))
		setServers(list)

		// copy the data from the first server to all others
		(1 to servers.size-1).toList.map((id)=>{
			val dp = Scads.getDataPlacementHandle(placement.get(0).publicDnsName,xtrace_on)
			println("Copying to: "+ servers.get(id).privateDnsName + ": "+ start +" - "+ end)
			dp.copy(namespace,range,servers.get(0).privateDnsName,Scads.server_port,Scads.server_sync,
									servers.get(id).privateDnsName,Scads.server_port,Scads.server_sync)
		})
		dpclient = Scads.getDataPlacementHandle(placement.get(0).publicDnsName,xtrace_on)
		if (dpclient.lookup_namespace(namespace).size != servers.size) println("Error registering servers with data placement")
		else println("SCADS servers registered with data placement.")
	}
	def partition(minK: Int, maxK: Int) = {
		assert( (maxK-minK)%servers.size==0,"deploy_scads: key space isn't evenly divisible by number of servers" )

		// have first server download the data, and assign the range with the placement server
		deployData(servers.get(0))
		val list = new java.util.ArrayList[DataPlacement]()
		val start = new StringKey( Scads.keyFormat.format(minK) )
		val end = new StringKey( Scads.keyFormat.format(maxK) )
		val range = KeyRange(start,end)
		list.add(new DataPlacement(servers.get(0).privateDnsName,Scads.server_port,Scads.server_sync,range))
		setServers(list)

		// move data slices from the first server to all others
		val slice = (maxK-minK)/servers.size
		(1 to servers.size-1).toList.map((id)=>{
			val dp = Scads.getDataPlacementHandle(placement.get(0).publicDnsName,xtrace_on)
			val startnum = id * slice
			val start = new StringKey( Scads.keyFormat.format(startnum) )
			val end =  new StringKey( Scads.keyFormat.format(startnum + slice))
			val range = KeyRange(start,end)
			println("Moving to: "+ servers.get(id).privateDnsName + ": "+ start +" - "+ end)
			dp.move(namespace,range,servers.get(0).privateDnsName,Scads.server_port,Scads.server_sync,
									servers.get(id).privateDnsName,Scads.server_port,Scads.server_sync)
		})
		dpclient = Scads.getDataPlacementHandle(placement.get(0).publicDnsName,xtrace_on)
		if (dpclient.lookup_namespace(namespace).size != servers.size) println("Error registering servers with data placement")
		else println("SCADS servers registered with data placement.")
	}

	def getName(): String = scadsName
	private def retry( call: => Any, recovery: => Unit, maxNRetries: Int, delay: Long ) {
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
	private def setServers(list: java.util.List[DataPlacement]) = {
		dpclient = Scads.getDataPlacementHandle(placement.get(0).publicDnsName,xtrace_on)

		var callSuccessful = false
		while (!callSuccessful) {
			try {
				dpclient.add(namespace,list)
				callSuccessful = true
			} catch {
				case e: Exception => {
					println("thrift \"add\" call failed, waiting 1 second");
					Thread.sleep(1000)
					dpclient = Scads.getDataPlacementHandle(placement.get(0).publicDnsName,xtrace_on)
				}
			}
		}
	}
}
