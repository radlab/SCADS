package scads.deployment
 
import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._

import scads.director._
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.thrift.{RangeSet,RecordSet,KnobbedDataPlacementServer,DataPlacement, RangeConversion}
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}

case class Scads(
	cluster_config:List[(DirectorKeyRange,String)],
	deploymentName:String,
	deployMonitoring:Boolean,
	monitoring:SCADSMonitoringDeployment) 
extends Component with RangeConversion {
	var servers:InstanceGroup = null
	var placement:InstanceGroup = null
	val serversName = "servers"; val placeName = "placement"
	var dpclient:KnobbedDataPlacementServer.Client = null
	
	var deploythread:Thread = null
	var collectorIP:String = null
	val minKey = 0
	val maxKey = 10000
	val namespace = "perfTest256"
	
	def boot = {
		servers = DataCenter.runInstances(cluster_config.size,"m1.small")
		placement = DataCenter.runInstances(1,"m1.small")
	}
	def waitUntilBooted = {
		val machines = Array(servers,placement)
		new InstanceGroup(machines).waitUntilReady
		servers.tagWith( DataCenter.keyName+"--SCADS--"+deploymentName+"--"+serversName)
		placement.tagWith( DataCenter.keyName+"--SCADS--"+deploymentName+"--"+placeName)
	}
	
	def deploy = {
		deploythread = new Thread(new ScadsDeployer)
		deploythread.start
	}
	
	def waitUntilDeployed = {
		if (deploythread != null) deploythread.join
	}
	
	private def deployData(server:Instance, url:String) = {
		// data for server config --- replace with s3 bucket url when we have them
		val dataRecipe = new JSONArray()
		dataRecipe.put("scads::dbs")
		val dataConfig = new JSONObject()
		dataConfig.put("recipes", dataRecipe)
	
		server.exec("sv stop /mnt/services/scads_bdb_storage_engine")
		server.exec("rm -rf /mnt/scads/data")
		println("deploying default data") // server.exec("wget ...")
		val serversDeployWait = server.deployNonBlocking(dataConfig)
		val serverDeployResult = serversDeployWait.value
		server.exec("sv start /mnt/services/scads_bdb_storage_engine")
		println("deployed!")
		println("server data deploy log: "); println(serverDeployResult.getStdout); println(serverDeployResult.getStderr)
	}
	
	private def replicate(minK: Int, maxK: Int) = {
		// have first server download the data, and assign the range with the placement server
		deployData(servers.get(0),null)
		val list = new java.util.ArrayList[DataPlacement]()
		val start = new StringKey( ScadsDeploy.keyFormat.format(minK) )
		val end = new StringKey( ScadsDeploy.keyFormat.format(maxK) )
		val range = KeyRange(start,end)
		list.add(new DataPlacement(servers.get(0).privateDnsName,ScadsDeploy.server_port,ScadsDeploy.server_sync,range))
		setServers(list)

		// copy the data from the first server to all others
		(1 to servers.size-1).toList.map((id)=>{
			val dp = ScadsDeploy.getDataPlacementHandle(placement.get(0).publicDnsName,deployMonitoring)
			println("Copying to: "+ servers.get(id).privateDnsName + ": "+ start +" - "+ end)
			dp.copy(namespace,range,servers.get(0).privateDnsName,ScadsDeploy.server_port,ScadsDeploy.server_sync,
									servers.get(id).privateDnsName,ScadsDeploy.server_port,ScadsDeploy.server_sync)
		})
		dpclient = ScadsDeploy.getDataPlacementHandle(placement.get(0).publicDnsName,deployMonitoring)
		if (dpclient.lookup_namespace(namespace).size != servers.size) println("Error registering servers with data placement")
		else println("SCADS servers registered with data placement.")
	}
	
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
		dpclient = ScadsDeploy.getDataPlacementHandle(placement.get(0).publicDnsName,deployMonitoring)

		var callSuccessful = false
		while (!callSuccessful) {
			try {
				dpclient.add(namespace,list)
				callSuccessful = true
			} catch {
				case e: Exception => {
					println("thrift \"add\" call failed, waiting 1 second");
					Thread.sleep(1000)
					dpclient = ScadsDeploy.getDataPlacementHandle(placement.get(0).publicDnsName,deployMonitoring)
				}
			}
		}
	}

	case class ScadsDeployer extends Runnable {
		var deployed = false
		def run = {
			// storage server config
			val serverRecipes = new JSONArray()
			val serverConfig = if (deployMonitoring) { serverRecipes.put("chukwa::default"); ScadsDeploy.getXtraceIntoConfig(monitoring.monitoringVM.privateDnsName) } else { new JSONObject() }
		    serverRecipes.put("scads::storage_engine")
		    serverConfig.put("recipes", serverRecipes)

			// placement config
			val placementRecipes = new JSONArray()
			val placementConfig = if (deployMonitoring) { placementRecipes.put("chukwa::default"); ScadsDeploy.getXtraceIntoConfig(monitoring.monitoringVM.privateDnsName) } else { new JSONObject() }
	 		placementRecipes.put("scads::data_placement")
		    placementConfig.put("recipes", placementRecipes)
			
			println("waiting for deployment to finish")
			placement.deploy(placementConfig)
			servers.deploy(serverConfig)

			println("deployed!")
			// put all the data on one machine
			replicate(minKey,maxKey)
		}
	}
}