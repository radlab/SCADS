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

object ScadsLoader {
	def loadState(deploymentName:String):Scads = {
		var myscads:Scads = null
		val servers = DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+deploymentName+"--"+ScadsDeploy.serversName, true )
		val placement = DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+deploymentName+"--"+ScadsDeploy.placeName, true )
		if (placement.size > 0) {
			val info = readPlacementInfo(placement.get(0))

			myscads = Scads(List[(DirectorKeyRange,String)]((null,null)),deploymentName,info._1)
			myscads.servers = servers; myscads.placement = placement; myscads; myscads.setMonitor(info._2)
			myscads.dpclient = ScadsDeploy.getDataPlacementHandle(placement.get(0).publicDnsName, myscads.deployMonitoring)
		}
		myscads
	}
	private def readPlacementInfo(machine:Instance): (Boolean,String) = {
		val info = machine.exec("cat "+ScadsDeploy.placementInfoFile).getStdout.replace("\n","").split(",")
		(info(0).toBoolean,info(1))
	}
}

case class Scads(
	cluster_config:List[(DirectorKeyRange,String)],
	deploymentName:String,
	deployMonitoring:Boolean)
extends Component with RangeConversion {
	var servers:InstanceGroup = null
	var placement:InstanceGroup = null
	var dpclient:KnobbedDataPlacementServer.Client = null
	var deploythread:Thread = null
	var monitorIP:String = "NA"

	val config_reverse_sorted = cluster_config.sort(_._1.maxKey > _._1.maxKey)
	val minKey = 0
	val maxKey = config_reverse_sorted.first._1.maxKey // get the highest key in the cluster
	val namespace = "perfTest256"
	val serverVMType = "m1.small"
	val placementVMType = "m1.small"

	// server node config used by initial server and when want to addservers
	var serverConfig:JSONObject = null

	def boot = {
		ScadsDeploy.logger.debug("scads: booting up "+cluster_config.size+" storage node(s) VM ("+serverVMType+")")
		ScadsDeploy.logger.debug("scads: booting up 1 placement VM ("+placementVMType+")")
		servers = DataCenter.runInstances(cluster_config.size,serverVMType)
		placement = DataCenter.runInstances(1,placementVMType)
	}

	def waitUntilBooted = {
		val machines = Array(servers,placement)
		new InstanceGroup(machines).waitUntilReady
		ScadsDeploy.logger.debug("scads: have storage and placement VMs")
		servers.tagWith( DataCenter.keyName+"--SCADS--"+deploymentName+"--"+ScadsDeploy.serversName)
		placement.tagWith( DataCenter.keyName+"--SCADS--"+deploymentName+"--"+ScadsDeploy.placeName)
	}

	def deploy = {
		deploythread = new Thread(new ScadsDeployer)
		deploythread.start
	}

	def waitUntilDeployed = {
		if (deploythread != null) deploythread.join
	}
	def setMonitor(ip:String) = { monitorIP = ip }
	def addServers(num:Int) = {
		if (serverConfig==null) {
			val serverRecipes = new JSONArray()
			serverConfig = if (deployMonitoring) { serverRecipes.put("chukwa::default"); ScadsDeploy.getXtraceIntoConfig(monitorIP) } else { new JSONObject() }
		    serverRecipes.put("scads::storage_engine")
		    serverConfig.put("recipes", serverRecipes)
		}
		//ScadsDeploy.logger.debug("scads: booting up "+num+" storage node(s) VM ("+serverVMType+")")
		val new_servers = DataCenter.runInstances(num,serverVMType)
		new_servers.waitUntilReady
		new_servers.tagWith( DataCenter.keyName+"--SCADS--"+deploymentName+"--"+ScadsDeploy.serversName )

		//ScadsDeploy.logger.debug("scads: deploying storage nodes")
		val serversDeployWait = new_servers.deployNonBlocking(serverConfig)
		val serverDeployResult = serversDeployWait()
		//ScadsDeploy.logger.debug("scads: deployed storage nodes")

		servers.addAll(new_servers)
	}

	def shutdownServer(host:String) = { // note: this does not remove from data placement!
		var found = false
		(0 to servers.size-1).toList.map((id)=>{
			if (servers.get(id).privateDnsName == host || servers.get(id).publicDnsName == host) {
				val machine:Instance = servers.get(id)
				//ScadsDeploy.logger.debug("Shutting down "+host)
				machine.stop
				//ScadsDeploy.logger.debug("Terminated "+host)
				found = true
			}
		})
		if (!found) ScadsDeploy.logger.warn("No machine with hostname "+host+" found.")
	}
	private def deployData(server:Instance, url:String) = {
		// data for server config --- replace with s3 bucket url when we have them
		val dataRecipe = new JSONArray()
		dataRecipe.put("scads::dbs")
		val dataConfig = new JSONObject()
		dataConfig.put("recipes", dataRecipe)
	
		server.exec("sv stop /mnt/services/scads_bdb_storage_engine")
		server.exec("rm -rf /mnt/scads/data")
		ScadsDeploy.logger.debug("scads deploying default data") // server.exec("wget ...")
		val serversDeployWait = server.deployNonBlocking(dataConfig)
		val serverDeployResult = serversDeployWait.value
		server.exec("sv start /mnt/services/scads_bdb_storage_engine")
		ScadsDeploy.logger.debug("scads: deployed data!")
		ScadsDeploy.logger.debug("scads: server data deploy log: ")
		ScadsDeploy.logger.debug(serverDeployResult.getStdout)
		ScadsDeploy.logger.debug(serverDeployResult.getStderr)
	}

	private def partitionData = {
		ScadsDeploy.logger.debug("Deploying all data on first server ["+minKey+", "+maxKey+")")
		deployData(servers.get(0),null) // give first server all the data

		// tell data placement that first server has all data
		val list = new java.util.ArrayList[DataPlacement]()
		val range = KeyRange( new StringKey( ScadsDeploy.keyFormat.format(minKey)) , new StringKey( ScadsDeploy.keyFormat.format(maxKey)) )
		list.add(new DataPlacement(servers.get(0).privateDnsName,ScadsDeploy.server_port,ScadsDeploy.server_sync,range))
		setServers(list)

		(1 until servers.size).foreach((index)=>{
			// move data from first server to other servers (the last config range will be left on first server)
			val dp = ScadsDeploy.getDataPlacementHandle(placement.get(0).publicDnsName,deployMonitoring)
			val start = new StringKey( ScadsDeploy.keyFormat.format(config_reverse_sorted(index)._1.minKey) )
			val end =  new StringKey( ScadsDeploy.keyFormat.format(config_reverse_sorted(index)._1.maxKey) )
			val range = KeyRange(start,end)
			ScadsDeploy.logger.debug("Moving to: "+ servers.get(index).privateDnsName + ": "+ start +" - "+ end)
			dp.move(namespace,range,servers.get(0).privateDnsName,ScadsDeploy.server_port,ScadsDeploy.server_sync,
									servers.get(index).privateDnsName,ScadsDeploy.server_port,ScadsDeploy.server_sync)
		})
		dpclient = ScadsDeploy.getDataPlacementHandle(placement.get(0).publicDnsName,deployMonitoring)
		if (dpclient.lookup_namespace(namespace).size != servers.size) ScadsDeploy.logger.warn("Error registering servers with data placement")
		else ScadsDeploy.logger.info("SCADS servers registered with data placement.")
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
					ScadsDeploy.logger.debug("exception; will wait and retry")
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
					ScadsDeploy.logger.debug("thrift \"add\" call failed, waiting 1 second");
					Thread.sleep(1000)
					dpclient = ScadsDeploy.getDataPlacementHandle(placement.get(0).publicDnsName,deployMonitoring)
				}
			}
		}
	}

	case class ScadsDeployer extends Runnable {
		var deployed = false
		def run = {
			// set up server config
			val serverRecipes = new JSONArray()
			serverConfig = if (deployMonitoring) { serverRecipes.put("chukwa::default"); ScadsDeploy.getXtraceIntoConfig(monitorIP) } else { new JSONObject() }
		    serverRecipes.put("scads::storage_engine")
		    serverConfig.put("recipes", serverRecipes)

			// placement config
			val placementRecipes = new JSONArray()
			val placementConfig = if (deployMonitoring) { placementRecipes.put("chukwa::default"); ScadsDeploy.getXtraceIntoConfig(monitorIP) } else { new JSONObject() }
	 		placementRecipes.put("scads::data_placement")
		    placementConfig.put("recipes", placementRecipes)
			
			ScadsDeploy.logger.debug("scads: deploying placement")
			placement.deploy(placementConfig)
			ScadsDeploy.logger.debug("scads: deployed placement")
			ScadsDeploy.logger.debug("scads: deploying storage nodes")
			servers.deploy(serverConfig)
			ScadsDeploy.logger.debug("scads: deployed storage nodes")

			placement.get(0).exec("/etc/init.d/apache2 start") // start apache for hosting put restriction file
			val info_str = if (deployMonitoring) { deployMonitoring+","+monitorIP } else { deployMonitoring+","+"NA" }
			placement.get(0).exec("echo '"+info_str+"' >> "+ScadsDeploy.placementInfoFile)

			// put all the data on one machine
			ScadsDeploy.logger.debug("scads: paritioning data")
			partitionData
			ScadsDeploy.logger.debug("scads: data ready")
		}
	}
}