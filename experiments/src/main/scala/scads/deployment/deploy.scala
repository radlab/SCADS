package scads.deployment

import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._
import scads.director._
import performance._

case class SCADSDeployment(
	deploymentName:String
) {
	var myscads:Scads = null
	var clients:ScadsClients = null
	var monitoring:SCADSMonitoringDeployment = null
	var director:DirectorDeployment = null
	
	var deployDirectorToMonitoring = true
	
	var experimentsJarURL = "http://scads.s3.amazonaws.com/experiments-1.0-jar-with-dependencies.jar"
	
	ScadsDeploy.initLogger
	
	private var _deployed = false
	var deployer:Deployer = null
	var deployerThread:Thread = null
	
	/**
	* This will load an existing deployment of scads based on the deployment name
	*/
	/*def loadExistingDeployment {
		scads = Scads.loadState(deploymentName)
		clients = Clients.loadState(deploymentName,scads)
		monitoring = SCADSMonitoringDeployment.loadState(deploymentName,scads)
		director = DirectorDeployment.loadState(deploymentName,scads,monitoring)
	}*/
	
	// parameters to add:
	// how many scads hot-standby's?
	// initial configuration and dataset
	// xtrace sampling probability
	case class Deployer(nClients:Int, deployMonitoring:Boolean, deployDirector:Boolean) extends Runnable {
		def run = {
			try {
				val doMonitoring = if (deployDirector) { true } else { deployMonitoring }

				// create the components
				if (doMonitoring) monitoring = SCADSMonitoringDeployment(deploymentName,experimentsJarURL)
				myscads = Scads(List[(DirectorKeyRange,String)]((DirectorKeyRange(0,ScadsDeploy.maxKey),null)),deploymentName,doMonitoring)
				if (nClients>0) clients = ScadsClients(myscads,nClients)
				if (deployDirector) director = DirectorDeployment(myscads,monitoring,deployDirectorToMonitoring)

				val components = List[Component](myscads,clients,monitoring,director)		
				components.filter(_!=null).foreach(_.boot) 				// boot up all machines
				components.filter(_!=null).foreach(_.waitUntilBooted) 	// wait until all machines booted
				if (doMonitoring) myscads.setMonitor(monitoring.monitoringVM.privateDnsName)	// inform scads component of monitor
				components.filter(_!=null).foreach(_.deploy)			// deploy on all
				components.filter(_!=null).foreach(_.waitUntilDeployed)	// wait until all deployed

				ScadsDeploy.logger.info("DEPLOYED SCADS")
				ScadsDeploy.logger.info("summary:\n"+summary)

				_deployed = true
			} catch { case e:Exception => ScadsDeploy.logger.error("SCADS deployment error:\n",e) }	
		}
	}
	
	def deploy(nClients:Int, deployMonitoring:Boolean, deployDirector:Boolean) {
		deployer = Deployer(nClients, deployMonitoring, deployDirector)
		deployerThread = new Thread(deployer)
		deployerThread.start

		ScadsDeploy.logger.info("deploying SCADS in the background; call .deployed to check if deployed")
	}
	
	def waitUntilDeployed = while (!deployed) Thread.sleep(1000)
	
	def deployed = _deployed
	
	def startWorkload(workload:WorkloadDescription) {
		if (clients!=null) {
			stopWorkload
			clients.startWorkload(workload,false)
		} else ScadsDeploy.logger.debug("can't start workload; no clients running")
	}
	
	def stopWorkload() = clients.stopWorkload
	
	def processLogFiles(nameOfExperiment:String) {
		if (clients!=null) {
			ScadsDeploy.logger.debug("processing log files from experiment")
			clients.processLogFiles(nameOfExperiment,true)
		} else 
			ScadsDeploy.logger.debug("can't process log files; no clients running")
	}
	
//	def pullExperimentData
	
	/**
	* Create a page on http://scm/scads/keypair_deploymentName.html that contains important links for this deployment:
	* chukwa ping, performance graphs, director logs, ...
	*/
	def summary:String = {
		"Director: "+ (if (directorVM==null) "NULL" else directorVM.publicDnsName) + "\n" +
		"monitoring: "+ (if (monitoringVM==null) "NULL" else monitoringVM.publicDnsName) + "\n" +
		"clients: "+ (if (clientVMs==null) "NULL" else clientVMs.map(_.publicDnsName).mkString(" ")) + "\n" +
		"placement: "+ (try { placementVM.publicDnsName } catch { case _ => "NULL" }) + "\n" +
		"storage: "+ (try { storageVMs.map(_.publicDnsName).mkString(" ") } catch { case _ => "NULL" })
	}
	
	def clientVMs:InstanceGroup = if (clients==null) { new InstanceGroup() } else clients.clients
	def monitoringVM:Instance = if (monitoring==null) null else monitoring.monitoringVM
	def directorVM:Instance = if (director==null) null else director.directorVM
	def storageVMs:InstanceGroup = if (myscads==null) null else myscads.servers
	def placementVM:Instance = if (myscads==null) null else myscads.placement.get(0)
	
	def tailStorageLog(serverI:Int, nLines:Int):String = { storageVMs.get(serverI).exec("tail -n 10 /mnt/services/scads_bdb_storage_engine/log/current").getStdout }
}
