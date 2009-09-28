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
	def deploy(nClients:Int, deployMonitoring:Boolean, deployDirector:Boolean) {
		try {
			val doMonitoring = if (deployDirector) { true } else { deployMonitoring }
		
			// create the components
			if (doMonitoring) monitoring = SCADSMonitoringDeployment(deploymentName,experimentsJarURL)
			myscads = Scads(List[(DirectorKeyRange,String)](null,null),deploymentName,doMonitoring,monitoring)
			if (nClients>0) clients = ScadsClients(myscads,nClients)
			if (deployDirector) director = DirectorDeployment(myscads,monitoring,deployDirectorToMonitoring)
		
			val components = List[Component](myscads,clients,monitoring,director)		
			components.filter(_!=null).foreach(_.boot) 				// boot up all machines
			components.filter(_!=null).foreach(_.waitUntilBooted) 	// wait until all machines booted
			components.filter(_!=null).foreach(_.deploy)			// deploy on all
			components.filter(_!=null).foreach(_.waitUntilDeployed)	// wait until all deployed

			ScadsDeploy.logger.info("DEPLOYED SCADS")			
		} catch { case e:Exception => ScadsDeploy.logger.error("SCADS deployment error:\n",e) }
	}
	
//	def startDirector(policy:Policy)
	
	def startWorkload(workload:WorkloadDescription) {
		if (clients!=null) clients.startWorkload(workload)
		else ScadsDeploy.logger.debug("can't start workload; no clients running")
	}
	
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
//	def summaryPage
	
	def clientVMs:InstanceGroup = if (clients==null) { new InstanceGroup() } else clients.clients
	def monitoringVM:Instance = if (monitoring==null) null else monitoring.monitoringVM
	def directorVM:Instance = if (director==null) null else director.directorVM
}
