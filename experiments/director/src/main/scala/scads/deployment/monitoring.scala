package scads.deployment

import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._
import com.twitter.commons._


/**
* This will deploy a machine with MySQL, chukwa collector, xtrace data parser, and metric service that generates histograms and workload and performance metrics.
* This should also do basic plotting ...
*/
case class SCADSMonitoringDeployment(
	deploymentName:String,
	experimentsJarURL:String
) extends Component {
	//var monitoringVMInstanceType = "m1.small"
	var monitoringVMInstanceType = "c1.medium"
	var monitoringVM:Instance = null
	
	var startedDeploying = false
	var deployer:Deployer = null
	var deployerThread:Thread = null
	
	var nBins = 200
	var minKey = 0
	var maxKey = ScadsDeploy.maxKey
	var aggregationInterval = 20000
	
	override def boot {
		// boot up a machine
		ScadsDeploy.logger.debug("monitoring: booting up 1 monitoring VM ("+monitoringVMInstanceType+")")
		monitoringVM = DataCenter.runInstances(1, monitoringVMInstanceType).getFirst()
	}
	
	override def waitUntilBooted = {
		monitoringVM.waitUntilReady
		ScadsDeploy.logger.debug("monitoring: have monitoring VM")
		monitoringVM.tagWith( DataCenter.keyName+"--SCADS--"+deploymentName+"--monitoring" )
	}
	
	override def deploy {
		deployer = Deployer()
		deployerThread = new Thread(deployer)
		deployerThread.start
		startedDeploying = true
	}
	def loadState = {
		monitoringVM = DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+deploymentName+"--monitoring", true ).getFirst
		startedDeploying = true
	}
	
	case class Deployer extends Runnable {
		def run = {
			val collectorConfig = new JSONObject()
			val collectorRecipes = new JSONArray()
		    collectorRecipes.put("chukwa::collector")
		    collectorConfig.put("recipes", collectorRecipes)
			ScadsDeploy.logger.debug("monitoring: deploying chukwa collector")
			val collectorDeployResult = monitoringVM.deploy(collectorConfig)
			ScadsDeploy.logger.debug("monitoring: collector deployed")
			ScadsDeploy.logger.debug("monitoring: collector deploy STDOUT:\n"+collectorDeployResult.getStdout)
			ScadsDeploy.logger.debug("monitoring: collector deploy STDERR:\n"+collectorDeployResult.getStderr)

			// deploy monitoring
			val monitoringCfg = Json.build( Map("recipes"->Array("scads::monitoring"),
										 	"monitoring"->Map(	"basedir"->"/mnt/monitoring",
																"experimentsJarURL"->experimentsJarURL,
														 		"metricService"->Map("port"->6001,"dbhost"->"localhost","dbuser"->"root","dbpassword"->"","dbname"->"metrics"),
																"xtraceParser"->Map("nBins"->nBins,"minKey"->minKey,"maxKey"->maxKey,
																					"aggregationInterval"->aggregationInterval,
																					"getSamplingProbability"->ScadsDeploy.getSamplingProbability,
																					"putSamplingProbability"->ScadsDeploy.putSamplingProbability
																					)
														)))
			ScadsDeploy.logger.debug("monitoring: deploying monitoring")
			val monitoringDeployResult = monitoringVM.deploy(monitoringCfg)
			ScadsDeploy.logger.debug("monitoring: monitoring deploy STDOUT:\n"+monitoringDeployResult.getStdout)
			ScadsDeploy.logger.debug("monitoring: monitoring deploy STDERR:\n"+monitoringDeployResult.getStderr)
			ScadsDeploy.logger.debug("monitoring: monitoring deployed")
		}
	}

	override def waitUntilDeployed { 
		while (!startedDeploying) {
			Thread.sleep(1000)
			ScadsDeploy.logger.debug("monitoring: waiting to start deployment")
		}
		if (deployerThread != null) deployerThread.join
	}
}

/*object SCADSMonitoringDeployment {
	def loadState(deploymentName,scads):SCADSMonitoringDeployment = {
		val monitoringVM = try { DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+scads.scadsName+"--monitoring", true ).getFirst } catch { case _: null }
		if (monitoringVM!=null) {
			val monitoring = SCADSMonitoringDeployment(scads)
			monitoring.monitoringVM = monitoringVM
			monitoring.deployed = true
			monitoring
		} else 
			null
	}
}*/