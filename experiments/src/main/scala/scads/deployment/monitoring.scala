package scads.deployment

/**
* This will deploy a machine with MySQL, chukwa collector, xtrace data parser, and metric service that generates histograms and workload and performance metrics.
* This should also do basic plotting ...
*/
case class SCADSMonitoringDeployment(
	scads:Scads
) extends Component {
	var monitoringVMInstanceType = "c1.small"
	var monitoringVM:Instance = null
	
	var deployed = false
	var deployer:Deployer = null
	var deployerThread:Thread = null
	
	var nBins = 200
	var minKey = 0
	var maxKey = 10000
	var aggregationInterval = 20000
	
	override def boot {
		// boot up a machine
		monitoringVM = DataCenter.runInstances(1, monitoringVMInstanceType).getFirst()
	}
	
	override def waitUntilBooted = {
		monitoringVM.waitUntilReady
		monitoringVM.tagWith( DataCenter.keyName+"--SCADS--"+scads.scadsName+"--monitoring" )
	}
	
	override def deploy {
		deployed = false
		deployer = Deployer()
		deployerThread = new Thread(deployer)
		deployerThread.start
	}
	
	case class Deployer extends Runnable {
		def run = {
			monitoringVM.exec("/etc/init.d/apache2 start") // start apache for viewing graphs
			
			// start xtrace parser
			monitoringVM.exec("java -DMSHost=localhost -DMSPort=6001 -DchukwaHost=localhost -DchukwaPort=9094 -DnBins="+nBins+" -DminKey="+minKey+" -DmaxKey="+maxKey+" -DhistogramDBHost=localhost -DaggregationInterval="+aggregationInterval+" -cp target/experiments-1.0-jar-with-dependencies.jar scads.director.ParseXtraceReports")
			deployed = true
		}
	}

	override def waitUntilDeployed = while (!deployed) Thread.sleep(100)
}

object SCADSMonitoringDeployment {
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
}