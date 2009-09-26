package scads.deployment

import performance._

import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import com.twitter.commons._
import scala.collection.jcl.Conversions._

/**
* Deployment of director
*/
case class DirectorDeployment(
	scads:Scads,
	monitoring:SCADSMonitoringDeployment,
	deployToMonitoringVM:Boolean
) extends Component {
	var directorVMInstanceType = "c1.small"
	var directorVM:Instance = null
	
	var deployed = false
	var deployer:Deployer = null
	var deployerThread:Thread = null
		
	override def boot {
		// boot up a machine only it not deploying to the monitoring VM
		if (deployToMonitoringVM)
			directorVM = monitoring.monitoringVM
		else
			directorVM = DataCenter.runInstances(1, directorVMInstanceType).getFirst()
		directorVM.tagWith( DataCenter.keyName+"--SCADS--"+scads.deploymentName+"--director" )
	}
	
	def loadState {
		directorVM = try { DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+scads.deploymentName+"--director", true ).getFirst } catch { case _ => null }
	}
	
	override def waitUntilBooted { if (directorVM!=null) directorVM.waitUntilReady }
	
	override def deploy {
		deployed = false
		deployer = Deployer()
		deployerThread = new Thread(deployer)
		deployerThread.start
	}
	
	case class Deployer extends Runnable {
		def run = {
			val dbhost = monitoring.monitoringVM.publicDnsName
			val config = Json.build( Map("recipes"->Array("director::scads_director"),
										 "director"->Map("basedir"->"/mnt/director",
														 "metricService"->Map("port"->6001,"dbhost"->dbhost,"dbuser"->"root","dbpassword"->"","dbname"->"metrics"))))
		    println( config.toString() )
		    directorVM.deploy(config)

			// add aws environment vars to bash_profile
			val local = Array[String]("aws.cfg")
			directorVM.upload(local,"/opt/scads/experiments")
			directorVM.exec("cat /opt/scads/experiments/aws.cfg >> /root/.bash_profile")
			
			directorVM.exec("cd /tmp/  &&  wget http://radlab_java.s3.amazonaws.com/jdk-6u18-ea-bin-b02-linux-i586-09_sep_2009.bin -O java6.bin")
			directorVM.exec("cd /tmp/  &&  chmod 755 java6.bin  &&  echo yes | ./java6.bin > /dev/null")
			directorVM.exec("echo 'export JAVA_HOME=/usr/lib/jvm/java-6-sun/' >> /root/.bash_profile")

			deployed = true
		}
	}

	override def waitUntilDeployed { while (!deployed) Thread.sleep(100) }
}

/*object DirectorDeployment {
	def loadState(deploymentName:String,scads:Scads,monitoring:SCADSMonitoringDeployment):DirectorDeployment = {
		val directorVM = try { DataCenter.getInstanceGroupByTag( DataCenter.keyName+"--SCADS--"+scads.scadsName+"--director", true ).getFirst } catch { case _: null }
		if (directorVM!=null) {
			val director = DirectorDeployment(scads,monitoring)
		} else 
			null
	}
}*/
