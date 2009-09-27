package scads.deployment

import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._

import performance._
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.thrift.{RangeSet,RecordSet,KnobbedDataPlacementServer,DataPlacement, RangeConversion}
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}


case class ScadsClients(myscads:Scads,num_clients:Int) extends Component {
	val scadsName = myscads.deploymentName
	val xtrace_on = myscads.deployMonitoring
	var deps:String = null
	var clients:InstanceGroup = null
	var host:String = null
	var clientConfig:JSONObject = null
	var deploythread:Thread = null
	
	def boot = {
		clients = DataCenter.runInstances(num_clients,"c1.medium")
	}
	def waitUntilBooted = {
		clients.waitUntilReady
		clients.tagWith( DataCenter.keyName+"--SCADS--"+scadsName+"--"+"clients")
	}

	def deploy = {
		host = myscads.placement.get(0).privateDnsName
	
		deploythread = new Thread(new ScadsDeployer)
		deploythread.start
	}
	
	def waitUntilDeployed = {
		if (deploythread != null) deploythread.join
	}

	case class ScadsDeployer extends Runnable {
		var deployed = false
		def run = {
			val clientRecipes = new JSONArray()
			clientConfig = if (xtrace_on) { clientRecipes.put("chukwa::default"); ScadsDeploy.getXtraceIntoConfig(myscads.monitoring.monitoringVM.privateDnsName) } else { new JSONObject() }
		    clientRecipes.put("scads::client_library")
		    clientConfig.put("recipes", clientRecipes)
			
			ScadsDeploy.logger.debug("deploying all clients")
			clients.deploy(clientConfig)

			ScadsDeploy.logger.debug("clients deployed!")
			//ScadsDeploy.logger.debug("clients deploy log: "); clientDeployResult.foreach( (x:ExecuteResponse) => {ScadsDeploy.logger.debug(x.getStdout); ScadsDeploy.logger.debug(x.getStderr)} )
		
			// get list of mvn dependencies
			deps = clients.get(0).exec("cd /opt/scads/experiments; cat cplist").getStdout.replace("\n","") + ":../target/classes"
			clients.get(0).exec("/etc/init.d/apache2 start") // start apache for viewing graphs
		}
	}

	def warm_cache(ns: String, minK:Int, maxK:Int) = {
		ScadsDeploy.logger.debug("Warming server caches.")
		val cmd = "cd /opt/scads/experiments/scripts; scala -cp "+ deps + " warm_cache.scala " + 
					host + " "+ ns +" " + minK + " " + maxK +" "+xtrace_on
		clients.get(0).exec(cmd)
		ScadsDeploy.logger.debug("Done warming.")
	}

	def startWorkload_NB(workload:WorkloadDescription) = {
		val testthread = new Thread(new WorkloadDescRunner(workload))
		testthread.start
	}
	
	case class WorkloadDescRunner(workload:WorkloadDescription) extends Runnable {
		def run() = {
			startWorkload(workload)
			ScadsDeploy.logger.debug("Workload test complete.")
		}
	}

	def startWorkload(workload:WorkloadDescription) {
		val totalUsers = (workload.getMaxNUsers/clients.size+1)*clients.size
		val workloadFile = "/tmp/workload.ser"
		workload.serialize(workloadFile)
		
		ScadsDeploy.logger.debug("This workload should run for "+"%.2f".format(workload.workload.map(_.duration).reduceLeft(_+_).toDouble/1000/60) +" minutes")
		
		// determine ranges to give each client
		assert( totalUsers % clients.size == 0, "deploy_scads: can't evenly divide number of users amongst client instances")

		var commands = Map[Instance, String]()
		val threads = (0 to clients.size-1).toList.map((id)=>{
			val minUser = id * (totalUsers/clients.size)
			val maxUser = minUser + (totalUsers/clients.size) - 1

			val args = host +" "+ xtrace_on + " " + minUser + " " + maxUser + " " + workloadFile
			val cmd = "cd /opt/scads/experiments/scripts; scala -cp "+ deps + " startWorkload.scala " + args + " &> /tmp/workload.log"
			commands += (clients.get(id) -> cmd)
			ScadsDeploy.logger.debug("Will run with arguments: "+ args)
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
	
	def processLogFiles(experimentName:String, parseAndPlot:Boolean) {
		val client0 = clients.get(0)
		val targetIP = client0.privateDnsName
		client0.exec( "mkdir -p /mnt/logs/"+experimentName+"/clients/" )
		client0.exec("ln -s /mnt/logs/ /var/www/")
		
		for (c <- clients) { 
			val f=experimentName+"_"+c.privateDnsName+".log"
			val df="/tmp/"+f
			val cmd="cat /mnt/xtrace/logs/* > "+f+" && scp -o StrictHostKeyChecking=no "+f+" "+targetIP+":/mnt/logs/"+experimentName+"/clients/"+f
			//ScadsDeploy.logger.debug(cmd)
			c.exec(cmd) 
			c.exec( "rm -f /mnt/xtrace/logs/*" )
		}
		
		val expDir = "/mnt/logs/"+experimentName
		val sourceF = expDir+"/"+experimentName+".log"
		client0.exec( "cat /mnt/logs/"+experimentName+"/clients/* > "+sourceF)
		client0.exec( "ulimit -n 20000" )

		ScadsDeploy.logger.debug("logs are in "+client0.publicDnsName+":"+sourceF)
				
		if (parseAndPlot) {
			ScadsDeploy.logger.debug("graphs will be in http://"+client0.publicDnsName+"/logs/")
			for (i <- List(1,5,10)) {
				ScadsDeploy.logger.debug("generating logs with "+i+" min aggregation")
				client0.exec( "scala /opt/scads/experiments/scripts/parselogs.scala "+sourceF+" "+expDir+"/"+experimentName+"_agg"+i+".csv "+i*1000+" 1")
				client0.exec( "echo \"source('/opt/scads/experiments/scripts/process.R'); pdf('"+expDir+"/"+experimentName+"_agg"+i+".pdf',width=10,height=15); plot.stats.for.file('"+expDir+"/"+experimentName+"_agg"+i+".csv') \" | R --vanilla")
			}
		}
		
	}
}