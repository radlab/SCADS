package scads.director

import scads.deployment._
import performance.WorkloadGenerators._
import performance._
import org.apache.log4j._
import org.apache.log4j.Level._

object BenchmarkStorage {
	val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
	val logger = Logger.getLogger("scads.experiment")
	val datadir = "/tmp/benchmark"

	def workloadDuration(workload:WorkloadDescription):Long = workload.workload.map(_.duration).reduceLeft(_+_)
	def main(args: Array[String]) {
		val experimentName = System.getenv("AWS_KEY_NAME")+ "_benchmark_"+System.currentTimeMillis

		logger.addAppender( new FileAppender(new PatternLayout("%d %5p %c - %m%n"),"/tmp/experiments/"+dateFormat.format(new java.util.Date)+"_"+experimentName+".txt",false) )
		logger.addAppender( new ConsoleAppender(new PatternLayout("%d %5p %c - %m%n")) )
		logger.setLevel(DEBUG)

		val maxKey = 200000
		val nClientMachines = 10
		val nHotStandbys = 0
		val namespace = "perfTest256"

		// deploy all VMs
		logger.info("deploying SCADS")
		ScadsDeploy.maxKey = maxKey
		val dep = SCADSDeployment(experimentName)
		dep.deploy(nClientMachines,nHotStandbys,true,false)
		dep.waitUntilDeployed

		// warm cache
		logger.info("warming up the cache")
		dep.clients.loadState
		dep.clients.warm_cache(namespace,0,maxKey)

		// print summary of experiment deployment
		logger.info("deployment summary:\n"+dep.summary)

		// run workload for variety of get ratios
		List(1.0,0.99,0.97,0.95,0.93,0.91).foreach((gets)=>{
			logger.info("starting experiment "+gets)

			// prepare workload
			logger.debug("preparing workload")
			//val workload = linearWorkload(gets,0.0,0, maxKey.toString, namespace, 200, 3000, 10)
			val workload = linearWorkload(gets,0.0,0, maxKey.toString, namespace, 250, 5000, 10)
			// start workload
			logger.debug("starting workload")
			dep.clients.loadState
			dep.startWorkload(workload)

			// wait for workload to finish, them upload results
			val duration = workloadDuration(workload)
			logger.debug("sleeping for "+duration+" during workload")
			Thread.sleep(duration)
			dep.stopWorkload
			logger.debug("uploading results")
			uploadLogsToS3(dep,experimentName,gets)
			logger.info("finished "+gets)

			// sleep between runs so there's a gap in data
			Thread.sleep(60*1000)
		})
	}
	def uploadLogsToS3(dep:SCADSDeployment,experimentName:String,num_gets:Double) {
		dep.monitoring = SCADSMonitoringDeployment(dep.deploymentName,null)
		dep.monitoring.loadState

		dep.monitoringVM.exec("mkdir "+datadir)
		val io = dep.monitoringVM.exec("/usr/local/mysql/bin/mysqldump --databases director metrics > "+datadir+"/dbdump_"+num_gets+"_"+dateFormat.format(new java.util.Date)+".sql")
		//logger.debug("dumped mysql databases. stdout:"+io._1+"\n"+"stderr:"+io._2)
		//Director.dropDatabases

		dep.monitoringVM.exec( "s3cmd -P sync "+datadir+"/ s3://scads-experiments/benchmark_"+experimentName+"/" )
		//logger.debug("executed s3cmd sync. stdout:"+io2._1+"\n"+"stderr:"+io2._2)
	}
}
