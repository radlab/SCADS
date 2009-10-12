package scads.director

import scads.deployment._
import performance.WorkloadGenerators._
import performance._
import org.apache.log4j._
import org.apache.log4j.Level._

object TestExperiment {

	def workloadDuration(workload:WorkloadDescription):Long = workload.workload.map(_.duration).reduceLeft(_+_)

  	def main(args: Array[String]) {

		val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		val experimentName = System.getProperty("experimentName")
	
		val logger = Logger.getLogger("scads.experiment")
		logger.addAppender( new FileAppender(new PatternLayout("%d %5p %c - %m%n"),"/tmp/experiments/"+dateFormat.format(new java.util.Date)+"_"+experimentName+".txt",false) )
		logger.addAppender( new ConsoleAppender(new PatternLayout("%d %5p %c - %m%n")) )
		logger.setLevel(DEBUG)
		logger.debug("starting experiment "+experimentName)

		val maxKey = 10000
		val nClientMachines = 2
		val nHotStandbys = 2
		val namespace = "perfTest256"
		val jar = "http://scads.s3.amazonaws.com/experiments-1.0-jar-with-dependencies-bodikp.jar"
	
		// deploy all VMs
		logger.info("deploying SCADS")
		ScadsDeploy.maxKey = maxKey
		val dep = SCADSDeployment(experimentName)
		dep.experimentsJarURL = jar
		dep.deploy(nClientMachines,true,true)
		dep.waitUntilDeployed
	
		// add hot-standbys
		logger.info("adding hot-standby servers")
		dep.myscads.addServers(nHotStandbys)

		// warm cache
		logger.info("warming up the cache")
		dep.clients.warm_cache(namespace,0,maxKey)
	
		// print summary of experiment deployment
		logger.info("deployment summary:\n"+dep.summary)

		// prepare workload
		logger.info("preparing workload")
		dep.clients.loadState	
		val workload = stdWorkloadEbatesWSpike(mix99, 100, maxKey)

		// start Director
		logger.info("starting director")
		val directorCmd = "bash -l -c 'java"+
							" -DpolicyName=SplitAndMergeOnWorkload" +
							" -DdeploymentName="+experimentName +
							" -DexperimentName="+experimentName +
							" -Dduration="+workloadDuration(workload).toString +
							" -DhysteresisUp=0.5" +
							" -DhysteresisDown=0.05" +
							" -Doverprovisioning=0.1" +
							" -DgetSLA=100" +
							" -DputSLA=100" +
							" -DslaInterval=" + (5*60*1000) +
							" -DslaCost=100" + 
							" -DslaQuantile=0.99" +
							" -DmachineInterval=" + (10*60*1000) +
							" -DmachineCost=1" +
							" -DmergeThreshold=1500" +
							" -DsplitThreshold=1500" +
							" -cp /mnt/monitoring/experiments.jar" +
							" scads.director.RunDirector'" // "> /var/www/director.txt 2>&1"
		logger.info("director command: "+directorCmd)
		
		dep.directorVM.exec("echo \""+directorCmd+"\" > /tmp/w.sh")
		dep.directorVM.exec("chmod 777 /tmp/w.sh")
		dep.directorVM.exec("nohup /tmp/w.sh &> /var/www/director.txt < /dev/null &")
	
		// start workload
		logger.info("starting workload")
		dep.startWorkload(workload)
	
		// exit
		logger.info("exiting")
	
	}
}