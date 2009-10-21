package scads.director

import scads.deployment._
import performance.WorkloadGenerators._
import performance._
import org.apache.log4j._
import org.apache.log4j.Level._

object HeuristicWikipedia {
	
	def main(args: Array[String]) {
		ScadsDeploy.useXact = true
		val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		val hystup = System.getProperty("hysteresisUp","0.9")
		val hystdown = System.getProperty("hysteresisDown","0.05")
		val overprov = System.getProperty("overProvision","0.2")
		
		//val filename = "wikipedia_60-84hr_60min_200users_200keys.ser"
		val filename = "wikipedia_60-84hr_60min_500users_200keys.ser"
		val maxKey = 200000
		val maxUsers = 500
		val experimentName = System.getenv("AWS_KEY_NAME")+ "_wiki_pinchoff_"+hystup+"_"+hystdown+"_"+overprov+"_"+filename+"_"+System.currentTimeMillis
			
		val logger = Logger.getLogger("scads.experiment")
		logger.addAppender( new FileAppender(new PatternLayout("%d %5p %c - %m%n"),"/tmp/experiments/"+dateFormat.format(new java.util.Date)+"_"+experimentName+".txt",false) )
		logger.addAppender( new ConsoleAppender(new PatternLayout("%d %5p %c - %m%n")) )
		logger.setLevel(DEBUG)
		logger.debug("starting experiment "+experimentName)

		val nClientMachines = 20
		val nHotStandbys = 15
		val namespace = "perfTest256"
		val jar = "http://scads.s3.amazonaws.com/experiments-1.0-jar-with-dependencies_beth.jar"
	
		// deploy all VMs
		logger.info("deploying SCADS")
		ScadsDeploy.maxKey = maxKey
		val dep = SCADSDeployment(experimentName)
		dep.experimentsJarURL = jar
		dep.deploy(nClientMachines,nHotStandbys,true,true)
		dep.waitUntilDeployed

		// warm cache
		logger.info("warming up the cache")
		dep.clients.loadState
		dep.clients.warm_cache(namespace,0,maxKey)
	
		// print summary of experiment deployment
		logger.info("deployment summary:\n"+dep.summary)

		// start Director
		logger.info("starting director")
		val directorCmd = "bash -l -c 'java"+
							" -DpolicyName=HeuristicOptimizerPolicy" +
							//" -DmodelPath=\"/opt/scads/experiments/scripts/perfmodels/gp_model.csv\"" +
							" -DmodelPath=\"/opt/scads/experiments/scripts/perfmodels/gp_model2_thr.csv\"" +
							" -DdeploymentName="+experimentName +
							" -DexperimentName="+experimentName +
							" -Dduration="+(60*60*1000) +
							" -DhysteresisUp="+hystup +
							" -DhysteresisDown="+hystdown +
							" -Doverprovisioning="+overprov +
							" -DgetSLA=100" +
							" -DputSLA=150" +
							" -DslaInterval=" + (5*60*1000) +
							" -DslaCost=100" + 
							" -DslaQuantile=0.99" +
							" -DmachineInterval=" + (10*60*1000) +
							" -DmachineCost=1" +
							" -DcostSkip=" + (10*60*1000) +
							" -cp /mnt/monitoring/experiments.jar" +
							" scads.director.RunDirector'" // "> /var/www/director.txt 2>&1"
		logger.info("director command: "+directorCmd)
		
		dep.director.loadState
		dep.directorVM.exec("echo \""+directorCmd+"\" > /tmp/w.sh")
		dep.directorVM.exec("chmod 777 /tmp/w.sh")
		dep.directorVM.exec("nohup /tmp/w.sh &> /var/www/director.txt < /dev/null &")
	
		// start workload
		logger.info("first downloading "+filename)
		dep.startWorkload(maxUsers,filename)
		logger.info("starting workload")
		dep.clients.loadState	
		// exit
		logger.info("exiting")
	}
}