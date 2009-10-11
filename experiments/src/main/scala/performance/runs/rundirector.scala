package scads.director

import scads.director._

object RunDirector {
	
	def main(args: Array[String]) {

		val policyName = System.getProperty("policyName")
		val deploymentName = System.getProperty("deploymentName")
		val experimentName = System.getProperty("experimentName")
		
		val duration = System.getProperty("duration").toLong

		// initialize workload prediction
		val hysteresisUp = System.getProperty("hysteresisUp").toDouble
		val hysteresisDown = System.getProperty("hysteresisDown").toDouble
		val overprovisioning = System.getProperty("overprovisioning").toDouble
		val workloadPredictor = SimpleHysteresis(hysteresisUp, hysteresisDown, overprovisioning)
		
		// initialize cost function
		val getSLAThreshold = System.getProperty("getSLA").toInt
		val putSLAThreshold = System.getProperty("putSLA").toInt
		val slaInterval = System.getProperty("slaInterval").toLong
		val slaCost = System.getProperty("slaCost").toDouble
		val slaQuantile = System.getProperty("slaQuantile").toDouble
		val machineInterval = System.getProperty("machineInterval").toInt
		val machineCost = System.getProperty("machineCost").toDouble
		val costSkip = 10*60*1000
		val costFunction = FullSLACostFunction(getSLAThreshold,putSLAThreshold,slaQuantile,slaInterval,slaCost,machineCost,machineInterval,costSkip)

		// initialize policy
		val policy = 
		if (policyName=="SplitAndMergeOnWorkload") {
			val mergeThreshold = System.getProperty("mergeThreshold").toDouble
			val splitThreshold = System.getProperty("splitThreshold").toDouble
			new SplitAndMergeOnWorkload(mergeThreshold,splitThreshold,workloadPredictor)
			
		} else 
		if (policyName=="HeuristicOptimizerPolicy") {
			val modelpath = System.getProperty("modelPath")
			val performanceModel = LocalL1PerformanceModel(modelpath)
			new HeuristicOptimizerPolicy(performanceModel, getSLAThreshold, putSLAThreshold, workloadPredictor)
			
		} else 
			exit(-1)

		// start director
		val director = Director(deploymentName,policy,costFunction,experimentName)
		director.direct
		
		// wait until end of experiment and upload logs
		Thread.sleep(duration)
		director.stop
		director.uploadLogsToS3
		
		// kill all the VMs in this experiment except the Director VM?
	}
	
}
