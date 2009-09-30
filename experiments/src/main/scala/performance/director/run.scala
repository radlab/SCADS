import scads.director._

object Run {
	
	def main(args: Array[String]) {

		val deploymentName = "test1"
		val experimentName = "directorTestRun"

		val workloadPredictor = SimpleHysteresis(0.9,0.05,0.2)
		val policy = new EmptyPolicy(workloadPredictor)
		val costFunction = FullSLACostFunction(100,100,0.99,1*60*1000,100,1,2*60*1000)

		val director = Director(deploymentName,policy,costFunction,experimentName)
		
		director.direct
		
		Thread.sleep(10*60*1000)
		director.stop
		director.uploadLogsToS3
		
	}
	
}
