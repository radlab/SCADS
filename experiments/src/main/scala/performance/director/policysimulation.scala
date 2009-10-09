package scads.director

import org.apache.log4j._
import org.apache.log4j.Level._

case class SimulationResult(
	policyName:String,
	workloadName:String,
	policyParameters:Map[String,String],
	nServerUnits:Double,
	nSLAViolations:Double,
	costFunction:FullCostFunction
) {
	def csvHeader:String = {
		"policy,workload"+
		policyParameters.keySet.toList.sort(_<_).map("p:"+_).mkString(",",",",",")+
		"nServerUnits,nSLAViolations"
	}
	
	def csvData:String = {
		policyName+","+workloadName+
		policyParameters.toList.sort(_._1<_._1).map(_._2).mkString(",",",",",")+
		nServerUnits.toString+","+nSLAViolations
	}
}

case class SimulationResults() {
	var results = List[SimulationResult]()
	
	def addResult(result:SimulationResult) { results += result }
	def toCSVString:String = results(0).csvHeader+"\n"+results.map(_.csvData).mkString("\n")
	def saveToCSV(path:String) { val out = new java.io.FileWriter(path); out.write(toCSVString); out.close }
}


case class PolicySimulator {
	
	def simulateSingleWorkload(initialConfig:SCADSconfig, 
								workloadName:String, 
								histograms:Map[Long,WorkloadHistogram], 
								policy:Policy, 
								costFunction:FullCostFunction, 
								performanceModel:PerformanceModel,
								deterministic:Boolean,
								dumpStatePeriodically:Boolean):SimulationResult = {
						
		val simT0 = new java.util.Date().getTime
		PolicySimulator.logger.info("starting simulation with workload "+workloadName+" and parameters: "+policy.getParams.toString)
			
		Director.initialize(policy.toString)
		Director.dropDatabases
		if (deterministic) Director.rnd = new java.util.Random(7)
		SCADSState.initLogging("localhost",6001)
		Plotting.initialize(Director.basedir+"/plotting/")
		policy.initialize
		costFunction.initialize

		val actionExecutor = ActionExecutor()
/*		val simulationGranularity = 10*1000*/
		val simulationGranularity = 1*1000
		val requestFraction = 0.1

		val histogramEndTimes = histograms.keySet.toList.sort(_<_).toArray
		var histogramI = 0
		var histogramEnd = histogramEndTimes(histogramI)
		var histogramRaw = histograms(histogramEnd)
		
		var policyUpdatePeriod = 20*1000L
		var lastPolicyUpdate = -policyUpdatePeriod
		
		var policyExecPeriod = 20*1000L
		var lastPolicyExec = -policyExecPeriod
		
		// create initial state
		var state = SCADSState.simulate(0,initialConfig,histograms(histogramEndTimes(0)),performanceModel,simulationGranularity,SCADSActivity(),requestFraction)

		var tnow = simulationGranularity
		var tlast = histogramEndTimes.last
		
		// main policy simulation loop
		while (tnow <= tlast) {
			
			// update the histogram
			if (tnow >= histogramEnd  &&  histogramI+1<histogramEndTimes.size) {
				histogramI += 1
				histogramEnd = histogramEndTimes(histogramI)
				histogramRaw = histograms(histogramEnd)				

				if (dumpStatePeriodically) SCADSState.dumpState(state)
			}
			
			// update the policy (mainly the workload prediction)
			if (tnow >= lastPolicyUpdate+policyUpdatePeriod) {
				policy.periodicUpdate(state)
				lastPolicyUpdate += policyUpdatePeriod
			}
			
			// execute the policy and adjust lastPolicyExec based on duration of policy.perform
			if (tnow >= lastPolicyExec+policyExecPeriod) {
				Action.currentInitTime = tnow
				val pt0 = new java.util.Date().getTime
				policy.perform(state,actionExecutor)
				val pt1 = new java.util.Date().getTime
				lastPolicyExec += (pt1-pt0) + policyExecPeriod
			}				
			
			// simulate execution of actions
			val (config,activity) = actionExecutor.simulateExecution(tnow-simulationGranularity,tnow,state.config)
			
			// update state
			state = SCADSState.simulate(tnow,config,histogramRaw,performanceModel,simulationGranularity,activity,requestFraction)
			//SCADSState.dumpState(state)
			costFunction.addState(state)
			Director.logger.info("STATE: \n"+state.toShortString)

			tnow += simulationGranularity
		}

		SCADSState.dumpState(state)
		Plotting.plotSimpleDirectorAndConfigs()
		
		val detailedCost = costFunction.detailedCost
		val nserverunits = detailedCost.filter(_.costtype=="server").map(_.units).reduceLeft(_+_)
		val nslaviolations = if (detailedCost.filter(_.costtype=="SLA").size>0) detailedCost.filter(_.costtype=="SLA").map(_.units).reduceLeft(_+_) else 0.0
		PolicySimulator.logger.info("simulation done (#server units:"+nserverunits+", #SLA violations:"+nslaviolations+") ["+
				"%.1f".format((new java.util.Date().getTime()-simT0)/1000.0)+" sec]")
		SimulationResult(policy.getClass.toString.split('.').last,workloadName,policy.getParams,nserverunits,nslaviolations,costFunction)
	}
		
}

object RunPolicySimulator {
  	def main(args: Array[String]) {
//		PolicySimulator.test1("/Users/bodikp/Downloads/scads/","/Users/bodikp/workspace/scads/")
//		r
//		r.saveToCSV("/tmp/data.csv")		
		PolicySimulator.optimizeCost("/Users/bodikp/Downloads/scads/","/Users/bodikp/workspace/scads/")
  	}
}

object PolicySimulator {

	val logger = Logger.getLogger("scads.director.simulation")
	private val logPath = Director.basedir+"/../simulation.txt"
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
	logger.setLevel(DEBUG)
	
	def test1(basedir:String, repodir:String):SimulationResults = {
		val maxKey = 10000

		val modelfile = repodir+"/experiments/scripts/perfmodels/gp_model.csv"
		val performanceModel = LocalL1PerformanceModel(modelfile)		

/*		val workloadName = "/ebates_mix99_mix99_1500users_200bins_20sec.hist"*/
		//val workloadName = "/ebates_mix99_mix99_1500users_1000bins_20sec.hist"
		val workloadName = "/dbworkload.hist"
		val workloadFile = basedir + workloadName
		val w = WorkloadHistogram.loadHistograms( workloadFile )
		
		//val costFunction = FullSLACostFunction(100,100,0.99,1*60*1000,100,1,2*60*1000)
		val costFunction = FullSLACostFunction(100,100,0.99,1*60*1000,100,1,10*60*1000)
		
		var results = SimulationResults()
		
		var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		config = config.splitAllInHalf.splitAllInHalf.splitAllInHalf
		val policySimulator = PolicySimulator()		

//		for (overprovision <- List(0.0, 0.1, 0.2, 0.3, 0.4)) {
//			for (alpha_up <- List(1.0, 0.5, 0.1, 0.05, 0.01)) {
//				for (alpha_down <- List(1.0, 0.5, 0.1, 0.05, 0.01)) {
				val overprovision = 0.1
				val alpha_up = 0.5
				val alpha_down = 0.05
					val policy = new SplitAndMergeOnWorkload( 1500, 1500, SimpleHysteresis(alpha_up,alpha_down,overprovision) )
					results.addResult( policySimulator.simulateSingleWorkload(config, workloadName, w, policy, costFunction, performanceModel, true, false) )
					results.saveToCSV("/tmp/simulation.csv")
//				}
//			}
//		}

		results
	}

	def optimizeCost(basedir:String, repodir:String) {
		val space1 = Map(
		"up" 	-> List(1.0, 0.5, 0.1, 0.05, 0.01),
		"down" 	-> List(1.0, 0.5, 0.1, 0.05, 0.01),
		"over"	-> List(0.0, 0.1, 0.2, 0.3, 0.4, 0.5) )
		
		val space2 = Map(
		"up" 	-> List(1.0, 0.7, 0.5, 0.2, 0.1, 0.07, 0.05, 0.02, 0.01),
		"down" 	-> List(1.0, 0.7, 0.5, 0.2, 0.1, 0.07, 0.05, 0.02, 0.01),
		"over"	-> List(0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5) )
		
		val space = space2
		
		var values = scala.collection.mutable.Map( "up"->1.0, "down"->1.0, "over"->0.0 )
		val paramOrder = List("up","down","over")
		var paramI = -1
		var param = ""
		
		val results = SimulationResults()

		val maxKey = 10000
		val modelfile = repodir+"/experiments/scripts/perfmodels/gp_model.csv"
		val performanceModel = LocalL1PerformanceModel(modelfile)		
		val workloadName = "/ebates_mix99_mix99_1500users_1000bins_20sec.hist"
		val workloadFile = basedir + workloadName
		val w = WorkloadHistogram.loadHistograms( workloadFile )

		val costFunction = FullSLACostFunction(100,100,0.99,1*60*1000,100,1,10*60*1000)
		val policySimulator = PolicySimulator()		
		
		PolicySimulator.logger.info("starting with values: "+values.toList.sort(_._1<_._1).map(p=>p._1+"="+p._2).mkString(", "))
		
		var nIterations = 0
		var maxIterations = 6
		
		while (nIterations <= maxIterations) {
			// pick parameter to search over
			nIterations += 1
			paramI = (paramI+1)%paramOrder.size
			param = paramOrder(paramI)

			PolicySimulator.logger.info("optimizing over "+param)

			var lineCosts = scala.collection.mutable.Map[Double,Double]()
			
			for (value <- space(param)) {
				var c = values.clone
				c += param -> value

				var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey)).splitAllInHalf.splitAllInHalf.splitAllInHalf
				val policy = new SplitAndMergeOnWorkload( 1500, 1500, SimpleHysteresis( c("up"), c("down"), c("over") ) )
				val result = policySimulator.simulateSingleWorkload(config, workloadName, w, policy, costFunction, performanceModel, true, false)
				results.addResult( result )
				
				lineCosts += value -> result.costFunction.cost
			}
			
			val minValue = lineCosts.toList.sort(_._2<_._2)(0)._1
			values(param) = minValue
			
			results.saveToCSV("/Users/bodikp/Downloads/scads/simulation_opt.csv")
			PolicySimulator.logger.info("line costs: "+lineCosts.toList.sort(_._1<_._1).map(p=>p._1+"->"+p._2).mkString(", "))
			PolicySimulator.logger.info("new values: "+values.toList.sort(_._1<_._1).map(p=>p._1+"="+p._2).mkString(", "))			
		}
		
		PolicySimulator.logger.info("done with optimization")
	}
	
}