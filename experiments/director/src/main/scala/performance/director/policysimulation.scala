package scads.director

import org.apache.log4j._
import org.apache.log4j.Level._

case class SimulationResult(
	policyName:String,
	workloadName:String,
	policyParameters:Map[String,String],
	nServerUnits:Double,
	nSLAViolations:Double,
	fractionSlow:Double,
	fractionGetSlow:Double,
	fractionPutSlow:Double,
	cost:Double,
	costFunction:FullCostFunction
) {
	def csvHeader:String = {
		"policy,workload"+
		policyParameters.keySet.toList.sort(_<_).map("p:"+_).mkString(",",",",",")+
		"nServerUnits,nSLAViolations,percentageSlow,percentageGetSlow,percentagePutSlow,cost"
	}
	
	def csvData:String = {
		policyName+","+workloadName+
		policyParameters.toList.sort(_._1<_._1).map(_._2).mkString(",",",",",")+
		nServerUnits.toString+","+nSLAViolations+","+
		"%.4f".format(100*fractionSlow)+","+"%.4f".format(100*fractionGetSlow)+","+"%.4f".format(100*fractionPutSlow)+","+cost
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
						
		Director.LOG_ACTIONS = false
		Director.dropDatabases
		Director.initialize(policy.toString+"_w="+workloadName)
		if (deterministic) Director.resetRnd(7)

		val simT0 = new java.util.Date().getTime
		PolicySimulator.logger.info("starting simulation with workload "+workloadName+" and parameters: "+policy.getParams.toString)
			
		SCADSState.initLogging("localhost",6001)
		Plotting.initialize(Director.basedir)
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
			if (dumpStatePeriodically  &&  tnow%policyUpdatePeriod==0) SCADSState.dumpState(state)						
			costFunction.addState(state)
			Director.logger.info("STATE: \n"+state.toShortString)

			tnow += simulationGranularity
		}

		//SCADSState.dumpState(state)
		Plotting.plotSimpleDirectorAndConfigs()
		
		var costString = costFunction.toString
		Director.summaryLogger.info("COST:\n"+costString)
		Director.summaryLogger.info("PERFORMANCE STATS:\n"+costFunction.performanceStats)
		
		val detailedCost = costFunction.detailedCost
		val nserverunits = detailedCost.filter(_.costtype=="server").map(_.units).reduceLeft(_+_)
		val nslaviolations = if (detailedCost.filter(_.costtype=="SLA").size>0) detailedCost.filter(_.costtype=="SLA").map(_.units).reduceLeft(_+_) else 0.0
		PolicySimulator.logger.info("simulation done (#server units:"+nserverunits+", #SLA violations:"+nslaviolations+") ["+
				"%.1f".format((new java.util.Date().getTime()-simT0)/1000.0)+" sec]")
				
		val costStats = costFunction.getStats
		SimulationResult(policy.getClass.toString.split('.').last,workloadName,policy.getParams,
			costStats("nserverunits"),costStats("nslaviolations"),
			costStats("fractionslow"),costStats("fractiongetslow"),costStats("fractionputslow"),costStats("cost"),costFunction)
	}
		
}

object RunPolicySimulator {
  	def main(args: Array[String]) {
//		PolicySimulator.test1("/Users/bodikp/Downloads/scads/workloads_for_sim/","/Users/bodikp/workspace/scads/")
//		PolicySimulator.comparison1("/Users/bodikp/Downloads/scads/workloads_for_sim/","/Users/bodikp/workspace/scads/")
		PolicySimulator.tryOneDimension("/Users/bodikp/Downloads/scads/workloads_for_sim/","/Users/bodikp/workspace/scads/")
//		PolicySimulator.tryMoreKeys("/Users/bodikp/Downloads/scads/workloads_for_sim/","/Users/bodikp/workspace/scads/")
//		r.saveToCSV("/tmp/data.csv")		
//		PolicySimulator.optimizeCost("/Users/bodikp/Downloads/scads/","/Users/bodikp/workspace/scads/")
  	}
}

object PolicySimulator {

	val costFunction = FullSLACostFunction(100,100,0.99,5*60*1000,100,1,10*60*1000,10*60*1000)

	val logger = Logger.getLogger("scads.director.simulation")
	private val logPath = Director.basedir+"/../simulation.txt"
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
	logger.setLevel(DEBUG)
	
	def test1(basedir:String, repodir:String):SimulationResults = {
		val modelfile = repodir+"/experiments/scripts/perfmodels/gp_model2_thr.csv"
		val performanceModel = L1PerformanceModelWThroughput(modelfile)

		//val workloadName = "/ebates1.hist"
		//val workloadName = "/wikipedia0.hist"
		//val workloadName = "/wikipedia_5000users.hist"
		val workloadName = "/wikipedia_5000users_8hours.hist"
		//val workloadName = "/wikipedia_5000users_3days.hist"
		//val workloadName = "/dbworkload.hist"
		val workloadFile = basedir + workloadName
		val w = multiplyWorkload( WorkloadHistogram.loadHistograms( workloadFile ), 0.7 )
		
		var results = SimulationResults()

		val maxKey = w.values.toList.first.rangeStats.keys.map(_.maxKey).reduceLeft( Math.max(_,_) )
		println("maxkey="+maxKey)
		val policySimulator = PolicySimulator()		

		val overprovision = 0.3
		val alpha_up = 1.0
		val alpha_down = 0.04

		var nodesConfig = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey)).splitAllInHalf.splitAllInHalf.splitAllInHalf.storageNodes
		println("XXX: "+nodesConfig.toString)

		//val policy = new SplitAndMergeOnWorkload( 1500, 1500, SimpleHysteresis(alpha_up,alpha_down,overprovision) )
		var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey)).splitAllInHalf.splitAllInHalf.splitAllInHalf
		config = config.updateNodes(nodesConfig)
		println("XXX: "+nodesConfig.toString)
		println("YYY: "+config.storageNodes.toString)
		var policy = new HeuristicOptimizerPolicy( performanceModel, 100, 100, SimpleHysteresis(alpha_up,alpha_down,overprovision) )
		results.addResult( policySimulator.simulateSingleWorkload(config, "wikipedia", w, policy, costFunction, performanceModel, true, true) )
		
//		config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey)).splitAllInHalf.splitAllInHalf.splitAllInHalf					
//		config = config.updateNodes(nodesConfig)
//		println("XXX: "+nodesConfig.toString)
//		println("YYY: "+config.storageNodes.toString)
//		policy = new HeuristicOptimizerPolicy( performanceModel, 100, 100, SimpleHysteresis(alpha_up,alpha_down,overprovision) )
//		results.addResult( policySimulator.simulateSingleWorkload(config, "wikipedia", w, policy, costFunction, performanceModel, true, false) )
		
		results.saveToCSV("/tmp/simulation.csv")

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

		val modelfile = repodir+"/experiments/scripts/perfmodels/gp_model2_thr.csv"
		val performanceModel = L1PerformanceModelWThroughput(modelfile)
		val workloadName = "/ebates_mix99_mix99_1500users_1000bins_20sec.hist"
		val workloadFile = basedir + workloadName
		val w = WorkloadHistogram.loadHistograms( workloadFile )
		val maxKey = w.values.toList.first.rangeStats.keys.map(_.maxKey).reduceLeft( Math.max(_,_) )

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

	def tryMoreKeys(basedir:String, repodir:String) {

		val policySimulator = PolicySimulator()
		val results = SimulationResults()
		
		val modelfile = repodir+"/experiments/scripts/perfmodels/gp_model2_thr.csv"
		val performanceModel = L1PerformanceModelWThroughput(modelfile)
		
		val workloads = Map( 	"ebates1" -> WorkloadHistogram.loadHistograms( basedir+"/eb1.hist" ),
								"ebates2" -> WorkloadHistogram.loadHistograms( basedir+"/eb2.hist" ),
								"ebates3" -> WorkloadHistogram.loadHistograms( basedir+"/eb3.hist" ),
								"ebates4" -> WorkloadHistogram.loadHistograms( basedir+"/eb4.hist" ),
								"ebates5" -> WorkloadHistogram.loadHistograms( basedir+"/eb5.hist" )  )
		
		val maxKey = workloads.values.toList.first.values.toList.first.rangeStats.keys.map(_.maxKey).reduceLeft( Math.max(_,_) )

		val values = scala.collection.mutable.Map( "up"->0.5, "down"->0.05, "safety"->0.3 )
		val c = values

		// "add more keys" to the histograms by multiplying the keys by a factor
		for (factor <- List(100.0, 5.0, 1.0)) {

			// remember one node config so that all simulations start from the same one
			var adjustedMaxKey = (factor*maxKey).toInt
			var nodesConfig = SCADSconfig.getInitialConfig(DirectorKeyRange(0,adjustedMaxKey)).splitAllInHalf.splitAllInHalf.splitAllInHalf.storageNodes

			for (w <- workloads.keys.toList.sort(_<_)) {
				val workload = multiplyKeys( workloads(w), factor )
				val maxKey = workloads.values.toList.first.values.toList.first.rangeStats.keys.map(_.maxKey).reduceLeft( Math.max(_,_) )
				
				var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,adjustedMaxKey)).updateNodes(nodesConfig)
				//val policy = new SplitAndMergeOnWorkload( 1500, 1500, SimpleHysteresis( c("up"), c("down"), c("safety") ) )
				val policy = new HeuristicOptimizerPolicy( performanceModel, 100, 100, SimpleHysteresis( c("up"), c("down"), c("safety") ) )
				val result = policySimulator.simulateSingleWorkload(config, w, workload, policy, costFunction, performanceModel, true, true)
				results.addResult( result )
				results.saveToCSV("/Users/bodikp/Downloads/scads/simulation_morekeys_0.csv")
			}
			
		}
		
	}

	def tryOneDimension(basedir:String, repodir:String) {
		val space2 = Map(
		"up" 	-> List(1.0, 0.9, 0.8, 0.6, 0.5, 0.3, 0.2, 0.1, 0.05),
		"down" 	-> List(1.0, 0.7, 0.5, 0.2, 0.1, 0.07, 0.05, 0.02, 0.01, 0.007, 0.005),
		"safety"-> List(0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5) )
		
		val modelfile = repodir+"/experiments/scripts/perfmodels/gp_model2_thr.csv"
		val performanceModel = L1PerformanceModelWThroughput(modelfile)
		
		val workloadFactor = 3.0
		
		val plots = false
		
/*		val workloads = Map( 	"ebates1" -> multiplyWorkload( WorkloadHistogram.loadHistograms( basedir+"/eb1.hist" ), workloadFactor ),
								"ebates2" -> multiplyWorkload( WorkloadHistogram.loadHistograms( basedir+"/eb2.hist" ), workloadFactor ),
								"ebates3" -> multiplyWorkload( WorkloadHistogram.loadHistograms( basedir+"/eb3.hist" ), workloadFactor ),
								"ebates4" -> multiplyWorkload( WorkloadHistogram.loadHistograms( basedir+"/eb4.hist" ), workloadFactor ),
								"ebates5" -> multiplyWorkload( WorkloadHistogram.loadHistograms( basedir+"/eb5.hist" ), workloadFactor )  )
*/
		val workloads = Map( 	"ebates_wspike_1" -> multiplyWorkload( WorkloadHistogram.loadHistograms( basedir+"/eb_spike_1.hist" ), workloadFactor ),
								"ebates_wspike_2" -> multiplyWorkload( WorkloadHistogram.loadHistograms( basedir+"/eb_spike_2.hist" ), workloadFactor ),
								"ebates_wspike_3" -> multiplyWorkload( WorkloadHistogram.loadHistograms( basedir+"/eb_spike_3.hist" ), workloadFactor ),
								"ebates_wspike_4" -> multiplyWorkload( WorkloadHistogram.loadHistograms( basedir+"/eb_spike_4.hist" ), workloadFactor ),
								"ebates_wspike_4" -> multiplyWorkload( WorkloadHistogram.loadHistograms( basedir+"/eb_spike_5.hist" ), workloadFactor ),
								"ebates_wspike_5" -> multiplyWorkload( WorkloadHistogram.loadHistograms( basedir+"/eb_spike_6.hist" ), workloadFactor )  )


		val maxKey = workloads.values.toList.first.values.toList.first.rangeStats.keys.map(_.maxKey).reduceLeft( Math.max(_,_) )
		val policySimulator = PolicySimulator()
		val results = SimulationResults()
		
		val space = space2
		
		//val values = scala.collection.mutable.Map( "up"->1.0, "down"->1.0, "safety"->0.5 )
		//val param = "down"
		//val values = scala.collection.mutable.Map( "up"->1.0, "down"->0.5, "safety"->0.5 )
		//val param = "safety"
		//val values = scala.collection.mutable.Map( "up"->1.0, "down"->0.05, "safety"->0.4 )
		//val param = "up"
		
		//val values = scala.collection.mutable.Map( "up"->1.0, "down"->1.0, "safety"->0.5 )
		//val param = "down"
		//val outputFile = "/Users/bodikp/Downloads/scads/simulation_searchSpike_down0.csv"
		//val values = scala.collection.mutable.Map( "up"->1.0, "down"->0.1, "safety"->0.5 )
		//val param = "safety"
		//val outputFile = "/Users/bodikp/Downloads/scads/simulation_searchSpike_safety0.csv"
		val values = scala.collection.mutable.Map( "up"->1.0, "down"->0.05, "safety"->0.5 )
		val param = "up"
		val outputFile = "/Users/bodikp/Downloads/scads/simulation_searchSpike_up0.csv"
		
		for (value <- space(param)) {
			var c = values.clone
			c += param -> value

			for (w <- workloads.keys.toList.sort(_<_)) {
				var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey)).splitAllInHalf.splitAllInHalf.splitAllInHalf
				//val policy = new SplitAndMergeOnWorkload( 1500, 1500, SimpleHysteresis( c("up"), c("down"), c("safety") ) )
				val policy = new HeuristicOptimizerPolicy( performanceModel, 100, 100, SimpleHysteresis( c("up"), c("down"), c("safety") ) )
				val result = policySimulator.simulateSingleWorkload(config, w, workloads(w), policy, costFunction, performanceModel, true, plots)
				results.addResult( result )
				results.saveToCSV(outputFile)
			}
		}
		
	}

	def comparison1(basedir:String, repodir:String) {
		var values = List( 
			Map( "up"->0.9, "down"->0.05, "safety"->0.9 ),
			Map( "up"->0.9, "down"->0.05, "safety"->0.7 ),
			Map( "up"->0.9, "down"->0.05, "safety"->0.5 ),
			Map( "up"->0.9, "down"->0.05, "safety"->0.3 ),
			Map( "up"->0.9, "down"->0.05, "safety"->0.1 ),
			Map( "up"->1.0, "down"->0.01, "safety"->1.0 ),
			Map( "up"->0.9, "down"->0.02, "safety"->0.9 ),
			Map( "up"->0.9, "down"->0.02, "safety"->0.7 ),
			Map( "up"->0.9, "down"->0.02, "safety"->0.5 ),
			Map( "up"->0.9, "down"->0.02, "safety"->0.3 ),
			Map( "up"->0.9, "down"->0.02, "safety"->0.1 ),
			Map( "up"->0.9, "down"->0.1, "safety"->0.9 ),
			Map( "up"->0.9, "down"->0.1, "safety"->0.7 ),
			Map( "up"->0.9, "down"->0.1, "safety"->0.5 ),
			Map( "up"->0.9, "down"->0.1, "safety"->0.3 ),
			Map( "up"->0.9, "down"->0.1, "safety"->0.1 )
		)
		
		val modelfile = repodir+"/experiments/scripts/perfmodels/gp_model2_thr.csv"
		val performanceModel = L1PerformanceModelWThroughput(modelfile)
		
		val workloads = Map( 	"ebates1" -> WorkloadHistogram.loadHistograms( basedir+"/ebates1.hist" ),
								"ebates2" -> WorkloadHistogram.loadHistograms( basedir+"/ebates2.hist" ),
								"ebates3" -> WorkloadHistogram.loadHistograms( basedir+"/ebates3.hist" ),
								//"ebates4" -> WorkloadHistogram.loadHistograms( basedir+"/ebates4.hist" ),
								"ebates5" -> WorkloadHistogram.loadHistograms( basedir+"/ebates5.hist" ),
								"ebates6" -> WorkloadHistogram.loadHistograms( basedir+"/ebates6.hist" )  )
		val maxKey = workloads.values.toList.first.values.toList.first.rangeStats.keys.map(_.maxKey).reduceLeft( Math.max(_,_) )
		val policySimulator = PolicySimulator()
		val results = SimulationResults()

		for (v <- values) {
			for (w <- workloads.keys.toList.sort(_<_)) {
				var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey)).splitAllInHalf.splitAllInHalf
				val policy = new HeuristicOptimizerPolicy( performanceModel, 100, 100, SimpleHysteresis( v("up"), v("down"), v("safety") ) )
				val result = policySimulator.simulateSingleWorkload(config, w, workloads(w), policy, costFunction, performanceModel, true, false)
				results.addResult( result )
				results.saveToCSV("/Users/bodikp/Downloads/scads/simulation_comparison1.csv")
			}
		}
	}
	
	def multiplyKeys(histograms:Map[Long,WorkloadHistogram], factor:Double):Map[Long,WorkloadHistogram] = {
		Map( histograms.toList.map( p => p._1 -> p._2.multiplyKeys(factor) ) :_* )
	}

	def multiplyWorkload(histograms:Map[Long,WorkloadHistogram], factor:Double):Map[Long,WorkloadHistogram] = {
		Map( histograms.toList.map( p => p._1 -> p._2.multiplyWorkload(factor) ) :_* )
	}
}