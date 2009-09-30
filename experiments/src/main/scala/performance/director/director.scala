package scads.director

import scads.deployment.{Scads,ScadsLoader,ScadsDeploy}
import performance._
import java.io._
import org.apache.log4j._
import org.apache.log4j.Level._
import java.text.SimpleDateFormat
import java.util.Date

import java.sql.Connection
import java.sql.SQLException
import java.sql.DriverManager

object Director {
	val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	val logPattern = "%d %5p %c - %m%n"
/*	val basedir = "/mnt/director/logs_"+dateFormat.format(new Date)+"/"*/
	val basedir = "/tmp/director/logs_"+dateFormat.format(new Date)+"/"
	
	val xtrace_on = true
	val namespace = "perfTest256"
	var myscads:scads.deployment.Scads = null
	var directorRunner:Runner = null
	var serverManager:ScadsServerManager = null
	var putRestrictionURL:String = null
	val restrictFileName = "restrict.csv"

	val databaseHost = "localhost"
	val databaseUser = "root"
//	val databaseUser = "director"
	val databasePassword = ""

	var rnd = new java.util.Random(7)
	
	val delay = 20*1000
	
	val logger = Logger.getLogger("scads.director.director")
	private val logPath = Director.basedir+"/director.txt"
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
	logger.setLevel(DEBUG)
	Logger.getRootLogger().addAppender( new FileAppender(new PatternLayout(Director.logPattern),Director.basedir+"/all.txt",false) )
	Runtime.getRuntime().exec("rm -f "+Director.basedir+"../current")
	Runtime.getRuntime().exec("ln -s "+Director.basedir+" "+Director.basedir+"../current")

	var lowLevelActionMonitor = LowLevelActionMonitor("director","lowlevel_actions")

	def dumpAndDropDatabases() {
		// dump old databases
		Runtime.getRuntime.exec("mysqldump --databases metrics director > /mnt/director/dbdump_"+dateFormat.format(new Date)+".sql")
		dropDatabases()
	}

	def dropDatabases() {
		// drop old databases
		try {
			val connection = connectToDatabase()
            val statement = connection.createStatement
            statement.executeUpdate("DROP DATABASE IF EXISTS metrics")
            statement.executeUpdate("DROP DATABASE IF EXISTS director")
			statement.close
		} catch { case ex: SQLException => ex.printStackTrace() }
	}

	def connectToDatabase():Connection = {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance()
        } catch { case ex: Exception => ex.printStackTrace() }

		var connection:Connection = null
        try {
            val connectionString = "jdbc:mysql://" + databaseHost + "/?user=" + databaseUser + "&password=" + databasePassword
			logger.info("connecting to database: "+connectionString)
            connection = DriverManager.getConnection(connectionString)
		} catch {
			case ex: SQLException => {
	            println("can't connect to the database")
	            println("SQLException: " + ex.getMessage)
	            println("SQLState: " + ex.getSQLState)
	           	println("VendorError: " + ex.getErrorCode)
	        }
		}
		connection
	}

	def createInsertStatement(table:String, data:Map[String,String]):String = {
		val colnames = data.keySet.toList
		"INSERT INTO "+table+" ("+colnames.mkString("`","`,`","`")+") values ("+colnames.map(data(_)).mkString(",")+")"
	}

	// TODO: create a director class and move these there
	var policy:Policy = null
	var costFunction:FullCostFunction = null
	var actionExecutor:ActionExecutor = null

	case class Runner(policy:Policy, costFunction:FullCostFunction, placementIP: String) extends Runnable {
		Director.dropDatabases
		SCADSState.initLogging("localhost",6001)
		Plotting.initialize(Director.basedir+"/plotting/")
		policy.initialize
		lowLevelActionMonitor = LowLevelActionMonitor("director","lowlevel_actions")

		var lastPlotTime = new Date().getTime
		var plottingPeriod:Long = 2*60*1000

		val period:Long = 20*1000
		val metricReader = new MetricReader(databaseHost,"metrics",period,0.02)
		val stateHistory = SCADSStateHistory(period,metricReader,placementIP,policy)
		stateHistory.startUpdating

		var actions = List[Action]()
		val actionExecutor = ActionExecutor()

		Director.policy = policy
		Director.costFunction = costFunction
		Director.actionExecutor = actionExecutor

		var running = true
		def run = {
			while (running) {
				policy.perform(stateHistory.getMostRecentState,actionExecutor)
				actionExecutor.execute
				
				if (new Date().getTime>lastPlotTime+plottingPeriod) {
					Plotting.plotSimpleDirectorAndConfigs()
					lastPlotTime = new Date().getTime
				}
				
				Thread.sleep(delay)
			}
			
			// add all states to cost function
			stateHistory.history.toList.sort(_._1<_._1).map(_._2).foreach( costFunction.addState(_) )
		}
		def stop = { 
			running = false 
			stateHistory.stopUpdating
		}
	}

	def setDeployment(deploy_name:String) {
		myscads = ScadsLoader.loadState(deploy_name)
		serverManager = new ScadsServerManager(deploy_name, myscads.deployMonitoring,namespace)
	}

	def direct(policy:Policy, costFunction:FullCostFunction) {
		// check for  scads deployment instance
		if (myscads == null) { println("Need scads deployment before directing"); return }
		val placementIP = myscads.placement.get(0).privateDnsName
		putRestrictionURL = placementIP +"/"+restrictFileName
		val dpclient = ScadsDeploy.getDataPlacementHandle(placementIP,xtrace_on)
		assert( dpclient.lookup_namespace(namespace).size > 0, "Placement server has no storage nodes registered" )
		logger.info("Will be directing with placement host: "+placementIP)
		
		directorRunner = new Runner(policy,costFunction,placementIP)
		val runthread = new Thread(directorRunner)
		runthread.start
	}
	def stop = directorRunner.stop
	
	private def writeMaps(maps: Array[Map[DirectorKeyRange,List[String]]], prefix:String) {
		(0 until maps.size).foreach((i)=>{
			val file = new FileWriter( new java.io.File(Director.basedir+"/"+prefix+i+".csv"), true )
			val mapstats = maps(i)
			mapstats.toList.sort(_._1.minKey < _._1.minKey).foreach((entry)=> file.write(entry._1.minKey+"_"+entry._1.maxKey+","+entry._2.mkString("",",","") +"\n"))
			file.flush
			file.close
		})
	}

	private def writeMaps(maps: Map[String, Map[DirectorKeyRange,List[String]]], prefix:String) {
		maps.foreach( e => {
			val file = new FileWriter( new java.io.File(Director.basedir+"/"+e._1+".csv") )
			val mapstats = e._2
			mapstats.toList.sort(_._1.minKey < _._1.minKey).foreach((entry)=> file.write(entry._1.minKey+"_"+entry._1.maxKey+","+entry._2.mkString("",",","") +"\n"))
			file.flush
			file.close
		})
	}


	def directorSimulation2(initialConfig:SCADSconfig, histograms:Map[Long,WorkloadHistogram], policy:Policy, costFunction:FullCostFunction, performanceModel:PerformanceModel):FullCostFunction = {

		val workloadPredictor = SimpleHysteresis(0.9,0.05,0.2)
		val actionExecutor = ActionExecutor()
		val simulationGranularity = 10*1000
		val W_MULT = 0.1

		var timeSim0:Long = 0
		var timeSim1:Long = 0
		val histogramEndTimes = histograms.keySet.toList.sort(_<_)
		
		// create initial state
		var state = SCADSState.simulate(0,initialConfig,histograms(histogramEndTimes(0)),histograms(histogramEndTimes(0)),performanceModel,((histogramEndTimes(1)-histogramEndTimes(0))*W_MULT).toLong,SCADSActivity())

		for (timeHistogramOver <- histogramEndTimes) {
			val histogramRaw = histograms(timeHistogramOver)
			
			// predict workload
			workloadPredictor.addHistogram(histogramRaw)
			val histogramPrediction = workloadPredictor.getPrediction()
			
			while (timeSim1<=timeHistogramOver) { // simulate from timeSim0 to timeSim1
				timeSim0 = timeSim1
				timeSim1 += simulationGranularity

				// ask policy for actions
				logger.info("STATE: \n"+state.toShortString)
				policy.periodicUpdate(state)
				policy.perform(state,actionExecutor)
				
				// simulate execution of actions
				val (config,activity) = actionExecutor.simulateExecution(timeSim0,timeSim1,state.config)
				
				// update state
				state = SCADSState.simulate(timeSim1,config,histogramRaw,histogramPrediction,performanceModel,((timeSim1-timeSim0)*W_MULT).toLong,activity)
				SCADSState.dumpState(state)
				costFunction.addState(state)
			}
		}
		Plotting.plotSimpleDirectorAndConfigs()
		costFunction
	}

	/**
	* run a simulation of 'policy' against the 'workload', using 'costFunction' for cost
	*/
	def directorSimulation(initialConfig:SCADSconfig, workload:WorkloadDescription, maxKey:Int,policy:Policy, costFunction:FullCostFunction, performanceModel:PerformanceModel, evaluateAllSteps:Boolean):FullCostFunction = {
		val workloadMultiplier = 10  		// workload rate of a single user
		val nHistogramBins = 200
		val simulationGranularity = 10 		//seconds
		val histogramWindowSize = 6
		val latency90pThr = 100
		var nextStep = 0
		var currentTime = 0
		
		var plotUpdatePeriod = 10
		var plotCounter = 0

		val stateCreationDurationMultiplier = 0.1

		val startTime = new Date().getTime/(simulationGranularity*1000)*(simulationGranularity*1000)
		var ranges:List[DirectorKeyRange] = null // use same ranges for whole simulation

		val getStats = scala.collection.mutable.Map[DirectorKeyRange,scala.collection.mutable.Buffer[String]]()
		//val getStatsPrediction = scala.collection.mutable.Map[DirectorKeyRange,scala.collection.mutable.Buffer[String]]()
		val putStats = scala.collection.mutable.Map[DirectorKeyRange,scala.collection.mutable.Buffer[String]]()

		// create initial state (state = config + workload histogram + performance metrics)
		var config = initialConfig		
		var prevTimestep:Long = -1
		var pastActions = List[Action]()
		
		val actionExecutor = ActionExecutor()
		
		for (w <- workload.workload) { 
			var timing = new Date
			var timingString = ""

			logger.info("TIME: "+currentTime)
			if (ranges == null) ranges = WorkloadHistogram.createEquiWidthRanges(w,w.numberOfActiveUsers*workloadMultiplier,nHistogramBins,maxKey)

			// "observe" and smooth/predict the workload histogram
			val histogramRaw = WorkloadHistogram.createFromRanges(ranges.toArray,w,w.numberOfActiveUsers*workloadMultiplier,nHistogramBins)
			timingString += "histograms: "+(new Date().getTime-timing.getTime)/1000.0+" sec\n"; timing=new Date

			histogramRaw.rangeStats.foreach((entry)=>{
				getStats(entry._1) = getStats.getOrElse(entry._1, new scala.collection.mutable.ListBuffer[String]()) + entry._2.getRate.toString
				//getStatsPrediction(entry._1) = getStatsPrediction.getOrElse(entry._1, new scala.collection.mutable.ListBuffer[String]()) + histogramPrediction.rangeStats(entry._1).getRate.toString
				putStats(entry._1) = putStats.getOrElse(entry._1, new scala.collection.mutable.ListBuffer[String]()) + entry._2.putRate.toString
			})

			logger.info("WORKLOAD: "+histogramRaw.toShortString)
			//logger.info("WORKLOAD PREDICTION: "+histogramPrediction.toShortString)

			if (currentTime>=nextStep) {
				// enough time passed, time for a simulation step
				logger.info("simulating ...")

				// create the new state
				val state = SCADSState.createFromPerfModel(startTime+currentTime*1000,config,histogramRaw,performanceModel,(w.duration*stateCreationDurationMultiplier).toInt)
				logger.info("STATE: \n"+state.toShortString)
				timingString += "state: "+(new Date().getTime-timing.getTime)/1000.0+" sec (in model: "+performanceModel.timeInModel/1000.0+" sec)\n"; timing=new Date; performanceModel.resetTimer
				SCADSState.dumpState(state)
				timingString += "dump state: "+(new Date().getTime-timing.getTime)/1000.0+" sec\n"; timing=new Date
				costFunction.addState(state)

				// execute policy
				policy.periodicUpdate(state)
				policy.perform(state,actionExecutor)

				// simulate execution of actions
/*				val (config,activity) = actionExecutor.simulateExecution(prevTimestep,startTime+currentTime*1000,state.config)*/
				val r = actionExecutor.simulateExecutionWithoutEffects(prevTimestep,startTime+currentTime*1000,state.config)
				config = r._1
				
/*				// ask policy for actions
				val actions = policy.perform(state,pastActions)
				timingString += "policy: "+(new Date().getTime-timing.getTime)/1000.0+" sec (in model: "+performanceModel.timeInModel/1000.0+" sec)\n"; timing=new Date; performanceModel.resetTimer

				// update config by executing actions on it
				config = state.config
				if (actions!=null)
					for (action <- actions) { 
						config = action.preview(config); 
						action.complete; 
						action.startTime=startTime+currentTime*1000; action.endTime=startTime+currentTime*1000+simulationGranularity*1000;
						Action.store(action); pastActions+=action 
					}
				val actionMsg = if (actions==null||actions.size==0) "\nACTIONS:\n<none>" else
									actions.map(_.toString).mkString("\nACTIONS: \n  ","\n  ","")
				logger.info(actionMsg)
				timingString += "actions: "+(new Date().getTime-timing.getTime)/1000.0+" sec\n"; timing=new Date
*/				
				plotCounter+=1; if (plotCounter%plotUpdatePeriod==0) Plotting.plotSimpleDirectorAndConfigs()

				nextStep += simulationGranularity				
				prevTimestep = startTime + currentTime*1000
				
			} else {
				if (evaluateAllSteps) {
					// don't simulate policy, just evaluate the config under the current workload
					// create new state
					val state = SCADSState.createFromPerfModel(startTime+currentTime*1000,config,histogramRaw,performanceModel,w.duration/1000)
					logger.info("STATE: \n"+state.toShortString)
					SCADSState.dumpState(state)
					costFunction.addState(state)
					timingString += "state: "+(new Date().getTime-timing.getTime)/1000.0+" sec\n"; timing=new Date

					if (prevTimestep!= -1) Plotting.plotSCADSState(state,prevTimestep,startTime+currentTime*1000,latency90pThr,"state_"+(startTime+currentTime*1000)+".png")
					prevTimestep = startTime + currentTime*1000
				}
			}
			currentTime += w.duration/1000

			// write out map stats
			writeMaps(Map(
				"getHistogramsRaw" -> Map[DirectorKeyRange,List[String]](getStats.toList map {entry => (entry._1, entry._2.toList)} : _*),
				//"getHistogramsPrediction" -> Map[DirectorKeyRange,List[String]](getStatsPrediction.toList map {entry => (entry._1, entry._2.toList)} : _*),
				"putHistogramsRaw" -> Map[DirectorKeyRange,List[String]](putStats.toList map {entry => (entry._1, entry._2.toList)} : _*)
			),startTime.toString)

			logger.debug("TIMING:\n"+timingString)
		}

		Plotting.plotSimpleDirectorAndConfigs()
		costFunction
	}
	
	def testSimulation():FullCostFunction = {
		Director.dropDatabases
		SCADSState.initLogging("localhost",6001)
		Plotting.initialize(Director.basedir+"/plotting/")
		
		val maxKey = 10000
		val mix = new MixVector( Map("get"->0.98,"getset"->0.0,"put"->0.02) )
		val workload = WorkloadGenerators.diurnalWorkload(mix,0,"perfTest256",10,1,10,1000,maxKey)

		var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		config = config.splitAllInHalf.splitAllInHalf.splitAllInHalf

		val workloadPredictor = SimpleHysteresis(0.9,0.05,0.2)
		val policy = new SplitAndMergeOnPerformance(1200,1500,workloadPredictor)
		//val policy = new RandomSplitAndMergePolicy(0.5)

		val performanceModel = L1PerformanceModel("/Users/bodikp/Downloads/l1model_getput_1.0.RData")
		val performanceEstimator = SimplePerformanceEstimator(performanceModel)
		//val costFunction = new SLACostFunction(100,100,0.99,100,1,performanceEstimator)
		val costFunction = FullSLACostFunction(100,100,0.99,1*60*1000,100,1,2*60*1000)

		val finalCost = directorSimulation(config,workload,maxKey,policy,costFunction,performanceModel,false)
		finalCost
	}
	
	def testSimulation2() {
		Director.dropDatabases
		SCADSState.initLogging("localhost",6001)
		Plotting.initialize(Director.basedir+"/plotting/")

		val maxKey = 10000
		val mix = new MixVector( Map("get"->0.97,"getset"->0.0,"put"->0.03) )
		val workload = WorkloadGenerators.diurnalWorkload(mix,0,"perfTest256",10,2,30,1000,maxKey)

		var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		config = config.splitAllInHalf.splitAllInHalf.splitAllInHalf

		val workloadPredictor = SimpleHysteresis(0.9,0.05,0.2)
		val policy = new EmptyPolicy(workloadPredictor)

		val performanceModel = L1PerformanceModel("/Users/bodikp/Downloads/l1model_getput_1.0.RData")
		val performanceEstimator = SimplePerformanceEstimator(performanceModel)
		//val costFunction = new SLACostFunction(100,100,0.99,100,1,performanceEstimator)
		val costFunction = FullSLACostFunction(100,100,0.99,1*60*1000,100,1,1*60*1000)

		directorSimulation(config,workload,maxKey,policy,costFunction,performanceModel,false)
	}
	
	def testHeuristicSimulation(modelfile:String):FullCostFunction = {
		Director.dropDatabases
		SCADSState.initLogging("localhost",6001)
		Plotting.initialize(Director.basedir+"/plotting/")

		val maxKey = 10000
		val mix = new MixVector( Map("get"->1.0,"getset"->0.0,"put"->0.00) )
		val workload = WorkloadGenerators.diurnalWorkload(mix,0,"perfTest256",10,1.5,48,4000,maxKey)
		var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		config = config.splitAllInHalf.splitAllInHalf.splitAllInHalf

		val performanceModel = LocalL1PerformanceModel(modelfile)
		val workloadPredictor = SimpleHysteresis(0.9,0.05,0.2)
		val policy = new HeuristicOptimizerPolicy(performanceModel,100,100,workloadPredictor)
		val performanceEstimator = SimplePerformanceEstimator(performanceModel)
		val costFunction = FullSLACostFunction(100,100,0.99,1*60*1000,100,1,2*60*1000)

		val finalcost = directorSimulation(config,workload,maxKey,policy,costFunction,performanceModel,false)
		finalcost
	}

	def heuristicSimulation(workload:WorkloadDescription, modelfile:String):FullCostFunction = {
		Director.dropDatabases
		Director.rnd = new java.util.Random(7)
		WorkloadDescription.setRndGenerator(Director.rnd)
		SCADSState.initLogging("localhost",6001)
		Plotting.initialize(Director.basedir+"/plotting/")

		val mix = new MixVector( Map("get"->1.0,"getset"->0.0,"put"->0.00) )
		val maxKey = 10000
		var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		config = config.splitAllInHalf.splitAllInHalf.splitAllInHalf

		val performanceModel = LocalL1PerformanceModel(modelfile)
		val workloadPredictor = SimpleHysteresis(0.9,0.05,0.2)
		val policy = new HeuristicOptimizerPolicy(performanceModel,100,100,workloadPredictor)
		val performanceEstimator = SimplePerformanceEstimator(performanceModel)
		val costFunction = FullSLACostFunction(100,100,0.99,1*60*1000,100,1,2*60*1000)

		val finalcost = directorSimulation(config,workload,maxKey,policy,costFunction,performanceModel,false)
		finalcost
	}
	
	def heuristicSimulation2(histograms:Map[Long,WorkloadHistogram], modelfile:String):FullCostFunction = {
		Director.dropDatabases
		Director.rnd = new java.util.Random(7)
		WorkloadDescription.setRndGenerator(Director.rnd)
		SCADSState.initLogging("localhost",6001)
		Plotting.initialize(Director.basedir+"/plotting/")

		val mix = new MixVector( Map("get"->1.0,"getset"->0.0,"put"->0.00) )
		val maxKey = 10000
		var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		config = config.splitAllInHalf.splitAllInHalf.splitAllInHalf

		val performanceModel = LocalL1PerformanceModel(modelfile)		
		val workloadPredictor = SimpleHysteresis(0.9,0.05,0.2)
		//val policy = new HeuristicOptimizerPolicy(performanceModel,100,100,workloadPredictor)
		val policy = new SplitAndMergeOnWorkload(2000,1500,workloadPredictor)
		val performanceEstimator = SimplePerformanceEstimator(performanceModel)
		val costFunction = FullSLACostFunction(100,100,0.99,1*60*1000,100,1,2*60*1000)

		val finalcost = directorSimulation2(config,histograms,policy,costFunction,performanceModel)
		finalcost
	}
	
	def testSim(dir:String) {		
		import performance.WorkloadGenerators._
		val w = stdWorkloadEbatesWMixChange(mix99,mix99,1000)
		val h = WorkloadHistogram.createHistograms( w, 120*1000, 10, 200, 10000 )
		Director.heuristicSimulation2(h,dir+"/experiments/scripts/perfmodels/gp_model.csv")
	}
	
	def testSim2(histFile:String, repoDir:String) {
		val h = WorkloadHistogram.loadHistograms(histFile)
		Director.heuristicSimulation2(h,repoDir+"/experiments/scripts/perfmodels/gp_model.csv")
	}
	
	def createWorkloadHistograms(basedir:String) {
		import performance.WorkloadGenerators._
		val w = stdWorkloadEbatesWMixChange(mix99,mix99,1500)
		WorkloadHistogram.createAndSerializeHistograms(w, 20*1000, basedir+"/ebates_mix99_mix99_1500users_200bins_20sec.hist", 10, 200, 10000)
	}
}