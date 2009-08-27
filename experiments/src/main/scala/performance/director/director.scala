package scads.director

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
	var myscads:Scads = null
	var directorRunner:Runner = null
	var serverManager:ScadsServerManager = null

	val databaseHost = "localhost"
	val databaseUser = "root"
	val databasePassword = ""
	
	val delay = 20
	
	val logger = Logger.getLogger("scads.director.director")
	private val logPath = Director.basedir+"/director.txt"
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
	logger.setLevel(DEBUG)

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

	case class Runner(policy:Policy,placementIP: String) extends Runnable {
		val metricReader = new MetricReader(databaseHost,"metrics",20,0.02)		
		var actions = List[Action]()

		var running = true
		def run = {
			while (running) {
				val state = SCADSState.refresh(metricReader, placementIP)
				logger.info("FRESH STATE: \n"+state.toShortString)

				val newActions = policy.perform(state,actions)
				if (newActions!=null && newActions.length>0) {
					// start executing new actions
					newActions.foreach((a:Action) => logger.info("EXECUTING: "+a.toString))
					newActions.foreach(_.startExecuting)
					actions ++= newActions
				} else logger.info("no new actions")

				Thread.sleep(delay*1000)
			}
		}
		def stop = { running = false }
	}

	def setDeployment(deploy_name:String) {
		myscads = new Scads(deploy_name,xtrace_on,namespace)
		if (!myscads.loadState) setupScads(myscads)
		serverManager = new ScadsServerManager(deploy_name, xtrace_on,namespace)
	}

	def direct(policy:Policy) {
		// check for  scads deployment instance
		if (myscads == null) { println("Need scads deployment before directing"); return }
		val placementIP = myscads.placement.get(0).privateDnsName
		val dpclient = Scads.getDataPlacementHandle(placementIP,xtrace_on)
		assert( dpclient.lookup_namespace(namespace).size > 0, "Placement server has no storage nodes registered" )
		logger.info("Will be directing with placement host: "+placementIP)
		
		directorRunner = new Runner(policy,placementIP)
		val runthread = new Thread(directorRunner)
		runthread.start
	}
	def stop = directorRunner.stop

	private def setupScads(deployment:Scads):String = {
		deployment.init(1)
		deployment.replicate(0,10000) // max 4194304, in memory 2000000
		deployment.placement.get(0).privateDnsName // return placement host name
	}
	
	private def writeMaps(maps: Array[Map[DirectorKeyRange,List[String]]], prefix:String) {
		/*(0 until maps.size).foreach((i)=>{
			val file = new FileWriter( new java.io.File("/Users/trush/Downloads/"+prefix+i+".csv"), true )
			val mapstats = maps(i)
			mapstats.toList.sort(_._1.minKey < _._1.minKey).foreach((entry)=> file.write(entry._1.minKey+"_"+entry._1.maxKey+","+entry._2.mkString("",",","") +"\n"))
			file.flush
			file.close
		})*/
	}

	/**
	* run a simulation of 'policy' against the 'workload', using 'costFunction' for cost
	*/
	def directorSimulation(initialConfig:SCADSconfig, workload:WorkloadDescription, maxKey:Int,policy:Policy, costFunction:FullCostFunction, performanceModel:PerformanceModel, evaluateAllSteps:Boolean):FullCostFunction = {
		
		val workloadMultiplier = 10  		// workload rate of a single user
		val nHistogramBinsPerServer = 20
		val simulationGranularity = 60 		//seconds
		val latency90pThr = 100
		var nextStep = 0
		var currentTime = 0
		
		val startTime = new Date().getTime/(simulationGranularity*1000)*(simulationGranularity*1000)
		var ranges:List[DirectorKeyRange] = null // use same ranges for whole simulation
		val getStats = scala.collection.mutable.Map[DirectorKeyRange,scala.collection.mutable.Buffer[String]]()
		val putStats = scala.collection.mutable.Map[DirectorKeyRange,scala.collection.mutable.Buffer[String]]()
		// create initial state (state = config + workload histogram + performance metrics)
		//var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		var config = initialConfig
		
		var prevTimestep:Long = -1

		var pastActions = List[Action]()
		
		for (w <- workload.workload) { 
			logger.info("TIME: "+currentTime)
			if (ranges == null) ranges = WorkloadHistogram.createRanges(w,w.numberOfActiveUsers*workloadMultiplier,config.storageNodes.size*nHistogramBinsPerServer,maxKey)

			if (currentTime>=nextStep) {
				// enough time passed, time for a simulation step
				logger.info("simulating ...")

				// create workload histogram from 'w'
				//val histogram = WorkloadHistogram.create(w,w.numberOfActiveUsers*workloadMultiplier,config.storageNodes.size*nHistogramBinsPerServer,maxKey)
				val histogram = WorkloadHistogram.createFromRanges(ranges,w,w.numberOfActiveUsers*workloadMultiplier,config.storageNodes.size*nHistogramBinsPerServer)
				histogram.rangeStats.foreach((entry)=>{
					getStats(entry._1) = getStats.getOrElse(entry._1, new scala.collection.mutable.ListBuffer[String]()) + entry._2.getRate.toString
					putStats(entry._1) = putStats.getOrElse(entry._1, new scala.collection.mutable.ListBuffer[String]()) + entry._2.putRate.toString
				})

				//logger.info("CONFIG: \n"+config)
				//logger.info("HISTOGRAM: \n"+histogram)
				logger.info("WORKLOAD: "+histogram.toShortString)

				// create the new state
				//val state = new SCADSState(new Date(startTime+currentTime), config, null, null, null, histogram)
				val state = SCADSState.createFromPerfModel(new Date(startTime+currentTime*1000),config,histogram,performanceModel,w.duration/1000)
				logger.info("STATE: \n"+state.toShortString)
				SCADSState.dumpState(state)
				costFunction.addState(state)

				// ask policy for actions
				val actions = policy.perform(state,pastActions)
				
				// update config by executing actions on it
				config = state.config
				if (actions!=null)
					for (action <- actions) { config = action.preview(config); action.complete; pastActions+=action }
				val actionMsg = if (actions==null||actions.size==0) "\nACTIONS:\n<none>" else
									actions.map(_.toString).mkString("\nACTIONS: \n  ","\n  ","")
				logger.info(actionMsg)

				if (prevTimestep!= -1) Plotting.plotSCADSState(state,prevTimestep,startTime+currentTime*1000,latency90pThr,"state_"+(startTime+currentTime*1000)+".png")

				nextStep += simulationGranularity				
				prevTimestep = startTime + currentTime*1000
				
			} else {
				if (evaluateAllSteps) {
					// don't simulate policy, just evaluate the config under the current workload
				 
					// create workload histogram from 'w'
					val histogram = WorkloadHistogram.createFromRanges(ranges,w,w.numberOfActiveUsers*workloadMultiplier,config.storageNodes.size*nHistogramBinsPerServer)
					histogram.rangeStats.foreach((entry)=>{
							getStats(entry._1) = getStats.getOrElse(entry._1, new scala.collection.mutable.ListBuffer[String]()) + entry._2.getRate.toString
							putStats(entry._1) = putStats.getOrElse(entry._1, new scala.collection.mutable.ListBuffer[String]()) + entry._2.putRate.toString
					})

					logger.info("WORKLOAD: "+histogram.toShortString)

					// create new state
					val state = SCADSState.createFromPerfModel(new Date(startTime+currentTime*1000),config,histogram,performanceModel,w.duration/1000)
					logger.info("STATE: \n"+state.toShortString)
					SCADSState.dumpState(state)
					costFunction.addState(state)

					if (prevTimestep!= -1) Plotting.plotSCADSState(state,prevTimestep,startTime+currentTime*1000,latency90pThr,"state_"+(startTime+currentTime*1000)+".png")
					prevTimestep = startTime + currentTime*1000
				}
			}
			currentTime += w.duration/1000
		}

		// write out map stats
		writeMaps(Array(
			Map[DirectorKeyRange,List[String]](getStats.toList map {entry => (entry._1, entry._2.toList)} : _*),
			Map[DirectorKeyRange,List[String]](putStats.toList map {entry => (entry._1, entry._2.toList)} : _*)
		),startTime.toString)

		costFunction
	}
	
	def testSimulation():FullCostFunction = {
		Director.dropDatabases
		SCADSState.initLogging("localhost",6001)
		Plotting.initialize(Director.basedir+"/plotting/")
		
		val mix = new MixVector( Map("get"->0.98,"getset"->0.0,"put"->0.02) )
		val workload = WorkloadGenerators.diurnalWorkload(mix,0,"perfTest256",10,1,10,1000)

		val maxKey = 10000
		var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		config = config.splitAllInHalf.splitAllInHalf.splitAllInHalf

		val policy = new SplitAndMergeOnPerformance(1200,1500)
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

		val mix = new MixVector( Map("get"->0.97,"getset"->0.0,"put"->0.03) )
		val workload = WorkloadGenerators.diurnalWorkload(mix,0,"perfTest256",10,2,30,1000)

		val maxKey = 10000
		var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		config = config.splitAllInHalf.splitAllInHalf.splitAllInHalf

		val policy = new EmptyPolicy()

		val performanceModel = L1PerformanceModel("/Users/bodikp/Downloads/l1model_getput_1.0.RData")
		val performanceEstimator = SimplePerformanceEstimator(performanceModel)
		//val costFunction = new SLACostFunction(100,100,0.99,100,1,performanceEstimator)
		val costFunction = FullSLACostFunction(100,100,0.99,1*60*1000,100,1,1*60*1000)

		directorSimulation(config,workload,maxKey,policy,costFunction,performanceModel,false)
	}
	def testHeuristicSimulation(modelfile:String) {
		Director.dropDatabases
		SCADSState.initLogging("localhost",6001)
		Plotting.initialize(Director.basedir+"/plotting/")

		val mix = new MixVector( Map("get"->1.0,"getset"->0.0,"put"->0.00) )
		val workload = WorkloadGenerators.diurnalWorkload(mix,0,"perfTest256",10,2,30,280)
		//val workload = WorkloadGenerators.linearWorkload(1.0,0.0,0,"10000","perfTest256",1000,10000,10)
		val maxKey = 10000
		var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		config = config.splitAllInHalf.splitAllInHalf

		val policy = new HeuristicOptimizerPolicy(modelfile,100,100)

		val performanceModel = L1PerformanceModel(modelfile)
		val performanceEstimator = SimplePerformanceEstimator(performanceModel)
		//val costFunction = new SLACostFunction(100,100,0.99,100,1,performanceEstimator)
		val costFunction = FullSLACostFunction(100,100,0.99,1*60*1000,100,1,1*60*1000)

		directorSimulation(config,workload,maxKey,policy,costFunction,performanceModel,false)
	}
}