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
	
	/**
	* run a simulation of 'policy' against the 'workload', using 'costFunction' for cost
	*/
	def directorSimulation(initialConfig:SCADSconfig, workload:WorkloadDescription, policy:Policy, costFunction:CostFunction, performanceModel:PerformanceModel, 
							evaluateAllSteps:Boolean):Double = {
		
		val workloadMultiplier = 10  		// workload rate of a single user
		val nHistogramBinsPerServer = 20
		val simulationGranularity = 60 		//seconds
		var nextStep = 0
		var currentTime = 0
		
		val startTime = new Date().getTime
		
		// create initial state (state = config + workload histogram + performance metrics)
		//var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		var config = initialConfig
		
		var totalCost = 0.0

		var pastActions = List[Action]()
		
		for (w <- workload.workload) { 
			logger.info("TIME: "+currentTime)
			if (currentTime>=nextStep) {
				// enough time passed, time for a simulation step
				logger.info("simulating ...")

				// create workload histogram from 'w'
				val histogram = WorkloadHistogram.create(w,w.numberOfActiveUsers*workloadMultiplier,config.storageNodes.size*nHistogramBinsPerServer)

				//logger.info("CONFIG: \n"+config)
				//logger.info("HISTOGRAM: \n"+histogram)
				logger.info("WORKLOAD: "+histogram.toShortString)

				// create the new state
				//val state = new SCADSState(new Date(startTime+currentTime), config, null, null, null, histogram)
				val state = SCADSState.createFromPerfModel(new Date(startTime+currentTime),config,histogram,performanceModel,w.duration/1000)
				logger.info("STATE: \n"+state.toShortString)

				// compute cost of the new state
				val cost = costFunction.cost(state)
				totalCost += cost
				logger.info("COST: "+cost)
				
				// ask policy for actions
				val actions = policy.perform(state,pastActions)
				
				// update config by executing actions on it
				config = state.config
				if (actions!=null)
					for (action <- actions) { config = action.preview(config); action.complete; pastActions+=action }
				val actionMsg = if (actions==null||actions.size==0) "\nACTIONS:\n<none>" else
									actions.map(_.toString).mkString("\nACTIONS: \n  ","\n  ","")
				logger.info(actionMsg)

				nextStep += simulationGranularity
				
			} else {
				if (evaluateAllSteps) {
					// don't simulate policy, just evaluate the config under the current workload
				 
					// create workload histogram from 'w'
					val histogram = WorkloadHistogram.create(w,w.numberOfActiveUsers*workloadMultiplier,config.storageNodes.size*nHistogramBinsPerServer)
					logger.info("WORKLOAD: "+histogram.toShortString)
				
					// create new state
					val state = SCADSState.createFromPerfModel(new Date(startTime+currentTime),config,histogram,performanceModel,w.duration/1000)
					logger.info("STATE: \n"+state.toShortString)
				}
			}
			currentTime += w.duration/1000
		}
		totalCost
	}
	
	def testSimulation() {
		val mix = new MixVector( Map("get"->0.98,"getset"->0.0,"put"->0.02) )
		val workload = WorkloadGenerators.diurnalWorkload(mix,0,"perfTest256",10,2,30,1000)

		val maxKey = 10000
		var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		config = config.splitAllInHalf.splitAllInHalf.splitAllInHalf

		val policy = new SplitAndMergeOnPerformance(1200,1500)
		//val policy = new RandomSplitAndMergePolicy(0.5)

		val performanceModel = L1PerformanceModel("/Users/bodikp/Downloads/l1model_getput_1.0.RData")
		val performanceEstimator = SimplePerformanceEstimator(performanceModel)
		val costFunction = new SLACostFunction(100,100,0.99,100,1,performanceEstimator)

		directorSimulation(config,workload,policy,costFunction,performanceModel,true)
	}
	
	def testSimulation2() {
		val mix = new MixVector( Map("get"->0.97,"getset"->0.0,"put"->0.03) )
		val workload = WorkloadGenerators.diurnalWorkload(mix,0,"perfTest256",10,2,30,1000)

		val maxKey = 10000
		var config = SCADSconfig.getInitialConfig(DirectorKeyRange(0,maxKey))
		config = config.splitAllInHalf.splitAllInHalf.splitAllInHalf

		val policy = new EmptyPolicy()

		val performanceModel = L1PerformanceModel("/Users/bodikp/Downloads/l1model_getput_1.0.RData")
		val performanceEstimator = SimplePerformanceEstimator(performanceModel)
		val costFunction = new SLACostFunction(100,100,0.99,100,1,performanceEstimator)

		directorSimulation(config,workload,policy,costFunction,performanceModel,false)
	}
}