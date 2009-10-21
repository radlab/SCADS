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


case class Director(
	deploymentName:String,
	policy:Policy,
	costFunction:FullCostFunction,
	experimentName:String
) {	
	val actionExecutor = ActionExecutor()
	
	var plottingPeriod:Long = 1*60*1000
	val period:Long = 20*1000	
	var costUpdatePeriod:Long = 10*60*1000

//	val metricReader = new MetricReader(Director.databaseHost,"metrics",period,0.02)
	val metricReader = new MetricReader(Director.databaseHost,"metrics",period,1.0)

	var myscads:scads.deployment.Scads = null
	var placementIP:String = null
	var putRestrictionURL:String = null
	
	var directorRunner:Runner = null
	var serverManager:ScadsServerManager = null

	Director.startRserve
	setDeployment(deploymentName)	
	Director.dropDatabases
	SCADSState.initLogging("localhost",6001)
	Plotting.initialize(Director.basedir)
	Plotting.startPlotting
	policy.initialize
	
	val lowLevelActionMonitor = LowLevelActionMonitor("director","lowlevel_actions")
	val stateHistory = SCADSStateHistory(period,metricReader,placementIP,policy)

	Director.director = this

	case class Runner(policy:Policy, costFunction:FullCostFunction, placementIP: String) extends Runnable {
		var lastPlotTime = new Date().getTime
		var lastCostUpdateTime = new Date().getTime
		stateHistory.setCostFunction(costFunction)
		stateHistory.startUpdating

		var running = true
		def run = {
			while (running) {
				policy.perform(stateHistory.getMostRecentState,actionExecutor)
				actionExecutor.execute
				
				if (new Date().getTime>lastCostUpdateTime+costUpdatePeriod) {
					costFunction.dumpToDB
					lastCostUpdateTime = new Date().getTime
				}
				
				Thread.sleep(period)
			}
		}
		def stop = { 
			running = false
			Plotting.stopPlotting
			Director.logger.info("done directing")
			Policy.logger.info("done directing")
			stateHistory.stopUpdating
			writeCostsToSummaryLog
			costFunction.dumpToDB
		}
	}

	private def writeCostsToSummaryLog {
		var costString = costFunction.toString
		Director.summaryLogger.info("COST SUMMARY: "+costFunction.toShortString)
		Director.summaryLogger.info("PERFORMANCE STATS:\n"+costFunction.performanceStats)
		Director.summaryLogger.info("COST DETAILS:\n"+costString)
	}
	def writeMovementSummary { // assumes director already got placement log
		import scala.io.Source
		val action_stats = scala.collection.mutable.Map[String,scala.collection.mutable.ListBuffer[Int]]()
		List("copy","move","remove").foreach((action)=> action_stats(action)=new scala.collection.mutable.ListBuffer[Int]())
		try {
			val f = Source.fromFile(Director.basedir+"/placement.txt")
			val lines = f.getLines
			lines.foreach((line)=>{
				val regex = "2.*server - "
				val features = line.replace("\n","").replaceAll(regex,"").split(",")
				val m = scala.collection.mutable.Map[String,String]()
				features.foreach((pair)=>{val split = pair.split("=");m += (split(0)->split(1))})
				action_stats(m("method")) += (m("end").replace("'","").toInt-m("start").replace("'","").toInt)
			})
			val action_statsList = Map[String,List[Int]](action_stats.toList map {entry=>(entry._1,entry._2.toList.sort(_<_))}:_*)

			Director.summaryLogger.info("\nMOVEMENT DETAILS:\n(num actions, total keys, min move, avg move, median move, max move)")
			action_statsList.toList.foreach((entry)=>{
				val total = entry._2.length
				val sum = entry._2.foldLeft(0) {(out,num)=>out+num}
				Director.summaryLogger.info(entry._1+": "+total+","+sum+","+entry._2(0)+","+ (sum/total)+","+entry._2(total/2) +","+entry._2(total-1))
			})
		} catch {case e => Director.summaryLogger.warn("Exception getting movement data from placement.") }
	}

	private def setDeployment(deploy_name:String) {
		Director.logger.info("Loading SCADS state")
		myscads = ScadsLoader.loadState(deploy_name)
		serverManager = new ScadsServerManager(deploy_name, myscads.deployMonitoring, Director.namespace)
		placementIP = myscads.placement.get(0).privateDnsName
		actionExecutor.setPlacement(placementIP) // tell action executor about placement
		
		// figure out which scads servers are registered with data placement, and which ones are standbys
		val dpentries = ScadsDeploy.getDataPlacementHandle(myscads.placement.get(0).privateDnsName,Director.xtrace_on).lookup_namespace(Director.namespace)

		// determine all servers
		val allservers = new scala.collection.mutable.ListBuffer[String]()
		val iter = myscads.servers.iterator
		while (iter.hasNext) { allservers += iter.next.privateDnsName }

		// determine inplay servers
		val inplay = new scala.collection.mutable.ListBuffer[String]()
		val iter2 = dpentries.iterator
		while (iter2.hasNext) { inplay += iter2.next.node }

		// set up standbys: scads servers alive but not currently responsible for any data
		serverManager.standbys.insertAll(0, (allservers.toList -- inplay.toList) )
	}

	def direct() {
		// check for  scads deployment instance
		if (myscads == null) { println("Need scads deployment before directing"); return }
		putRestrictionURL = placementIP +"/"+ScadsDeploy.restrictFileName
		val dpclient = ScadsDeploy.getDataPlacementHandle(placementIP,Director.xtrace_on)
		assert( dpclient.lookup_namespace(Director.namespace).size > 0, "Placement server has no storage nodes registered" )
		Director.logger.info("Will be directing with placement host: "+placementIP)
		
		directorRunner = new Runner(policy,costFunction,placementIP)
		val runthread = new Thread(directorRunner)
		runthread.start
	}

	def stop = directorRunner.stop

	def uploadLogsToS3 {
		Director.dumpAndDropDatabases
		val io = Director.exec( "s3cmd -P sync "+Director.basedir+" s3://scads-experiments/"+Director.startDate+"_"+experimentName+"/" )
		Director.logger.debug("executed s3cmd sync. stdout:"+io._1+"\n"+"stderr:"+io._2)
	}
}

object Director {
	var director:Director = null
	
	var LOG_ACTIONS = true
	
	val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	val logPattern = "%d %5p %c - %m%n"
	var startDate = dateFormat.format(new Date)
	var basedir = "/tmp/director/logs_"+startDate+"/"
	
	val xtrace_on = true
	val namespace = "perfTest256"
	var putRestrictionURL:String = null

	val databaseHost = "localhost"
	val databaseUser = "root"
//	val databaseUser = "director"
	val databasePassword = ""

	private var rnd = new java.util.Random(7)
	
	val delay = 20*1000
	
	var logger:Logger = null
	var summaryLogger:Logger = null
	
	initialize("")
	
	Director.exec("rm -f "+Director.basedir+"../current")
	Director.exec("ln -s "+Director.basedir+" "+Director.basedir+"../current")

	var lowLevelActionMonitor = LowLevelActionMonitor("director","lowlevel_actions")

	def initialize(experimentName:String) {
		startDate = dateFormat.format(new Date)
		basedir = "/tmp/director/logs_"+startDate+"_"+experimentName+"/"

		logger = Logger.getLogger("scads.director.director")
		val logPath = Director.basedir+"/director.txt"
		logger.removeAllAppenders
		logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
		logger.setLevel(DEBUG)

		summaryLogger = Logger.getLogger("scads.director.summary")
		summaryLogger.removeAllAppenders
		summaryLogger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),Director.basedir+"/summary.txt",false) )
		summaryLogger.setLevel(DEBUG)

		Logger.getRootLogger.removeAllAppenders
		Logger.getRootLogger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),Director.basedir+"/all.txt",false) )		
	}

	def resetRnd(seed:Int) { 
		//Director.logger.debug("RND: resetting using seed="+seed)
		rnd = new java.util.Random(seed) 
	}
	
	def nextRndInt(n:Int):Int = {
		val i = rnd.nextInt(n)
		//Director.logger.debug("RND: nextInt = "+i)
		i
	}
	
	def nextRndDouble():Double = {
		val x = rnd.nextDouble()
		//Director.logger.debug("RND: nextDouble = "+x)
		x
	}

	def exec(cmd:String):(String,String) = {
		val proc = Runtime.getRuntime.exec( Array("sh","-c",cmd) )
		Director.logger.debug("executing "+cmd)
		val err = scala.io.Source.fromInputStream( proc.getErrorStream ).getLines.mkString("")
		(scala.io.Source.fromInputStream( proc.getInputStream ).getLines.mkString(""),err)
	}

	def dumpAndDropDatabases() {
		// dump old databases
		val io = Director.exec("mysqldump --databases director metrics > "+Director.basedir+"/dbdump_"+dateFormat.format(new Date)+".sql")
		Director.logger.debug("dumped mysql databases. stdout:"+io._1+"\n"+"stderr:"+io._2)
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

	def connectToDatabase():Connection = connectToDatabase(databaseHost)
	
	def connectToDatabase(_dbhost:String):Connection = {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance()
        } catch { case ex: Exception => ex.printStackTrace() }

		var connection:Connection = null
        try {
            val connectionString = "jdbc:mysql://" + _dbhost + "/?user=" + databaseUser + "&password=" + databasePassword
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

	def startRserve {
		Director.exec("killall -9 Rserve")
		val io = Director.exec("R CMD Rserve --no-save --RS-workdir /opt/scads/experiments/ > /mnt/monitoring/rserve.log 2>&1")
		Director.logger.debug("started Rserve. stdout:"+io._1+"\n"+"stderr:"+io._2)
	}

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

	def createWorkloadHistograms(basedir:String) {
		import performance.WorkloadGenerators._
		val w = stdWorkloadEbatesWMixChange(mix99,mix99,1500,10000)
		WorkloadHistogram.createAndSerializeHistograms(w, 20*1000, basedir+"/ebates_mix99_mix99_1500users_200bins_20sec.hist", 10, 200, 10000)
	}
}