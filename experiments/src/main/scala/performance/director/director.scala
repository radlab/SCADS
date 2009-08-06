package scads.director

import performance._
import org.apache.log4j._
import org.apache.log4j.Level._
import java.text.SimpleDateFormat
import java.util.Date

object Director {
	val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	val logPattern = "%d %5p %c - %m%n"
	val basedir = "/mnt/director/logs_"+dateFormat.format(new Date)+"/"
	
	val databaseHost = "localhost"
	val xtrace_on = true
	val namespace = "perfTest256"
	var myscads:Scads = null
	var directorRunner:Runner = null
	var serverManager:ScadsServerManager = null
	
	val delay = 20
	
	val logger = Logger.getLogger("scads.director.director")
	private val logPath = Director.basedir+"/director.txt"
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
	logger.setLevel(DEBUG)

	case class Runner(policy:Policy,placementIP: String) extends Runnable {
		val metricReader = new MetricReader(databaseHost,"metrics",20,0.02)		
		var actions = List[Action]()
		
		var running = true
		def run = {
			while (running) {
				val state = SCADSState.refresh(metricReader, placementIP)
				logger.info("FRESH STATE: \n"+state.toShortString)

				val newActions = policy.act(state,actions)
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
		assert( dpclient.lookup_namespace(namespace).size == 1, "Placement server has no storage nodes registered" )
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
}