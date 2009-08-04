package scads.director

import org.apache.log4j._
import org.apache.log4j.Level._
import java.text.SimpleDateFormat
import java.util.Date

object Director {
	val dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	val logPattern = "%d %5p %c - %m%n"
	val basedir = "/mnt/director/logs_"+dateFormat.format(new Date)+"/"
	
	val databaseHost = "localhost"
	
	val delay = 20
	
	val logger = Logger.getLogger("scads.director.director")
	private val logPath = Director.basedir+"/director.txt"
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
	logger.setLevel(DEBUG)

	def direct(policy:Policy, placementIP:String) {		
		val metricReader = new MetricReader(databaseHost,"metrics",20,0.02)		
		var actions = List[Action]()
		
		while (true) {
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
}