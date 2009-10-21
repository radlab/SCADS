package scads.director

import scads.deployment.{ScadsDeploy,ScadsLoader}
import edu.berkeley.cs.scads.thrift.{RangeConversion, DataPlacement}
import edu.berkeley.cs.scads.keys._

import org.apache.log4j._
import org.apache.log4j.Level._
import java.util.Date

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

import java.io._

// TODO:
// - check for failures
// - check state
// - timeouts?


case class ActionExecutor {
	var actions = List[Action]()
	var placementIP:String = null

	def addAction(action:Action) {
		actions+=action 
		Policy.logger.info("adding action: "+action)
	}
	def setPlacement(ip:String) = { placementIP=ip }
	def getConfigFromPlacement:Map[String,DirectorKeyRange] = {
		if (placementIP==null)
			null
		else {
			assert(placementIP !=null, "action executor need to have placement ip set")
			var nodeConfig = Map[String,DirectorKeyRange]()

			val dp = ScadsDeploy.getDataPlacementHandle(placementIP,Director.xtrace_on)
			val placements = dp.lookup_namespace(Director.namespace)
			val iter = placements.iterator
			while (iter.hasNext) {
				val info = iter.next
				val range = new DirectorKeyRange(
					ScadsDeploy.getNumericKey( StringKey.deserialize_toString(info.rset.range.start_key,new java.text.ParsePosition(0)) ),
					ScadsDeploy.getNumericKey( StringKey.deserialize_toString(info.rset.range.end_key,new java.text.ParsePosition(0)) )
				)
				nodeConfig = nodeConfig.update(info.node, range)
			}
			nodeConfig
		}
	}
	def execute() {
		// start executing actions with all parents completed
		actions.filter(a=>a.parentsCompleted&&a.ready).foreach(_.startExecuting)
		// keep only actions that didn't complete yet (or didn't start)
		actions = actions.filter(!_.completed)
	}
	
	def simulateExecution(time0:Long, time1:Long, config:SCADSconfig): Tuple2[SCADSconfig,SCADSActivity] = {
		Director.logger.info("ActionExecutor.simulateExecution: config at the beginning ("+time0+" -> "+time1+")\n"+config.toString)
		Director.logger.info("all actions: \n"+actions.map(_.toString).mkString("\n"))
		
		// start simulating actions with all parents completed
		actions.filter(a=>a.parentsCompleted&&a.ready).foreach(_.startSimulation(time0,config))
		
		// simulate running actions
		var (newConfig,newActivity) = actions.filter(_.running).
										foldRight((config,SCADSActivity()))( (a,ca) => {Director.logger.debug("updatingConfigAndActivity for "+a)
																						val n=a.updateConfigAndActivity(time0,time1,ca._1,ca._2); 
																						Director.logger.debug(n._1); 
																						n} )
			
		// stop simulation of completed actions
		actions.filter(_.completed).foreach(_.stopSimulation(time0))

		// keep only actions that didn't complete yet (or didn't start)
		actions = actions.filter(!_.completed)
		
		(newConfig,newActivity)
	}
	
	def simulateExecutionWithoutEffects(time0:Long, time1:Long, config:SCADSconfig): Tuple2[SCADSconfig,SCADSActivity] = {
		var newConfig = config
		if (actions!=null)
			for (action <- actions) { 
				newConfig = action.preview(newConfig)
				action.complete
				action.startTime=time0
				action.endTime=time1
				Action.store(action)
			}
		actions = List[Action]()		
		(newConfig,SCADSActivity())
	}
	
	def allActionsCompleted():Boolean = if (actions.size==0) true else actions.forall(_.completed)
	def status:String = actions.map(a=> "%-10s".format("("+a.state+")") + "  " + a.toString).mkString("\n")
}


abstract class Action(
	val actionShortName:String
) extends Runnable {
	object ActionState extends Enumeration("Ready","Running","Completed") {
	  type ActionState = Value
	  val Ready, Running, Completed = Value
	}	
	import ActionState._

	val logger = Logger.getLogger("scads.director.action."+actionShortName)
	private val logPath = Director.basedir+"/actions/"+Director.dateFormat.format(new Date)+"_"+actionShortName+".txt"
	if (Director.LOG_ACTIONS) {
		logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
		logger.setLevel(DEBUG)
	} else logger.setLevel(OFF)
	
	val sleepTime = 0 // how long to sleep after performing an action, simulating clients' ttl on mapping
	var createsConsistentConfig = true // does this action depend on any others in order to be consistent
	//var initTime: Long = Director.director.policy.currentInterval
	var initTime: Long = Action.currentInitTime
	var startTime: Long = -1
	var endTime: Long = -1
		
	var _state = ActionState.Ready
	def state():ActionState = _state
	def completed():Boolean = state()==ActionState.Completed
	def running():Boolean = state()==ActionState.Running
	def ready():Boolean = state()==ActionState.Ready
	def complete { _state=ActionState.Completed }
	
	var executionThread: Thread = null
	
	var dbID: Int = -1
	
	val parents = new scala.collection.mutable.ListBuffer[Action]()
		
	def addParent(action:Action) { parents += action }
	def addParents(actions:List[Action]) { parents.insertAll(parents.size,actions) }
	def parentsCompleted():Boolean = if (parents.size==0) true else parents.forall(_.completed)
		
	def startExecuting() {
		logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
		dbID = Action.store(this)
		executionThread = new Thread(this)
		executionThread.start
	}
	
	def startSimulation(time:Long, config:SCADSconfig) {
		initSimulation(config)
		_state = ActionState.Running	
		startTime = time
	}
	
	def stopSimulation(time:Long) {
		endTime = time;
		Action.store(this);
		complete; 
	}
	def initSimulation(config:SCADSconfig) = {}
	def updateConfigAndActivity(time0:Long, time1:Long, config:SCADSconfig, activity:SCADSActivity):Tuple2[SCADSconfig,SCADSActivity] = { this.complete; (this.preview(config),activity) }
	
	def csvArgs():String = ""
	
	override def run() {
		_state = ActionState.Running
		logger.info("starting action execution")
		Policy.logger.info("starting execution of "+toString)
		startTime = new Date().getTime
		Action.store(this)
		execute()
		_state = ActionState.Completed
		logger.info("action execution completed")
		Policy.logger.info("done executing "+toString)
		endTime = new Date().getTime
		Action.store(this)
	}
	
	def execute()
	def preview(config:SCADSconfig): SCADSconfig = config
	def participants: Set[String]
	def toString(): String
}

abstract class ActionSelector {
	def getRandomAction(state: SCADSState):Action
}

class UniformSelector(choices:List[String]) extends ActionSelector {
	var mychoices = choices
	val replica_limit = 4
	def getRandomAction(state:SCADSState):Action = {
		var nodes = state.config.getNodes 		// inspect config to see what servers are available to take action on
		val node1:String = nodes.apply(Director.nextRndInt(nodes.size))
		nodes = nodes.remove((elem:String) => elem == node1 )
		val node2:String = if (nodes.size > 0) { nodes.apply(Director.nextRndInt(nodes.size)) }
							else { mychoices = mychoices.remove((elem:String) => elem == "MergeTwo" ); null} // need >1 node for merge

		val choice = mychoices.apply(Director.nextRndInt(mychoices.size)) // choose uniformly at rondom amongst possible choices
		choice match {
			case "SplitInTwo" => SplitInTwo(node1,-1)
			case "MergeTwo" => MergeTwo(node1,node2)
			case "Replicate" => Replicate(node1,Director.nextRndInt(replica_limit)+1)
			case _ => null
		}
	}
}

object Action {
	val dbname = "director"
	val dbtable = "actions"
	
	var currentInitTime = -1L
	
	var connection = Director.connectToDatabase
	initDatabase
	
	def initDatabase() {
        // create database if it doesn't exist and select it
        try {
            val statement = connection.createStatement
/*			statement.executeUpdate("DROP DATABASE IF EXISTS "+dbname)*/
            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbname)
            statement.executeUpdate("USE " + dbname)
/*			statement.executeUpdate("DROP TABLE IF EXISTS "+dbtable)*/
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS "+dbtable+" (`id` INT NOT NULL AUTO_INCREMENT, `update_time` BIGINT, `action_name` VARCHAR(30),"+
																			"`init_time` BIGINT, `start_time` BIGINT, `end_time` BIGINT, `status` VARCHAR(50),"+
																			"`short_name` VARCHAR(100), `args` VARCHAR(200), PRIMARY KEY(`id`) ) ")
			statement.close
       	} catch { case ex: SQLException => ex.printStackTrace() }

        println("initialized Action table")
    }

	def store(action:Action): Int = {
		if (connection==null) connection = Director.connectToDatabase
		val statement = connection.createStatement
		
		val cols = Map("update_time"-> (new Date).getTime.toString,
					   "action_name"-> action.getClass.toString.split('.').last,
					   "init_time"-> (if (action.initTime== -1) "null" else action.initTime.toString),
					   "start_time"-> (if (action.startTime== -1) "null" else action.startTime.toString),
					   "end_time"-> (if (action.endTime== -1) "null" else action.endTime.toString),
					   "status"-> action.state.toString,
					   "short_name"-> action.actionShortName,
					   "args"-> action.csvArgs).transform( (x,y) => if (y=="null") "null" else ("'"+y+"'") )

		var primaryKey = action.dbID
		if (primaryKey== -1) {
			// insert new action
			try {
				val colnames = cols.keySet.toList
				val sql = "INSERT INTO "+dbtable+" ("+colnames.mkString("`","`,`","`")+") values ("+colnames.map(cols(_)).mkString(",")+")"
				action.logger.debug("storing action: "+sql)
				statement.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS)
			
				// extract primary key of the action
				val rs = statement.getGeneratedKeys();
	            rs.first();
	            primaryKey = rs.getInt(1);
				statement.close
			} catch { case ex:Exception => action.logger.warn("exception when storing action",ex) }
			finally { statement.close }
			
		} else {
			// update existing action
			try {
				val sql = "UPDATE "+dbtable+" SET "+cols.map( v => "`"+v._1+"`="+v._2 ).mkString(",")+" WHERE id='"+action.dbID+"'"
				action.logger.debug("updating action: "+sql)
				statement.executeUpdate(sql)
			} catch { case ex:Exception => action.logger.warn("exception when updating action",ex) }
			finally { statement.close }
		}
		primaryKey
	}
	
	def timeOverlap(ta0:Long, ta1:Long, tb0:Long, tb1:Long):Boolean = if (ta1<=tb0||ta0>=tb1) false else true
}


class TestAction(
	val delay: Int
) extends Action("TestAction("+delay+")") {
	override def execute() {
		logger.debug("falling asleep")
		Thread.sleep( delay )
		logger.debug("waking up")
	}
	def participants = Set[String]()
	override def toString():String = actionShortName
	override def csvArgs():String = "delay="+delay
}

case class SplitInTwo(
	val server: String,
	val pivot:Int // split pivot
) extends Action("splitintwo("+server+","+pivot+")") with PlacementManipulation {

	override def execute() {
		logger.debug("Getting new storage server")
		val new_guys = Director.director.serverManager.getServers(1)
		if (new_guys.isEmpty) { logger.warn("Split failed: no available servers"); return }
		val new_guy = new_guys(0)

		// determine current range and split-point to give new server
		val bounds = getNodeRange(server,this)
		val start = bounds._1
		val end = bounds._2
		val middle = if (pivot >0) {pivot} else {((end-start)/2) + start}
		Thread.sleep(5*1000)
		
		// do the move and update local list of servers
		logger.info("Moving "+middle+" - "+end+" from "+server+" to "+ new_guy)
		move(server,new_guy,middle,end,this)
		logger.debug("Sleeping")
		Thread.sleep(sleepTime) // wait
	}
	
	var timeToBootup:Long = -1
	var timeToMoveData:Long = -1

	override def initSimulation(config:SCADSconfig) {
		logger.info("initializing simulation of SplitInTwo")
		val bounds = config.storageNodes(server)
		val start = bounds.minKey
		val end = bounds.maxKey
		val middle = if (pivot >0) {pivot} else {((end-start)/2) + start}
		
		timeToBootup = ActionModels.machineBootupTimeModel.sample
		timeToMoveData = ActionModels.copyDurationModel.sample( end - middle )
		logger.debug("timeToBootup = "+timeToBootup)
		logger.debug("timeToMoveData = "+timeToMoveData)
	}
	
	override def updateConfigAndActivity(time0:Long, time1:Long, config:SCADSconfig, activity:SCADSActivity):Tuple2[SCADSconfig,SCADSActivity] = {
		val (dt0,dt1) = (time0 - startTime, time1 - startTime)
		val (movet0,movet1) = (timeToBootup,timeToBootup+timeToMoveData)
		var (newConfig,newActivity) = (config,activity)
		
		logger.info("simulating "+dt0+" -> "+dt1+"  ("+time0+" -> "+time1+")")

		if (dt1 <= timeToBootup) logger.debug("waiting for maching to boot up, doing nothing")	// machine booting up, do nothing
		else if ( Action.timeOverlap(dt0,dt1,movet0,movet1) ) {
			// move interval overlaps with simulation interval, simulate activity
			logger.debug("moving data, setting activity on server "+server)
			newActivity.copyRate += server -> 1.0
		}
		
		if (dt1 > timeToBootup + timeToMoveData) {
			logger.debug("action completed, updating configuration")
			newConfig = preview(config)
			this.complete
		}
		(newConfig,newActivity)
	}
	
	override def preview(config:SCADSconfig): SCADSconfig = {
		val bounds = config.storageNodes(server)
		var nodeConfig = config.storageNodes - server // make copy of previous node arrangment, removing the obsolete entry

		val start = bounds.minKey
		val end = bounds.maxKey
		val middle = if (pivot >0) {pivot} else {((end-start)/2) + start}
		nodeConfig = nodeConfig.update(server, new DirectorKeyRange(start,middle))
		nodeConfig = nodeConfig.update(SCADSconfig.getRandomServerNames(config,1).first, new DirectorKeyRange(middle,end))
		config.updateNodes(nodeConfig)
	}
	def participants = Set[String](server)
	override def toString:String = actionShortName
}

case class SplitFrom(
	val server:String,
	val range:DirectorKeyRange
) extends Action("splitfrom("+server+","+range+")") with PlacementManipulation {

	override def execute() {
		logger.debug("Getting new storage server")
		val new_guys = Director.director.serverManager.getServers(1)
		if (new_guys.isEmpty) { logger.warn("Split failed: no available servers"); return }
		val new_guy = new_guys(0)

		// determine current range and split-point to give new server
		//val bounds = getNodeRange(server)
		val start = range.minKey
		val end = range.maxKey

		// do the move and update local list of servers
		logger.info("Moving "+start+" - "+end+" from "+server+" to "+ new_guy)
		move(server,new_guy,start,end,this)
		logger.debug("Sleeping")
		Thread.sleep(sleepTime) // wait
	}
	override def preview(config:SCADSconfig):SCADSconfig = {
		val bounds = config.storageNodes(server)
		var nodeConfig = config.storageNodes - server // make copy of previous node arrangment, removing the obsolete entry

		val start = range.minKey
		val end = range.maxKey
		assert(start==bounds.minKey || end==bounds.maxKey,"Can only move data from either end of server's range")
		val old_start = if (start==bounds.minKey) { end } else { bounds.minKey }
		val old_end = if (start==bounds.minKey) { bounds.maxKey } else { start }

		nodeConfig = nodeConfig.update(server, new DirectorKeyRange(old_start,old_end))
		nodeConfig = nodeConfig.update(SCADSconfig.getRandomServerNames(config,1).first, new DirectorKeyRange(start,end))
		config.updateNodes(nodeConfig)
	}
	def participants = Set[String](server)
	override def toString:String = actionShortName
}

case class ShiftBoundary(
	val server_left:String,
	val server_right:String,
	val boundary_new:Int
) extends Action("shiftboundary("+server_left+","+server_right+","+boundary_new+")") with PlacementManipulation {
	override def execute() {
		val right_bounds = getNodeRange(server_right,this)

		// figure out if left <- right or left -> right
		if (right_bounds._1 < boundary_new) {
			logger.info("Moving "+right_bounds._1+" - "+boundary_new+" from "+server_right+" to "+ server_left)
			move(server_right,server_left,right_bounds._1,boundary_new,this)
		}
		else if (boundary_new < right_bounds._1) {
			logger.info("Moving "+boundary_new+" - "+right_bounds._1+" from "+server_left+" to "+ server_right)
			move(server_left,server_right,boundary_new,right_bounds._1,this)
		}
		else logger.warn("new boundary isn't different than current one! taking no action")
		logger.debug("Sleeping")
		Thread.sleep(sleepTime) // wait
	}
	override def preview(config:SCADSconfig): SCADSconfig = {
		val right_bounds = config.storageNodes(server_right)
		val left_bounds = config.storageNodes(server_left)
		var nodeConfig = (config.storageNodes - server_left) - server_right

		nodeConfig = nodeConfig.update(server_right, new DirectorKeyRange(boundary_new,right_bounds.maxKey))
		nodeConfig = nodeConfig.update(server_left, new DirectorKeyRange(left_bounds.minKey,boundary_new))

		config.updateNodes(nodeConfig)
	}
	def participants = Set[String](server_left,server_right)
	override def toString:String = actionShortName
}

case class MergeTwo(
	val server1: String,
	val server2: String
) extends Action("mergetwo("+server1+","+server2+")") with PlacementManipulation {
	var num_keys:Int = 0
	override def execute() {
		val bounds = getNodeRange(server1,this)
		val start = bounds._1
		val end = bounds._2
		num_keys = end-start
		logger.info("Moving "+start+" - "+end+" from "+server1+" to "+ server2)
		copy(server1,server2,start,end,this) // do copy instead of move to avoid sync problems?
		logger.debug("Removing from placement: "+ server1)
		val removing = server1
		remove(removing,this)

		val overflow = SCADSconfig.returnStandbys(List[String](server1))
		if (overflow.isEmpty) { logger.debug("Returned "+ server1+" to standby pool"); }
		else { logger.debug("Releasing server "+ server1); Director.director.serverManager.releaseServer(removing) }
		logger.debug("Sleeping")
		Thread.sleep(sleepTime) // wait
	}
	
	var timeToMoveData:Long = -1
	var removeTime:Long = -1
	
	override def initSimulation(config:SCADSconfig) {
		logger.info("initializing simulation of MergeTwo")		
		val bounds1 = config.storageNodes(server1)
		removeTime = ActionModels.removeDurationModel.sample( bounds1.maxKey - bounds1.minKey )
		timeToMoveData = ActionModels.copyDurationModel.sample( bounds1.maxKey - bounds1.minKey ) + removeTime
		logger.debug("timeToMoveData = "+timeToMoveData)
	}
	override def csvArgs():String = "num_keys="+num_keys
	override def updateConfigAndActivity(time0:Long, time1:Long, config:SCADSconfig, activity:SCADSActivity):Tuple2[SCADSconfig,SCADSActivity] = {
		val (dt0,dt1) = (time0 - startTime, time1 - startTime)
		val (movet0,movet1) = (0,timeToMoveData)
		var (newConfig,newActivity) = (config,activity)

		logger.info("simulating "+dt0+" -> "+dt1+"  ("+time0+" -> "+time1+")")

		if ( Action.timeOverlap(dt0,dt1,movet0,movet1) ) {
			// move interval overlaps with simulation interval, simulate activity
			if (dt1 > (timeToMoveData-removeTime)) { // finished copying, doing removing
				logger.debug("removing data, setting activity on server "+server1)
				newActivity.copyRate += server1 -> 1.0
			}
			else {
				logger.debug("moving data, setting activity on servers "+server1+", "+server2)
				newActivity.copyRate += server1 -> 1.0
				newActivity.copyRate += server2 -> 1.0
			}
		}
		
		if (dt1 > timeToMoveData) {
			logger.debug("simulation completed, updating configuration")
			/*val new_standbys = if (config.standbys.size < SCADSconfig.standbyMaxPoolSize)
					{ logger.debug("Returned "+ server1+" to standby pool"); config.standbys ::: List(server1) }
				else {config.standbys}*/
			newConfig = preview(config)//.updateStandbys(new_standbys)
			this.complete
		}
		(newConfig,newActivity)
	}
	
	override def preview(config:SCADSconfig): SCADSconfig = {
		val bounds1 = config.storageNodes(server1)
		val bounds2 = config.storageNodes(server2)
		var nodeConfig = (config.storageNodes - server1) - server2 // copy of config, without two obsolete entries

		val start = Math.min(bounds1.minKey,bounds2.minKey)
		val end = Math.max(bounds1.maxKey, bounds2.maxKey)
		nodeConfig = nodeConfig.update(server2, new DirectorKeyRange(start,end))
		config.updateNodes(nodeConfig)
	}
	def participants = Set[String](server1,server2)
	override def toString:String = actionShortName
}

case class Replicate(
	val server: String,
	val num: Int // how many additional replicas to make
) extends Action("replicate("+server+","+num+")") with PlacementManipulation {

	override def execute() {
		logger.debug("Getting "+num+" new storage server(s)")
		val new_guys = Director.director.serverManager.getServers(num)
		if (new_guys.isEmpty) { logger.warn("Replication failed: no available servers"); return }

		// determine current range to give new servers
		val bounds = getNodeRange(server,this)
		val start = bounds._1
		val end = bounds._2
		Thread.sleep(5*1000)

		// do the copy and update local list of servers (serially)
		new_guys.foreach((new_guy)=> {
			logger.info("Copying "+start+" - "+end+" from "+server+" to "+ new_guy)
			copy(server,new_guy,start,end,this)
		})
		logger.debug("Sleeping")
		Thread.sleep(sleepTime) // wait
	}
	override def preview(config:SCADSconfig): SCADSconfig = {
		val bounds = config.storageNodes(server)
		var nodeConfig = config.storageNodes

		val start = bounds.minKey
		val end = bounds.maxKey
		val newNames = SCADSconfig.getRandomServerNames(config,num)
		newNames.foreach((name)=> {
			nodeConfig = nodeConfig.update(name, new DirectorKeyRange(start,end))
		})
		config.updateNodes(nodeConfig)
	}
	def participants = Set[String](server)
	override def toString:String = actionShortName
}

/**
* Get num new servers, then copy to them the specified range from the given server
*/
case class ReplicateFrom(
	val server:String,
	val range:DirectorKeyRange,
	val num:Int
) extends Action("replicatefrom("+server+","+range+","+num+")") with PlacementManipulation {
	var timeToBootup:Long = -1
	var timeToMoveData:Long = -1
	var started = false // has action started simulation

	override def execute() {
		val start = range.minKey
		val end = range.maxKey

		// grab from standby pool
		val standbys = SCADSconfig.getStandbys(num)
		val remaining = num-standbys.size
		logger.debug("Got "+(num-remaining)+" servers from the standby pool")
		// boot remaining needed servers
		var new_guys = List[String]()
		if (remaining > 0) {
			logger.debug("Getting "+remaining+" new storage server(s)")
			new_guys = Director.director.serverManager.getServers(remaining)
			if (new_guys.isEmpty) { logger.warn("Replication failed: no available servers"); return }
			logger.debug("Waiting to boot up")
			Thread.sleep(ActionModels.machineBootupTimeModel.sample) // sleep while "booting up"
		}

		// do the copy and update local list of servers (serially)
		(standbys++new_guys).foreach((new_guy)=> {
			logger.info("Copying "+start+" - "+end+" from "+server+" to "+ new_guy)
			copy(server,new_guy,start,end,this)
		})
		logger.debug("Sleeping")
		Thread.sleep(sleepTime) // wait
	}
	override def preview(config:SCADSconfig):SCADSconfig = {
		val bounds = config.storageNodes(server)
		var nodeConfig = config.storageNodes// - server

		val start = range.minKey
		val end = range.maxKey
		//assert(start==bounds.minKey || end==bounds.maxKey,"Can only copy data from either end of server's range")
		//val old_start = if (start==bounds.minKey) { end } else { bounds.minKey }
		//val old_end = if (start==bounds.minKey) { bounds.maxKey } else { start }

		// replicate range specified number of times on new servers
		//nodeConfig = nodeConfig.update(server, new DirectorKeyRange(old_start,old_end))
		val newNames = SCADSconfig.getRandomServerNames(config,num)
		newNames.foreach((name)=> {
			nodeConfig = nodeConfig.update(name, new DirectorKeyRange(start,end))
		})
		config.updateNodes(nodeConfig)
	}
	override def initSimulation(config:SCADSconfig) {
		logger.info("initializing simulation of ReplicateFrom")
		timeToBootup = ActionModels.machineBootupTimeModel.sample
		timeToMoveData = ActionModels.copyDurationModel.sample( range.maxKey-range.minKey )*num // copying one by one
		logger.debug("timeToBootup = "+timeToBootup)
		logger.debug("timeToMoveData = "+timeToMoveData)
	}
	override def updateConfigAndActivity(time0:Long, time1:Long, config:SCADSconfig, activity:SCADSActivity):Tuple2[SCADSconfig,SCADSActivity] = {
		val (dt0,dt1) = (time0 - startTime, time1 - startTime)
		var (newConfig,newActivity) = (config,activity)
		if (!started) { // try to grab standbys, updating timeToBootup if got everything from the standby pool
			val standbys = config.standbys.take(num); newConfig = newConfig.updateStandbys(config.standbys -- standbys)
			if (standbys.size >= num) { logger.debug("Got all needed servers from standby pool, removing bootup time"); timeToBootup = -1 }
			started = true
		}
		val (movet0,movet1) = (timeToBootup,timeToBootup+timeToMoveData)
		logger.info("simulating "+dt0+" -> "+dt1+"  ("+time0+" -> "+time1+")")

		if (dt1 <= timeToBootup) logger.debug("waiting for maching to boot up, doing nothing")	// machine booting up, do nothing
		else if ( Action.timeOverlap(dt0,dt1,movet0,movet1) ) {
			// move interval overlaps with simulation interval, simulate activity
			logger.debug("copying data, setting activity on server "+server)
			newActivity.copyRate += server -> 1.0
		}

		if (dt1 > timeToBootup + timeToMoveData) {
			logger.debug("action completed, updating configuration")
			newConfig = preview(config)
			this.complete
		}
		(newConfig,newActivity)
	}
	override def csvArgs():String = "num_keys="+(range.maxKey-range.minKey)
	def participants = Set[String](server)
	override def toString:String = actionShortName
}

case class Remove(
	val servers:List[String]
) extends Action("remove("+servers.mkString(",")+")") with PlacementManipulation {
	var timeToMoveData:Long = -1
	val num = servers.size
	override def execute() {
		// remove servers serially
		servers.foreach((server)=>{
			logger.debug("Removing from placement: "+ server)
			remove(server,this)
			logger.debug("Releasing server "+ server)
			Director.director.serverManager.releaseServer(server)
		})
		logger.debug("Sleeping")
		Thread.sleep(sleepTime) // wait
	}
	override def preview(config:SCADSconfig):SCADSconfig = {
		config.updateNodes(config.storageNodes -- servers)
	}
	override def initSimulation(config:SCADSconfig) {
		logger.info("initializing simulation of Remove")
		timeToMoveData = ActionModels.removeDurationModel.sample(-1) *num // remove serially
		logger.debug("timeToMoveData = "+timeToMoveData)
	}
	override def updateConfigAndActivity(time0:Long, time1:Long, config:SCADSconfig, activity:SCADSActivity):Tuple2[SCADSconfig,SCADSActivity] = {
		val (dt0,dt1) = (time0 - startTime, time1 - startTime)
		val (movet0,movet1) = (0,timeToMoveData)
		var (newConfig,newActivity) = (config,activity)

		logger.info("simulating "+dt0+" -> "+dt1+"  ("+time0+" -> "+time1+")")

		if ( Action.timeOverlap(dt0,dt1,movet0,movet1) ) {
			// move interval overlaps with simulation interval, simulate activity
			var server:String = null
			(0 until num).foreach((i)=> if (dt0 <= i*(timeToMoveData/num)) {server = servers(i)})
			if (server!=null) {
				logger.debug("removing serially, setting activity on server "+server)
				newActivity.copyRate += server -> 1.0
			} else logger.warn("couldn't update copyRate in Remove")
		}

		if (dt1 > timeToMoveData) {
			logger.debug("simulation completed, updating configuration")
			/*val new_standbys = if (config.standbys.size < SCADSconfig.standbyMaxPoolSize)
					{ logger.debug("Returned "+ server1+" to standby pool"); config.standbys ::: List(server1) }
				else {config.standbys}*/
			newConfig = preview(config)
			this.complete
		}
		(newConfig,newActivity)
	}
	def participants = Set[String](servers:_*)
	override def toString:String = actionShortName
}

case class RemoveFrom(
	val servers:List[String],
	val range:DirectorKeyRange
) extends Action("removefrom("+servers.mkString(",")+","+range+")") with PlacementManipulation {
	var timeToMoveData:Long = -1
	val num = servers.size
	override def execute() {
		val start = range.minKey
		val end = range.maxKey

		// do the removal (serially) and update local list of servers
		logger.info("Removing "+start+" - "+end+" from "+servers.mkString(","))
		servers.foreach((server)=> removeData(server,start,end,this) )
		logger.debug("Sleeping")
		Thread.sleep(sleepTime) // wait
	}
	override def preview(config:SCADSconfig):SCADSconfig = {
		val bounds = config.storageNodes(servers.first) // they should all be replicas
		var nodeConfig = config.storageNodes -- servers
		val start = range.minKey
		val end = range.maxKey

		assert(start==bounds.minKey || end==bounds.maxKey,"Can only remove data from either end of servers' range")
		val old_start = if (start==bounds.minKey) { end } else { bounds.minKey }
		val old_end = if (start==bounds.minKey) { bounds.maxKey } else { start }

		servers.foreach((name)=> {
			nodeConfig = nodeConfig.update(name, new DirectorKeyRange(old_start,old_end))
		})
		config.updateNodes(nodeConfig)
	}
	override def initSimulation(config:SCADSconfig) {
		logger.info("initializing simulation of RemoveData")
		timeToMoveData = ActionModels.removeDataDurationModel.sample( range.maxKey - range.minKey ) *num // remove serially
		logger.debug("timeToMoveData = "+timeToMoveData)
	}
	override def updateConfigAndActivity(time0:Long, time1:Long, config:SCADSconfig, activity:SCADSActivity):Tuple2[SCADSconfig,SCADSActivity] = {
		val (dt0,dt1) = (time0 - startTime, time1 - startTime)
		val (movet0,movet1) = (0,timeToMoveData)
		var (newConfig,newActivity) = (config,activity)

		logger.info("simulating "+dt0+" -> "+dt1+"  ("+time0+" -> "+time1+")")

		if ( Action.timeOverlap(dt0,dt1,movet0,movet1) ) {
			// move interval overlaps with simulation interval, simulate activity
			var server:String = null
			(0 until num).foreach((i)=> if (dt0 <= i*(timeToMoveData/num)) {server = servers(i)})
			if (server!=null) {
				logger.debug("removing data serially, setting activity on server "+server)
				newActivity.copyRate += server -> 1.0
			} else logger.warn("couldn't update copyRate in RemoveFrom")
		}

		if (dt1 > timeToMoveData) {
			logger.debug("simulation completed, updating configuration")
			newConfig = preview(config)
			this.complete
		}
		(newConfig,newActivity)
	}
	override def csvArgs():String = "num_keys="+(range.maxKey-range.minKey)
	def participants = Set[String](servers:_*)
	override def toString:String = actionShortName
}

case class ModifyPutRestrictions(
	val changes:Map[DirectorKeyRange,Double],
	val oldmap:Map[DirectorKeyRange,Double] // old mapping from the existing configuration
) extends Action("modifyputs("+changes.toList.mkString(",")+")") with PlacementManipulation {
		val loc = "/var/www/"+ScadsDeploy.restrictFileName // location of put restriction file

		override def execute() {
			val csv_string = getNewMapping.foldLeft("") {(out,entry)=>out + ScadsDeploy.keyFormat.format(entry._1.minKey)+","+ScadsDeploy.keyFormat.format(entry._1.maxKey)+","+entry._2+";"}
			 // write restriction file on placement machine
			try {
				Director.director.myscads.placement.get(0).exec("rm -f " +loc+";echo '"+csv_string+"' >> "+loc)
			} catch {case e: java.net.SocketException => {
				Director.director.myscads = ScadsLoader.loadState(Director.director.myscads.deploymentName) // refresh state, then try again
				Director.director.myscads.placement.get(0).exec("rm -f " +loc+";echo '"+csv_string+"' >> "+loc)
			}}
		}
		override def preview(config:SCADSconfig):SCADSconfig = {
			config.updateRestrictions(getNewMapping)
		}
		private def getNewMapping:Map[DirectorKeyRange,Double] = {
			// update existing values, remove 1.0 entries, keep unmodified entries
			val keys = Set[DirectorKeyRange]((changes.keys.collect++oldmap.keys.collect):_*)
			val new_restrictions = Map[DirectorKeyRange,Double]( keys.toList map {
					key=>(key -> changes.getOrElse(key,oldmap(key)))} : _*)
			new_restrictions.filter(_._2<1.0)
		}
		def participants = Set[String]()
		override def toString:String = actionShortName
}

trait PlacementManipulation extends RangeConversion with AutoKey {
	val xtrace_on = Director.xtrace_on
	val namespace = Director.namespace
	var placement_host:String = null

	private def init = {
		placement_host = Director.director.myscads.placement.get(0).privateDnsName
	}

	protected def getNodeRange(host:String, action:Action):(Int, Int) = {
		val t0 = new Date().getTime
		Policy.logger.info("low-level action start: getNodeRange(host="+host+")")
		
		if (placement_host == null) init
		val dp = ScadsDeploy.getDataPlacementHandle(placement_host,xtrace_on)
		val s_info = dp.lookup_node(namespace,host,ScadsDeploy.server_port,ScadsDeploy.server_sync)
		val range = s_info.rset.range
		val value = (ScadsDeploy.getNumericKey( StringKey.deserialize_toString(range.start_key,new java.text.ParsePosition(0)) ),
		ScadsDeploy.getNumericKey( StringKey.deserialize_toString(range.end_key,new java.text.ParsePosition(0)) ))
		
		val t1 = new Date().getTime
		Policy.logger.info("low-level action end: getNodeRange(host="+host+")")
		Director.lowLevelActionMonitor.log("getNodeRange",action,t0,t1,Map("host"->host))
		
		value
	}

	protected def move(source_host:String, target_host:String,startkey:Int, endkey:Int, action:Action) = {
		val t0 = new Date().getTime
		Policy.logger.info("low-level action start: move(source="+source_host+", target="+target_host+", startKey="+startkey+", endKey="+endkey+")")
		
		if (placement_host == null) init
		val dpclient = ScadsDeploy.getDataPlacementHandle(placement_host,xtrace_on)
		val range = new KeyRange(new StringKey(ScadsDeploy.keyFormat.format(startkey)), new StringKey(ScadsDeploy.keyFormat.format(endkey)) )
		dpclient.move(namespace,range, source_host, ScadsDeploy.server_port,ScadsDeploy.server_sync, target_host, ScadsDeploy.server_port,ScadsDeploy.server_sync)
		
		val t1 = new Date().getTime
		Policy.logger.info("low-level action done: move(source="+source_host+", target="+target_host+", startKey="+startkey+", endKey="+endkey+")")
		Director.lowLevelActionMonitor.log("move",action,t0,t1,Map("source_host"->source_host,"target_host"->target_host,"startkey"->startkey.toString,"endkey"->endkey.toString))
	}

	protected def copy(source_host:String, target_host:String,startkey:Int, endkey:Int, action:Action) = {
		val t0 = new Date().getTime
		Policy.logger.info("low-level action start: copy(source="+source_host+", target="+target_host+", startKey="+startkey+", endKey="+endkey+")")
		
		if (placement_host == null) init
		val dpclient = ScadsDeploy.getDataPlacementHandle(placement_host,xtrace_on)
		val range = new KeyRange(new StringKey(ScadsDeploy.keyFormat.format(startkey)), new StringKey(ScadsDeploy.keyFormat.format(endkey)) )
		dpclient.copy(namespace,range, source_host, ScadsDeploy.server_port,ScadsDeploy.server_sync, target_host, ScadsDeploy.server_port,ScadsDeploy.server_sync)
		
		val t1 = new Date().getTime
		Policy.logger.info("low-level action done: copy(source="+source_host+", target="+target_host+", startKey="+startkey+", endKey="+endkey+")")
		Director.lowLevelActionMonitor.log("copy",action,t0,t1,Map("source_host"->source_host,"target_host"->target_host,"startkey"->startkey.toString,"endkey"->endkey.toString))
	}
	
	protected def remove(host:String, action:Action) = {
		val t0 = new Date().getTime
		Policy.logger.info("low-level action start: remove(host="+host+")")
		
		if (placement_host == null) init
		val dpclient = ScadsDeploy.getDataPlacementHandle(placement_host,xtrace_on)
		val bounds = getNodeRange(host,action)
		val range = new KeyRange(new StringKey(ScadsDeploy.keyFormat.format(bounds._1)), new StringKey(ScadsDeploy.keyFormat.format(bounds._2)) )
		val list = new java.util.LinkedList[DataPlacement]()
		list.add(new DataPlacement(host,ScadsDeploy.server_port,ScadsDeploy.server_sync,range))
		dpclient.remove(namespace,list)
		
		val t1 = new Date().getTime
		Policy.logger.info("low-level action done: remove(host="+host+")")
		Director.lowLevelActionMonitor.log("remove",action,t0,t1,Map("host"->host))
	}

	protected def removeData(host:String,startKey:Int,endKey:Int, action:Action) = {
		val t0 = new Date().getTime
		Policy.logger.info("low-level action start: removeData(host="+host+", startKey="+startKey+", endKey="+endKey+")")
		
		if (placement_host == null) init
		val bounds = getNodeRange(host,action)
		assert(startKey==bounds._1 || endKey==bounds._2,"Can only remove data from either end of servers' range")
		val new_start = if (startKey==bounds._1) { endKey } else { bounds._1 }
		val new_end = if (startKey==bounds._1) { bounds._2 } else { startKey }

		val dpclient = ScadsDeploy.getDataPlacementHandle(placement_host,xtrace_on)
		val list = new java.util.LinkedList[DataPlacement]()

		// first remove the incorrect entry and data
		val range = new KeyRange(new StringKey(ScadsDeploy.keyFormat.format(startKey)), new StringKey(ScadsDeploy.keyFormat.format(endKey)) )
		list.add(new DataPlacement(host,ScadsDeploy.server_port,ScadsDeploy.server_sync,range))
		dpclient.remove(namespace,list)
		val tm = new Date().getTime

		// now add the correct range info
		val range_new = new KeyRange(new StringKey(ScadsDeploy.keyFormat.format(new_start)), new StringKey(ScadsDeploy.keyFormat.format(new_end)) )
		list.clear
		list.add(new DataPlacement(host,ScadsDeploy.server_port,ScadsDeploy.server_sync,range_new))
		dpclient.add(namespace,list)
		
		val t1 = new Date().getTime
		Policy.logger.info("low-level action done: removeData(host="+host+", startKey="+startKey+", endKey="+endKey+")")
		Director.lowLevelActionMonitor.log("removeData",action,t0,t1,Map("host"->host,"startkey"->startKey.toString,"endkey"->endKey.toString))
		Director.lowLevelActionMonitor.log("removeData_remove",action,t0,tm,Map("host"->host,"startkey"->startKey.toString,"endkey"->endKey.toString))
		Director.lowLevelActionMonitor.log("removeData_add",action,tm,t1,Map("host"->host,"startkey"->startKey.toString,"endkey"->endKey.toString))
	}
}


case class LowLevelActionMonitor(
	dbname:String,
	dbtable:String
) {

	var connection = Director.connectToDatabase
	initDatabase

	def initDatabase() {
		Director.logger.debug("initializing LowLevelAction database")
        try {
            val statement = connection.createStatement
            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbname)
            statement.executeUpdate("USE " + dbname)
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS "+dbtable+" (`id` INT NOT NULL AUTO_INCREMENT, `type` VARCHAR(30),"+
																			"`action` VARCHAR(30),"+
																			"`action_wargs` VARCHAR(130),"+
																			"`start_time` BIGINT, `end_time` BIGINT, "+
																			"`features` TEXT, PRIMARY KEY(`id`) ) ")
			statement.close
	        Director.logger.debug("initialized LowLevelAction table")
       	} catch { case ex: SQLException => Director.logger.warn("exception when initializing LowLevelAction table",ex) }
    }

	def log(lowlevelaction:String, action:Action, time0:Long, time1:Long, features:Map[String,String]) {
		try {
			val actionSQL = Director.createInsertStatement(dbtable, Map("type"->("'"+lowlevelaction+"'"),
																		"action"->("'"+action.getClass.toString.split('.').last+"'"),
																		"action_wargs"->("'"+action.toString+"'"),
																		"start_time"->time0.toString,
																		"end_time"->time1.toString,
																		"features"->("'"+features.map((p)=>p._1+"="+p._2).mkString(",")+"'") ))
			Director.logger.debug("sql = "+actionSQL)
	        val statement = connection.createStatement
			statement.executeUpdate(actionSQL)
			statement.close
		} catch {
			case e:Exception => Director.logger.warn("exception when logging low-level action",e)
		}
	}

}

object LowLevelActionMonitor {
	
	def loadLowLevelActions(dbhost:String, experiment:String):List[LowLevelActionStats] = {
		var connection = Director.connectToDatabase
		val statement = connection.createStatement
		statement.executeUpdate("use director")

		val actions = new scala.collection.mutable.ListBuffer[LowLevelActionStats]()
		try {
			val result = statement.executeQuery("select * from lowlevel_actions")
			while (result.next) {
				val action = result.getString("type")
				val t0 = result.getLong("start_time")
				val t1 = result.getLong("end_time")
				val featureString = result.getString("features")
				val features = Map( featureString.split(",").map( p => {val s=p.split("="); s(0)->s(1)}).toList :_* )
				val stats = LowLevelActionStats(action,t0,t1,features,experiment)				
				actions += stats
			}
       	} catch { case ex: SQLException => Director.logger.warn("SQL exception in metric reader",ex)}
		statement.close
		connection.close
		
		actions.toList
	}
	
	def extractTransientData() {
		val outputdir = "/Users/bodikp/Downloads/scads/"

		val cacheloc = "/tmp/experimentcache/"
		val baseloc = "http://scads-experiments.s3.amazonaws.com"
		val runs = List("2009-10-15-02-11-17_bodikp-keypair_ebates_pinchoff_1.0_1.0_0.2_1255571856690/dbdump_2009-10-15-03-12-28.sql",
						"2009-10-15-01-03-09_trush_ebates-spike_pinchoff_0.9_0.05_0.3_100k_1255567920501/dbdump_2009-10-15-02-04-25.sql",
						"2009-10-15-00-18-37_trush_ebates_pinchoff_0.9_0.05_0.3_100k_1255565005958/dbdump_2009-10-15-01-20-10.sql")
		
		val timeAdd = 5 * 60 * 1000
		var actionID = 0
		
		var effects = new java.io.FileWriter(outputdir+"/actioneffects.csv",false)
		effects.write("actionid,server,time,workload,latency_50p,latency_99p\n")

		// download and cache data
		for (run <- runs) {
			val path = run
			val dir = path.split("/")(0)
			println("checking "+run)
			if (!(new File(cacheloc+"/"+path)).exists) {
				println("  don't have file ... downloading")
				(new File(cacheloc+"/"+dir)).mkdirs
				Director.exec("wget "+baseloc+"/"+path+" -O "+cacheloc+"/"+path)
				//println(io._1)
				//println(io._2)
			}
		}

		// load low-level actions
		val allLowLevelActions = new scala.collection.mutable.ListBuffer[LowLevelActionStats]()
		for (run <- runs) {
			val path = run
			val experiment = path.split("/")(0)
			val io = Director.exec("/usr/local/mysql/bin/mysql -uroot < "+cacheloc+"/"+path)
			val actions = loadLowLevelActions("localhost",experiment)
			allLowLevelActions ++= actions
			
			val metricReader = MetricReader("localhost","metrics",1000,1.0)
			
			for (a <- actions) {
				a.setID(actionID)
				actionID += 1
				val t0 = a.time0 - timeAdd
				val t1 = a.time1 + timeAdd
				
				if (a.host1!="") {
					val workload = metricReader.getSingleMetric(a.host1,"workload","ALL",t0,t1)
					val latency50 = metricReader.getSingleMetric(a.host1,"latency_50p","ALL",t0,t1)
					val latency99 = metricReader.getSingleMetric(a.host1,"latency_99p","ALL",t0,t1)
					for (t <- workload.keys.toList.sort(_<_))
						effects.write(a.id+","+a.host1+","+t+","+workload(t)+","+latency50(t)+","+latency99(t)+"\n")
				}

				if (a.host2!="") {
					val workload = metricReader.getSingleMetric(a.host2,"workload","ALL",t0,t1)
					val latency50 = metricReader.getSingleMetric(a.host2,"latency_50p","ALL",t0,t1)
					val latency99 = metricReader.getSingleMetric(a.host2,"latency_99p","ALL",t0,t1)
					for (t <- workload.keys.toList.sort(_<_))
						effects.write(a.id+","+a.host2+","+t+","+workload(t)+","+latency50(t)+","+latency99(t)+"\n")
				}
			}
		}
		effects.close
		
		(new java.io.File(outputdir)).mkdirs()

		var out = new java.io.FileWriter(outputdir+"/lowlevelactions.csv",false)
		out.write("experiment,action,actionid,time,duration,nkeys,host1,host2\n") 
		for(a <- allLowLevelActions) out.write(a.experiment+","+a.action+","+a.id+","+a.time0+","+(a.time1-a.time0)+","+(try{a.features("endkey").toInt-a.features("startkey").toInt}catch{case _=>"NaN"})+
													","+a.host1+","+a.host2+"\n")
		out.close
		
	}
}

case class LowLevelActionStats(
	action:String,
	time0:Long,
	time1:Long,
	features:Map[String,String],
	experiment:String
) {
	var id = -1
	
	def setID(i:Int) { id = i }
	
	def host1:String = { 
		if (features.contains("source_host")) features("source_host")
		else if (features.contains("host")) features("host")
		else ""
	}

	def host2:String = { 
		if (features.contains("target_host")) features("target_host")
		else ""
	}
}


object ActionModels {
	var machineBootupTimeModel = ConstantMachineBootupTimeModel(1*60*1000)
	var copyDurationModel = LinearCopyDurationModel(0.1485)
	var removeDataDurationModel = LinearRemoveDataDurationModel(0.07145)
	//var removeDurationModel = LinearRemoveDurationModel(0.04844)
	var removeDurationModel = ConstantRemoveDurationModel(3780)
	var getNodeRangeDurationModel = ConstantGetNodeRangeDurationModel(6)
}

// duration of machine boot up
abstract class MachineBootupTimeModel {
	def sample(): Long
}
case class ConstantMachineBootupTimeModel(
	duration: Long
) extends MachineBootupTimeModel {
	def sample():Long = duration
}

// duration of placement "copy"
abstract class CopyDurationModel {
	def sample(nKeys:Int): Long
}
case class LinearCopyDurationModel(
	durationPerKey: Double
) extends CopyDurationModel {
	def sample(nKeys:Int):Long = (durationPerKey*nKeys).toLong
}

// duration of placement "removeData"
abstract class RemoveDataDurationModel {
	def sample(nKeys:Int): Long
}
case class LinearRemoveDataDurationModel(
	durationPerKey: Double
) extends RemoveDataDurationModel {
	def sample(nKeys:Int):Long = (durationPerKey*nKeys).toLong
}

// duration of placement "remove"
abstract class RemoveDurationModel {
	def sample(nKeys:Int): Long
}
case class ConstantRemoveDurationModel(
	duration: Long
) extends RemoveDurationModel {
	def sample(nKeys:Int):Long = duration
}
case class LinearRemoveDurationModel(
	durationPerKey: Double
) extends RemoveDurationModel {
	def sample(nKeys:Int):Long = (nKeys*durationPerKey).toLong
}

// duration of placement "getNodeRange"
abstract class GetNodeRangeDurationModel {
	def sample(): Long
}
case class ConstantGetNodeRangeDurationModel(
	duration: Long
) extends GetNodeRangeDurationModel {
	def sample():Long = duration
}

