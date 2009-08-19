package scads.director

import performance.Scads
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

// TODO:
// - check for failures
// - check state
// - timeouts?


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
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
	logger.setLevel(DEBUG)
	
	var initTime: Date = new Date
	var startTime: Date = null
	var endTime: Date = null
		
	var _state = ActionState.Ready
	def state():ActionState = _state
	def completed():Boolean = state()==ActionState.Completed
	
	var executionThread: Thread = null
	
	var dbID: Int = -1
		
	def startExecuting() {
		dbID = Action.store(this)
		executionThread = new Thread(this)
		executionThread.start
	}
	
	def csvArgs():String = ""
	
	override def run() {
		_state = ActionState.Running
		logger.info("starting action execution")
		startTime = new Date
		Action.store(this)
		execute()
		_state = ActionState.Completed
		logger.info("action execution completed")
		endTime = new Date
		Action.store(this)
	}
	
	def execute()
	def preview(config:SCADSconfig): SCADSconfig = config
	def participants: Set[String]
	def toString(): String
}

abstract class ActionSelector {
	import java.util.Random
	val rand = new Random
	def getRandomAction(state: SCADSState):Action
}

class UniformSelector(choices:List[String]) extends ActionSelector {
	var mychoices = choices
	val replica_limit = 4
	def getRandomAction(state:SCADSState):Action = {
		var nodes = state.config.getNodes 		// inspect config to see what servers are available to take action on
		val node1:String = nodes.apply(rand.nextInt(nodes.size))
		nodes = nodes.remove((elem:String) => elem == node1 )
		val node2:String = if (nodes.size > 0) { nodes.apply(rand.nextInt(nodes.size)) }
							else { mychoices = mychoices.remove((elem:String) => elem == "MergeTwo" ); null} // need >1 node for merge

		val choice = mychoices.apply(rand.nextInt(mychoices.size)) // choose uniformly at rondom amongst possible choices
		choice match {
			case "SplitInTwo" => SplitInTwo(node1)
			case "MergeTwo" => MergeTwo(node1,node2)
			case "Replicate" => Replicate(node1,rand.nextInt(replica_limit))
			case _ => null
		}
	}
}

object Action {
	val dbname = "director"
	val dbtable = "actions"
	
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

        println("have connection to database")
    }

	def store(action:Action): Int = {
		if (connection==null) connection = Director.connectToDatabase
		val statement = connection.createStatement
		
		val cols = Map("update_time"-> (new Date).getTime.toString,
					   "action_name"-> action.getClass.toString.split('.').last,
					   "init_time"-> (if (action.initTime==null) "null" else action.initTime.getTime.toString),
					   "start_time"-> (if (action.startTime==null) "null" else action.startTime.getTime.toString),
					   "end_time"-> (if (action.endTime==null) "null" else action.endTime.getTime.toString),
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
	val server: String
) extends Action("splitintwo("+server+")") with PlacementManipulation {

	override def execute() {
		logger.debug("Getting new storage server")
		val new_guys = Director.serverManager.getServers(1)
		if (new_guys.isEmpty) { logger.warn("Split failed: no available servers"); return }
		val new_guy = new_guys(0)

		// determine current range and split-point to give new server
		val bounds = getNodeRange(server)
		val start = bounds._1
		val end = bounds._2
		val middle = ((end-start)/2) + start
		Thread.sleep(5*1000)
		
		// do the move and update local list of servers
		logger.info("Moving "+middle+" - "+end+" from "+server+" to "+ new_guy)
		move(server,new_guy,middle,end)
		logger.debug("Sleeping")
		Thread.sleep(60*1000) // wait minute
	}
	override def preview(config:SCADSconfig): SCADSconfig = {
		val bounds = config.storageNodes(server)
		var nodeConfig = config.storageNodes - server // make copy of previous node arrangment, removing the obsolete entry

		val start = bounds.minKey
		val end = bounds.maxKey
		val middle = ((end-start)/2) + start
		nodeConfig = nodeConfig.update(server, new DirectorKeyRange(start,middle))
		nodeConfig = nodeConfig.update("SPLIT_"+server, new DirectorKeyRange(middle,end))
		SCADSconfig(nodeConfig)
	}
	def participants = Set[String](server)
	override def toString:String = actionShortName
}

case class MergeTwo(
	val server1: String,
	val server2: String
) extends Action("mergetwo("+server1+","+server2+")") with PlacementManipulation {

	override def execute() {
		val bounds = getNodeRange(server1)
		val start = bounds._1
		val end = bounds._2
		logger.info("Copying "+start+" - "+end+" from "+server1+" to "+ server2)
		copy(server1,server2,start,end) // do copy instead of move to avoid sync problems?
		logger.debug("Removing from placement: "+ server1)
		val removing = server1
		remove(removing)
		logger.debug("Releasing server "+ server1)
		Director.serverManager.releaseServer(removing)
		logger.debug("Sleeping")
		Thread.sleep(60*1000) // wait minute
	}

	override def preview(config:SCADSconfig): SCADSconfig = {
		val bounds1 = config.storageNodes(server1)
		val bounds2 = config.storageNodes(server2)
		var nodeConfig = (config.storageNodes - server1) - server2 // copy of config, without two obsolete entries

		val start = Math.min(bounds1.minKey,bounds2.minKey)
		val end = Math.max(bounds1.maxKey, bounds2.maxKey)
		nodeConfig = nodeConfig.update(server2, new DirectorKeyRange(start,end))
		SCADSconfig(nodeConfig)
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
		val new_guys = Director.serverManager.getServers(num)
		if (new_guys.isEmpty) { logger.warn("Replication failed: no available servers"); return }

		// determine current range to give new servers
		val bounds = getNodeRange(server)
		val start = bounds._1
		val end = bounds._2
		Thread.sleep(5*1000)

		// do the copy and update local list of servers (serially)
		new_guys.foreach((new_guy)=> {
			logger.info("Copying "+start+" - "+end+" from "+server+" to "+ new_guy)
			copy(server,new_guy,start,end)
			logger.debug("Sleeping")
		})
		Thread.sleep(60*1000) // wait minute
	}
	override def preview(config:SCADSconfig): SCADSconfig = {
		val bounds = config.storageNodes(server)
		var nodeConfig = config.storageNodes

		val start = bounds.minKey
		val end = bounds.maxKey
		(1 to num).foreach((n)=> {
			nodeConfig = nodeConfig.update("REPLICA_"+n+"_"+server, new DirectorKeyRange(start,end))
		})
		SCADSconfig(nodeConfig)
	}
	def participants = Set[String](server)
	override def toString:String = actionShortName
}

trait PlacementManipulation extends RangeConversion with AutoKey {
	val xtrace_on = Director.xtrace_on
	val namespace = Director.namespace
	var placement_host:String = null

	private def init = {
		placement_host = Director.myscads.placement.get(0).privateDnsName
	}

	protected def getNodeRange(host:String):(Int, Int) = {
		if (placement_host == null) init
		val dp = Scads.getDataPlacementHandle(placement_host,xtrace_on)
		val s_info = dp.lookup_node(namespace,host,Scads.server_port,Scads.server_sync)
		val range = s_info.rset.range
		(Scads.getNumericKey( StringKey.deserialize_toString(range.start_key,new java.text.ParsePosition(0)) ),
		Scads.getNumericKey( StringKey.deserialize_toString(range.end_key,new java.text.ParsePosition(0)) ))
	}

	protected def move(source_host:String, target_host:String,startkey:Int, endkey:Int) = {
		if (placement_host == null) init
		val dpclient = Scads.getDataPlacementHandle(placement_host,xtrace_on)
		val range = new KeyRange(new StringKey(Scads.keyFormat.format(startkey)), new StringKey(Scads.keyFormat.format(endkey)) )
		dpclient.move(namespace,range, source_host, Scads.server_port,Scads.server_sync, target_host, Scads.server_port,Scads.server_sync)
	}

	protected def copy(source_host:String, target_host:String,startkey:Int, endkey:Int) = {
		if (placement_host == null) init
		val dpclient = Scads.getDataPlacementHandle(placement_host,xtrace_on)
		val range = new KeyRange(new StringKey(Scads.keyFormat.format(startkey)), new StringKey(Scads.keyFormat.format(endkey)) )
		dpclient.copy(namespace,range, source_host, Scads.server_port,Scads.server_sync, target_host, Scads.server_port,Scads.server_sync)

	}
	protected def remove(host:String) = {
		if (placement_host == null) init
		val dpclient = Scads.getDataPlacementHandle(placement_host,xtrace_on)
		val bounds = getNodeRange(host)
		val range = new KeyRange(new StringKey(Scads.keyFormat.format(bounds._1)), new StringKey(Scads.keyFormat.format(bounds._2)) )
		val list = new java.util.LinkedList[DataPlacement]()
		list.add(new DataPlacement(host,Scads.server_port,Scads.server_sync,range))
		dpclient.remove(namespace,list)
	}
}