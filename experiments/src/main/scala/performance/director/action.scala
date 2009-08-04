package scads.director

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
	object ActionState extends Enumeration {
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
	
	Action.store(this)
		
	def startExecuting() {
		executionThread = new Thread(this)
		executionThread.start
	}
	
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
	def toString(): String
}


object Action {
	var dbhost: String = ""
	val dbname = "actions"
	val dbtable = "actions"
	val user = "root"
	val pass = ""
	
	var connection: Connection = null
	def init(host:String) {
		dbhost = host
	}
	
	def connectToDatabase() {
        // open connection to the database
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance()
        } catch {
			case ex: Exception => ex.printStackTrace() }

        try {
            val connectionString = "jdbc:mysql://" + dbhost + "/?user=" + user + "&password=" + pass
            connection = DriverManager.getConnection(connectionString)
		} catch {
			case ex: SQLException => {
            	// handle any errors
	            println("can't connect to the database")
	            println("SQLException: " + ex.getMessage)
	            println("SQLState: " + ex.getSQLState)
	           	println("VendorError: " + ex.getErrorCode)
	        }
		}

        // create database if it doesn't exist and select it
        try {
            val statement = connection.createStatement
			statement.executeUpdate("DROP DATABASE IF EXISTS "+dbname)
            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbname)
            statement.executeUpdate("USE " + dbname)
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS "+dbtable+" (`id` INT NOT NULL AUTO_INCREMENT, `update_time` BIGINT, `action_name` VARCHAR(30),"+
																			"`init_time` BIGINT, `start_time` BIGINT, `end_time` BIGINT, `status` VARCHAR(50),"+
																			"`short_name` VARCHAR(50), `args` VARCHAR(200), PRIMARY KEY(`id`) ) ")
			statement.close
       	} catch { case ex: SQLException => ex.printStackTrace() }

        println("have connection to database")
    }

	def store(action:Action) {
		if (connection==null) connectToDatabase
		val statement = connection.createStatement
		try {
			val cols = Map("update_time"-> (new Date).getTime.toString,
						   "action_name"-> action.getClass.toString.split('.').last,
						   "init_time"-> (if (action.initTime==null) "null" else action.initTime.getTime.toString),
						   "start_time"-> (if (action.startTime==null) "null" else action.startTime.getTime.toString),
						   "end_time"-> (if (action.endTime==null) "null" else action.endTime.getTime.toString),
						   "status"-> action.state.toString,
						   "short_name"-> action.actionShortName,
						   "args"->"").transform( (x,y) => if (y=="null") "null" else ("'"+y+"'") )
			val colnames = cols.keySet.toList
			val sql = "INSERT INTO "+dbtable+" ("+colnames.mkString("`","`,`","`")+") values ("+colnames.map(cols(_)).mkString(",")+")"
			action.logger.debug("storing action: "+sql)
			statement.executeUpdate(sql)
			statement.close
		} catch { case ex:Exception => action.logger.warn("exception when storing action",ex) }
		finally { statement.close }
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
	override def toString():String = actionShortName
}

class SplitInTwo(
	val server: String,
	val placement: String
) extends Action("splitintwo("+server+")") {
	override def execute() {
		
	}
	override def toString:String = actionShortName
}

class MergeTwo(
	val server1: String,
	val server2: String
) extends Action("mergetwo("+server1+","+server2+")") {
	override def execute() {
		
	}
	override def toString:String = actionShortName
}