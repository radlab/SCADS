package scads.director

import performance.Scads
import org.apache.log4j._
import org.apache.log4j.Level._

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement


abstract class Policy {
	val logger = Logger.getLogger("scads.director.policy")
	private val logPath = Director.basedir+"/policy.txt"
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
	logger.setLevel(DEBUG)
	
	protected def act(state:SCADSState, pastActions:List[Action]): List[Action]
	
	def stateColumns(): List[String] = List[String]()
	def stateValues(): List[String] = List[String]()
	
	val dbname = "director"
	val dbtable = "policystate"
	
	var connection = Director.connectToDatabase
	createTable
	Action.initDatabase
	
	def perform(state:SCADSState, pastActions:List[Action]): List[Action] = {
		val actions = act(state,pastActions)
		storeState
		actions
	}
	
	def storeState() {
		val statement = connection.createStatement
		
		try {
			val sql = "INSERT INTO "+dbtable+" ("+(List("time","policyname")++stateColumns).mkString("`","`,`","`")+") values ("+
						(List(new java.util.Date().getTime,"'"+getClass.getName.split('.').last+"'")++stateValues.map("'"+_+"'")).mkString(",")+")"
			logger.debug("storing policy state: "+sql)
			statement.executeUpdate(sql)
		} catch { case e:Exception => logger.warn("exception when storing policy state: ",e) }
		finally { statement.close }
	}
	
    def createTable() {
    // create database if it doesn't exist and select it
        try {
            val statement = connection.createStatement
            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbname)
            statement.executeUpdate("USE " + dbname)
/*			statement.executeUpdate("DROP TABLE IF EXISTS "+dbtable)*/
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS "+dbtable+" (`time` BIGINT, `policyname` VARCHAR(50)"+stateColumns.map(",`"+_+"` VARCHAR(50)").mkString(",")+")" )
			statement.close
       	} catch { case ex: SQLException => ex.printStackTrace() }

    }

	
}


class TestPolicy(
	val maxactions:Int
) extends Policy {
	val rnd = new java.util.Random
	
	object PolicyState extends Enumeration("waiting","noNewActions","newActions") {
	  type PolicyState = Value
	  val Waiting, NoNewActions, NewActions = Value
	}
	import PolicyState._
	
	private var _stateValues = List[String]()
	override def stateColumns():List[String] = List("state","nActionsStarted")
	override def stateValues():List[String] = _stateValues
	
	override def act(state:SCADSState, pastActions:List[Action]): List[Action] = {
		logger.debug("acting")

		var policyState =
		if (!pastActions.forall(_.completed)) Waiting
		else if (rnd.nextDouble>0.5) NewActions
		else NoNewActions
		
		val actions = policyState match {
			case Waiting => List[Action]()
			case NoNewActions => List[Action]()
			case NewActions => (1 to (rnd.nextInt(maxactions)+1)).map( (d:Int) => new TestAction(rnd.nextInt((d+1)*30)*1000) ).toList
		}		
		_stateValues = List(policyState.toString,actions.length.toString)
		actions
	}
}

class RandomSplitAndMergePolicy(
	val fractionOfSplits:Double
) extends Policy {
	val rnd = new java.util.Random
	
	override def act(state:SCADSState, pastActions:List[Action]):List[Action] = {
		if (rnd.nextDouble<fractionOfSplits)
			List(new SplitInTwo( state.config.storageNodes.keySet.toList(rnd.nextInt(state.config.storageNodes.size)) ))
		else 
			if (state.config.storageNodes.size>=2) {
				val i = rnd.nextInt(state.config.storageNodes.size-1)
				val ordered = state.config.storageNodes.map(x=>(x._1,x._2)).toList.sort(_._2.minKey<_._2.minKey).toList
				List(new MergeTwo(ordered(i)._1,ordered(i+1)._1))
			} else List[Action]()
	}
}

class SplitAndMergeOnPerformance(
	val latencyToMerge: Double,
	val latencyToSplit: Double
) extends Policy {
	
	object PolicyState extends Enumeration {
	  type PolicyState = Value
	  val Waiting, Executing = Value
	}
	import PolicyState._
	
	val scads_deployment = Director.myscads
	var policyState = PolicyState.Waiting
	var scadsState: SCADSState = null
	var pastActions: List[Action] = null
	
	override def act(state:SCADSState, pastActions:List[Action]): List[Action] = {
		this.scadsState = state
		this.pastActions = pastActions
		
		policyState = selectNextState
		
		var actions = new scala.collection.mutable.ListBuffer[Action]()
		policyState match {
			case PolicyState.Waiting => {null}
			case PolicyState.Executing => {
				val nodes = scadsState.storageNodes
				// decide what actions to take: splitting and/or merging
				nodes.foreach((node_state)=>{ // use workload for now
					if (node_state.metrics.workload >= latencyToSplit) {
						logger.debug("Adding split action: "+node_state.ip)
						actions += new SplitInTwo(node_state.ip)
					}
				})
				var id = 0
				while (id <= nodes.size-1 && id <= nodes.size-2) {
					if ( ((nodes(id).metrics.workload + nodes(id+1).metrics.workload) <= latencyToMerge) ||
					 nodes(id).metrics.workload.isNaN || nodes(id+1).metrics.workload.isNaN ) { // also check for NaNs
						logger.debug("Adding merge action: "+nodes(id).ip+" and "+nodes(id+1).ip)
						actions += new MergeTwo(nodes(id).ip,nodes(id+1).ip)
						id+=2 // only merge disjoint two at a time
					}
					else id+=1
				}
			} // end executing
		}
		actions.toList
	}
	
	private def selectNextState():PolicyState = {
		policyState match {
			case PolicyState.Waiting => {
				if (pastActions.forall(_.completed)) Executing
				else Waiting
			}
			case PolicyState.Executing => {
				if (!pastActions.forall(_.completed)) Waiting // check if actions running
				else Executing
			}
		}
	}

	override def toString = "SplitAndMergeOnPerformance ( Merge: "+latencyToMerge+", Split: "+latencyToSplit+ " )"
}

class HeuristicOptimizerPolicy(
	val modelfile_location:String,
	val getSLA:Int,
	val putSLA:Int
	) extends Policy {
		val performanceEstimator = SimplePerformanceEstimator( L1PerformanceModel(modelfile_location) )
		val optimizer = new HeuristicOptimizer(performanceEstimator,getSLA,putSLA)
		override def act(state:SCADSState, pastActions:List[Action]):List[Action] = optimizer.optimize(state)
}
