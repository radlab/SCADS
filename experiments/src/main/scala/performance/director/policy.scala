package scads.director

import org.apache.log4j._
import org.apache.log4j.Level._

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement


abstract class Policy(
	val workloadPredictor:WorkloadPrediction
){
	val logger = Logger.getLogger("scads.director.policy")
	private val logPath = Director.basedir+"/policy.txt"
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
	logger.setLevel(DEBUG)
	
/*	protected def act(state:SCADSState, pastActions:List[Action]): List[Action]*/
	protected def act(state:SCADSState, actionExecutor:ActionExecutor)
	
	def stateColumns(): List[String] = List[String]()
	def stateValues(): List[String] = List[String]()
	
	val dbname = "director"
	val dbtable = "policystate"
	
	var connection = Director.connectToDatabase
	initialize
	
	def initialize {
		connection = Director.connectToDatabase
		createTable
		Action.initDatabase
		workloadPredictor.initialize
	}
	
	def getParams:Map[String,String] = workloadPredictor.getParams
	
	def periodicUpdate(state:SCADSState) {
		workloadPredictor.addHistogram( state.workloadHistogram )
	}
	
	def perform(state:SCADSState, actionExecutor:ActionExecutor) {
		try { 
			Action.currentInitTime = new java.util.Date().getTime
			act(state,actionExecutor)
		} catch {
			case e:Exception => logger.warn("exception in policy.act",e)
		}
		storeState
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
	override def toString:String = getParams.toList.sort(_._1<_._1).map( p => p._1+"="+p._2 ).mkString("(",",",")")
}


class TestPolicy(
	val maxactions:Int,
	override val workloadPredictor:WorkloadPrediction
) extends Policy(workloadPredictor) {
	object PolicyState extends Enumeration("waiting","noNewActions","newActions") {
	  type PolicyState = Value
	  val Waiting, NoNewActions, NewActions = Value
	}
	import PolicyState._
	
	private var _stateValues = List[String]()
	override def stateColumns():List[String] = List("state","nActionsStarted")
	override def stateValues():List[String] = _stateValues
	
	override def act(state:SCADSState, actionExecutor:ActionExecutor) {
		logger.debug("acting")

		var policyState =
		if (!actionExecutor.allActionsCompleted) Waiting
		else if (Director.rnd.nextDouble>0.5) NewActions
		else NoNewActions
		
		val actions = policyState match {
			case Waiting => List[Action]()
			case NoNewActions => List[Action]()
			case NewActions => (1 to (Director.rnd.nextInt(maxactions)+1)).map( (d:Int) => new TestAction(Director.rnd.nextInt((d+1)*30)*1000) ).toList
		}		
		_stateValues = List(policyState.toString,actions.length.toString)
		actions.foreach(actionExecutor.addAction(_))
	}
}


class TestSimpleSplitMerge(
	val delay:Long,
	override val workloadPredictor:WorkloadPrediction
) extends Policy(workloadPredictor) {
	var lastActionTime = new java.util.Date().getTime
	var splitLast = false
		
	override def act(state:SCADSState, actionExecutor:ActionExecutor) {
		if (state!=null) {
			logger.debug("ACTING: received the following state ("+(new java.util.Date().getTime-state.time)/1000+" seconds behind)\n"+state.toShortString)
			logger.debug("just config: \n"+state.toConfigString)
		} else
			logger.debug("ACTING: state == NULL")
			
		val prediction = workloadPredictor.getPrediction
		if (prediction!=null)
			logger.debug("workload prediction: "+workloadPredictor.getPrediction.toShortString)
		else
			logger.debug("workload prediction == NULL")
		
		if (new java.util.Date().getTime-lastActionTime > delay  &&  state!=null) {
			logger.debug("should act. splitLast="+splitLast)
			val nodes = state.config.storageNodes.keySet.toList
			logger.debug("have the following nodes: "+nodes.mkString(","))
			
			if (splitLast && nodes.size>=2) {
				logger.debug("will merge")
				actionExecutor.addAction( new MergeTwo(nodes(0),nodes(1)) )
				splitLast = false
			} else {
				logger.debug("will split")
				actionExecutor.addAction( new SplitInTwo(nodes(0),-1) )
				splitLast = true
			}
			lastActionTime = new java.util.Date().getTime
		} else 
			logger.debug("policy waiting")
	}
}

class EmptyPolicy(
	override val workloadPredictor:WorkloadPrediction
) extends Policy(workloadPredictor) {
	override def act(state:SCADSState, actionExecutor:ActionExecutor) {
		if (state!=null) 
			logger.debug("ACTING: received the following state ("+(new java.util.Date().getTime-state.time)/1000+" seconds behind)\n"+state.toShortString)
		else
			logger.debug("ACTING: state == NULL")
			
		val prediction = workloadPredictor.getPrediction
		if (prediction!=null)
			logger.debug("workload prediction: "+workloadPredictor.getPrediction.toShortString)
		else
			logger.debug("workload prediction == NULL")
	}
}

class RandomSplitAndMergePolicy(
	val fractionOfSplits:Double,
	override val workloadPredictor:WorkloadPrediction
) extends Policy(workloadPredictor) {
	override def act(state:SCADSState, actionExecutor:ActionExecutor) {
		val actions = if (Director.rnd.nextDouble<fractionOfSplits)
			List(new SplitInTwo( state.config.storageNodes.keySet.toList(Director.rnd.nextInt(state.config.storageNodes.size)),-1 ))
		else 
			if (state.config.storageNodes.size>=2) {
				val i = Director.rnd.nextInt(state.config.storageNodes.size-1)
				val ordered = state.config.storageNodes.map(x=>(x._1,x._2)).toList.sort(_._2.minKey<_._2.minKey).toList
				List(new MergeTwo(ordered(i)._1,ordered(i+1)._1))
			} else List[Action]()
		actions.foreach(actionExecutor.addAction(_))
	}
}

class SplitAndMergeOnPerformance(
	val latencyToMerge: Double,
	val latencyToSplit: Double,
	override val workloadPredictor:WorkloadPrediction	
) extends Policy(workloadPredictor) {
	
	object PolicyState extends Enumeration {
	  type PolicyState = Value
	  val Waiting, Executing = Value
	}
	import PolicyState._
	
	var policyState = PolicyState.Waiting
	var scadsState: SCADSState = null
	
	override def act(state:SCADSState, actionExecutor:ActionExecutor) {
		this.scadsState = state
		
		policyState = selectNextState(actionExecutor)
		
		var actions = new scala.collection.mutable.ListBuffer[Action]()
		policyState match {
			case PolicyState.Waiting => {null}
			case PolicyState.Executing => {
				val nodes = scadsState.storageNodes.sort( (s,t) => scadsState.config.storageNodes(s.ip).minKey<scadsState.config.storageNodes(t.ip).minKey )
				// decide what actions to take: splitting and/or merging
				nodes.foreach((node_state)=>{ // use workload for now
					//logger.debug("split "+node_state.ip+"? w="+node_state.metrics.workload+" >? "+latencyToSplit)
					if (node_state.metrics.workload >= latencyToSplit) {
						logger.debug("Adding split action: "+node_state.ip)
						actions += new SplitInTwo(node_state.ip,-1)
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
		actions.foreach(actionExecutor.addAction(_))
	}
	
	protected def selectNextState(actionExecutor:ActionExecutor):PolicyState = {
		policyState match {
			case PolicyState.Waiting => {
				if (actionExecutor.allActionsCompleted) Executing
				else Waiting
			}
			case PolicyState.Executing => {
				if (!actionExecutor.allActionsCompleted) Waiting // check if actions running
				else Executing
			}
		}
	}

	//override def toString = "SplitAndMergeOnPerformance ( Merge: "+latencyToMerge+", Split: "+latencyToSplit+ " )"
}

case class SplitAndMergeOnWorkload(
	val mergeThreshold: Double,
	val splitThreshold: Double,
	override val workloadPredictor:WorkloadPrediction
) extends SplitAndMergeOnPerformance(mergeThreshold,splitThreshold,workloadPredictor) {

	override def getParams:Map[String,String] = Map("mergeThreshold"->mergeThreshold.toString,"splitThreshold"->splitThreshold.toString)++workloadPredictor.getParams

	override def act(state:SCADSState, actionExecutor:ActionExecutor) {
		this.scadsState = state

		policyState = selectNextState(actionExecutor)

		var actions = new scala.collection.mutable.ListBuffer[Action]()
		policyState match {
			case PolicyState.Waiting => {null}
			case PolicyState.Executing => {
				val workloadPrediction = workloadPredictor.getPrediction
				val serverWorkload = PerformanceEstimator.estimateServerWorkload(scadsState.config,workloadPrediction)
				
				//val servers_ranges = PerformanceEstimator.getServerHistogramRanges(scadsState.config,state.workloadHistogram)
				val servers_ranges = PerformanceEstimator.getServerHistogramRanges(scadsState.config,workloadPrediction)
				val config_nodes = scadsState.config.storageNodes
				val nodes = scadsState.storageNodes.sort( (s,t) => config_nodes(s.ip).minKey < config_nodes(t.ip).minKey )

				nodes.foreach((node_state)=>{
					//if (node_state.metrics.workload*(1+overprovision) >= splitThreshold) {
					if (serverWorkload(node_state.ip).sum >= splitThreshold) {
						logger.debug("Adding split action: "+node_state.ip)
						val ranges = servers_ranges(node_state.ip)
						val histogram_slice = new WorkloadHistogram( // include info for ranges for this server
								workloadPrediction.rangeStats.filter((entry)=>ranges.contains(entry._1)))
						val first_split = histogram_slice.split(2)(0).toArray
						val splitKey = first_split(first_split.size-1).maxKey
						if (splitKey<config_nodes(node_state.ip).maxKey)
							actions += new SplitInTwo(node_state.ip, splitKey) // split on end of first half's last key
						else
							logger.debug("not splitting "+node_state.ip+" because there would be no keys on the new server")
					}
				})
				var id = 0
				while (id <= nodes.size-1 && id <= nodes.size-2) {
					val w0 = serverWorkload(nodes(id).ip).sum
					val w1 = serverWorkload(nodes(id+1).ip).sum
					
					if ( w0+w1 < mergeThreshold || w0.isNaN || w1.isNaN ) {
					
/*					if ( ((nodes(id).metrics.workload + nodes(id+1).metrics.workload)*(1+overprovision) <= mergeThreshold) ||
					 nodes(id).metrics.workload.isNaN || nodes(id+1).metrics.workload.isNaN ) { // also check for NaNs
*/												
						logger.debug("Adding merge action: "+nodes(id).ip+" and "+nodes(id+1).ip)
						actions += new MergeTwo(nodes(id).ip,nodes(id+1).ip)
						id+=2 // only merge disjoint two at a time
					}
					else id+=1
				}
			} // end executing
		}
		actions.foreach(actionExecutor.addAction(_))
	}
	//override def toString = "SplitAndMergeOnWorkload ( Merge: "+mergeThreshold+", Split: "+splitThreshold+ " )"	
}

class HeuristicOptimizerPolicy(
	val performanceModel:PerformanceModel,
	val getSLA:Int,
	val putSLA:Int,
	override val workloadPredictor:WorkloadPrediction
) extends Policy(workloadPredictor) {
	val performanceEstimator = SimplePerformanceEstimator( performanceModel )
	val optimizer = new HeuristicOptimizer(performanceEstimator,getSLA,putSLA,workloadPredictor)
	val maxPoolSize = 3
	val maxReplicas = 5

	override def act(state:SCADSState, actionExecutor:ActionExecutor) = {
		logger.debug("epoch: "+(new java.util.Date().getTime)+" ---------------------------\n")
		if (state!=null) logger.debug("ACTING: received the following state ("+(new java.util.Date().getTime-state.time)/1000+" seconds behind)\n"+state.toShortString)
		else logger.debug("ACTING: state == NULL")
		val prediction = workloadPredictor.getPrediction
		if (prediction!=null) logger.debug("workload prediction: "+workloadPredictor.getPrediction.toShortString)
		else logger.debug("workload prediction == NULL")

		// which actions are currently ongoing?
		val currentActions = actionExecutor.actions
		logger.debug("Current actions before optimizer acts:\n"+currentActions.mkString("\n"))

		// now actually act!
		if (actionExecutor.allActionsCompleted) { optimizer.optimize(state, actionExecutor) }

		// which actions scheduled?
		logger.debug("Actions optimizer added:\n"+(actionExecutor.actions -- currentActions).mkString("\n"))
	}
}
