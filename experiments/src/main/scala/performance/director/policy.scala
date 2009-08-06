package scads.director

import performance.Scads
import org.apache.log4j._
import org.apache.log4j.Level._

abstract class Policy {
	val logger = Logger.getLogger("scads.director.policy")
	private val logPath = Director.basedir+"/policy.txt"
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
	logger.setLevel(DEBUG)
	
	def act(state:SCADSState, pastActions:List[Action]): List[Action]
}


class TestPolicy(
	val maxactions:Int
) extends Policy {
	val rnd = new java.util.Random
	
	override def act(state:SCADSState, pastActions:List[Action]): List[Action] = {
		logger.debug("acting")
		if (!pastActions.forall(_.completed)) {
			logger.info("some actions still runnning")
			return List[Action]()
		}
	
		val r = rnd.nextDouble
		val i = rnd.nextInt(maxactions)
		val range = 0 to i
		logger.debug("rnd="+r+"  >0.5="+(r>0.5)+"  i="+i+"  range="+range)
		if (r>0.5) range.map( (d:Int) => new TestAction(rnd.nextInt((d+1)*30)*1000) ).toList
		else List[Action]()
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
					if (node_state.metrics.workload >= latencyToSplit) actions += new SplitInTwo(node_state.ip)
				})
				var id = 0
				while (id <= nodes.size-1 && id <= nodes.size-2) {
					if ( (nodes(id).metrics.workload + nodes(id+1).metrics.workload) <= latencyToMerge) {
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


