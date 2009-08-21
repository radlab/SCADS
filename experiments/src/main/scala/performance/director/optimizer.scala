package scads.director

abstract class CostFunction {
	def cost(state:SCADSState):Double
}

class TestCostFunction extends CostFunction {
	import java.util.Random
	val rand = new Random

	def cost(state:SCADSState):Double = rand.nextDouble
}

class SLACostFunction(
	getSLA:Int, 			// time in ms
	putSLA:Int, 			// time in ms
	slaPercentile:Double,	// what percentile to consider when evaluating possible violation
	violationCost:Double, 	// cost for violating SLA
	nodeCost:Double,		// cost for adding new storage server
	performanceEstimator:PerformanceEstimator  
) extends CostFunction
{
	assert(getSLA==50||getSLA==100||putSLA==50||putSLA==100,"only supporting SLA of 50ms or 100ms (see PerformanceStats)")
	
	def cost(state:SCADSState):Double = {
		val perfStats = performanceEstimator.estimatePerformance(state.config,state.workloadHistogram,1,null)
		val nMachines = state.config.storageNodes.size
		
		var getSLAViolation = false
		var putSLAViolation = false
		if (getSLA==50) if( 1-perfStats.nGetsAbove50.toDouble/perfStats.nGets<slaPercentile ) 	getSLAViolation=true
		else if( 1-perfStats.nGetsAbove100.toDouble/perfStats.nGets<slaPercentile ) 			getSLAViolation=true
		if (putSLA==50) if( 1-perfStats.nPutsAbove50.toDouble/perfStats.nPuts<slaPercentile ) 	putSLAViolation=true
		else if( 1-perfStats.nPutsAbove100.toDouble/perfStats.nPuts<slaPercentile ) 			putSLAViolation=true
		
		nMachines*nodeCost + (if(getSLAViolation||putSLAViolation)violationCost else 0.0)
	}
}

abstract class Optimizer {
	/**
	* Given the current systen configuration and performance,
	* determine an optimal set of actions to perform on this state
	*/
	def optimize(state:SCADSState): List[Action]
	def overlaps(servers1:Set[String], servers2:Set[String]):Boolean = servers1.intersect(servers2).size !=0
}

case class DepthOptimizer(depth:Int, coster:CostFunction, selector:ActionSelector) extends Optimizer {

	def optimize(state:SCADSState): List[Action] = {
		var best_cost = coster.cost(state)
		var actions = new scala.collection.mutable.ListBuffer[Action]
		var participating_servers = Set[String]()
		var new_state = state

		// try adding another action while not exceeding depth or run out of servers to do stuff to
		while ( (actions.size < depth) && (participating_servers.size < state.config.getNodes.size) ) {
			val next_action = selector.getRandomAction(state) // only consider new actions on original state, since conc exec possible
			val tentative_state = new_state.changeConfig( next_action.preview(new_state.config) )
			println("trying action "+next_action + " with cost "+coster.cost(tentative_state))
			// if adding this actinos reduces costs, and doesn't use already used servers, let's use it!
			if ( (coster.cost(tentative_state) < best_cost) && !overlaps(next_action.participants,participating_servers) ) {
				actions += next_action
				participating_servers = participating_servers ++ next_action.participants
				new_state = tentative_state
			}
		}
		actions.toList
	}
}
