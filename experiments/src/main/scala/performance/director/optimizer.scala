package scads.director

abstract class CostFunction {
	def cost(state:SCADSState):Double
	def detailedCost(state:SCADSState):Map[String,Double]
}

class TestCostFunction extends CostFunction {
	import java.util.Random
	val rand = new Random

	def cost(state:SCADSState):Double = rand.nextDouble
	def detailedCost(state:SCADSState):Map[String,Double] = Map("rnd"->rand.nextDouble)
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
	
	def cost(state:SCADSState):Double = detailedCost(state).map(_._2).reduceLeft(_+_)
	
	def detailedCost(state:SCADSState):Map[String,Double] = {
		val stats = performanceEstimator.estimatePerformance(state.config,state.workloadHistogram,1,null)
		val nMachines = state.config.storageNodes.size
		
		val violation = ( 	if (getSLA==50) ( 1-stats.nGetsAbove50.toDouble/stats.nGets<slaPercentile )
							else ( 1-stats.nGetsAbove100.toDouble/stats.nGets<slaPercentile )
						) || (
							if (putSLA==50) ( 1-stats.nPutsAbove50.toDouble/stats.nPuts<slaPercentile )
							else ( 1-stats.nPutsAbove100.toDouble/stats.nPuts<slaPercentile )
						)
		Map("machines"->nMachines*nodeCost, "slaviolations"->(if(violation)violationCost else 0.0))
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
/* TODO: (1) make loop finite even if cost doesn't decrease, (2) explore multiple branches at depth,
* (3) consider periods where cost goes up a little bit
*/
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

case class HeuristicOptimizer(performanceEstimator:PerformanceEstimator, getSLA:Int, putSLA:Int) extends Optimizer {
	val slaPercentile = 0.99
	val max_replicas = 5

	def optimize(state:SCADSState): List[Action] = {
		var actions = new scala.collection.mutable.ListBuffer[Action]()
		val overloaded:Map[String,PerformanceStats] = getOverloadedServers(state)
		println("Have "+overloaded.size+" overloaded servers")

		// for overloaded servers, determine their mini ranges and replicas
		val overloaded_config = new SCADSconfig(Map(overloaded.keys.toList map {s => (s, state.config.storageNodes(s))} : _*))
		val overloaded_ranges = performanceEstimator.getServerHistogramRanges(overloaded_config,state.workloadHistogram)

		// create mapping of List[replicas] -> List[histogram ranges]
		val overloaded_ranges_replicas = Map[List[String],List[DirectorKeyRange]](overloaded_ranges.toList map {entry =>
			(state.config.getReplicas(entry._1), entry._2)
			} : _*)

		// try splitting/replicating the overloaded servers
		overloaded_ranges_replicas.foreach((entry)=>{
			println("Attempting to optimze overloaded server "+entry._1.first + " with "+entry._1.size + " replicas")
			val changes = trySplitting(entry._1, entry._2,state)
			actions.insertAll(actions.size,translateToActions(entry._1,changes,state))
			actions += Remove(entry._1) // remove original server(s)
		})

		// pick a replica to remove or merge to do, if possible
		// TODO

		actions.toList
	}
	/**
	* determine overloaded servers by seeing which ones have SLA violations
	*/
	def getOverloadedServers(state:SCADSState):Map[String,PerformanceStats] = {
		val server_stats = estimateServerStats(state)
		server_stats.filter((entry)=> violatesSLA(entry._2))
	}
	/**
	* Check if predicted stats violate the get() or put() SLA,
	* based on the SLA percentile.
	*/
	def violatesSLA(stats:PerformanceStats):Boolean = {
		( 	if (getSLA==50) ( 1-stats.nGetsAbove50.toDouble/stats.nGets<slaPercentile )
			else ( 1-stats.nGetsAbove100.toDouble/stats.nGets<slaPercentile )
		) || (
			if (putSLA==50) ( 1-stats.nPutsAbove50.toDouble/stats.nPuts<slaPercentile )
			else ( 1-stats.nPutsAbove100.toDouble/stats.nPuts<slaPercentile )
		)
	}
	def estimateServerStats(state:SCADSState):Map[String,PerformanceStats] = { // TODO: sometimes empty serverworkloadranges?
		Map[String,PerformanceStats](state.config.storageNodes.toList map {
			entry => (entry._1, estimateSingleServerStats(entry._1, state.config.getReplicas(entry._1).size, 1.0,DirectorKeyRange(entry._2.minKey,entry._2.maxKey), state)) 
			} : _*)
	}
	def estimateSingleServerStats(server:String, num_replicas:Int, allowed_puts:Double, range:DirectorKeyRange, state:SCADSState):PerformanceStats = {
		performanceEstimator.estimatePerformance(new SCADSconfig( Map[String,DirectorKeyRange](server -> range)),state.workloadHistogram.divide(num_replicas,allowed_puts),10,null) 
	}
	/**
	* Attempt splitting actions of a set of replicas, where at least one of the replicas is overloaded
	* Actions done to one replica should be done to all others
	*/
	 def trySplitting(servers:List[String], ranges: List[DirectorKeyRange],state:SCADSState): Map[DirectorKeyRange,(Int,Double)] = {
		var changes = Map[DirectorKeyRange,(Int,Double)]()
		val server = servers.first // try actions on just one of the replicas
		val rangeArray = ranges.sort(_.minKey<_.minKey).toArray
		var id = 0
		var startId = 0
		var endId = startId
		println("Working on range "+rangeArray(0).minKey+" - "+rangeArray(ranges.size-1).maxKey+" ----------------- ")
		while (id < rangeArray.size) {
			// include this mini range on new server(s) if it wouldn't violate SLA
			if ( !violatesSLA(estimateSingleServerStats(server,servers.size,1.0,DirectorKeyRange(rangeArray(startId).minKey,rangeArray(id).maxKey), state)) ) {
				endId = id
				println(rangeArray(startId).minKey+" - "+rangeArray(endId).maxKey +", size("+(endId-startId)+") ok")
				id+=1
				// finish up the server when get to last range
				if (id==rangeArray.size) changes += (DirectorKeyRange(rangeArray(startId).minKey,rangeArray(id-1).maxKey) -> (servers.size,0) )
			}
			else {
				println(rangeArray(startId).minKey+" - "+rangeArray(id).maxKey +", size("+(id-startId)+") violated SLA")
				if ( (id-startId) == 0 ) { // even one mini range violates, try replication
					changes ++= tryReplicatingAndRestricting(servers, rangeArray(id),state); id += 1
					}
				else {
					println("Creating split from "+rangeArray(startId).minKey+" - "+rangeArray(endId).maxKey)
					changes += (DirectorKeyRange(rangeArray(startId).minKey,rangeArray(endId).maxKey) -> (servers.size,0) )
				}
				startId = id // continue split attempts from where left off
				endId = startId
			}
		}
		changes
	}
	/**
	* Try to add more replicas for this range to make it acceptable,
	* while also decrementing the amount of allowed puts
	*/
	def tryReplicatingAndRestricting(servers:List[String], range: DirectorKeyRange,state:SCADSState): Map[DirectorKeyRange,(Int,Double)] = {
		var changes = Map[DirectorKeyRange,(Int,Double)]()
		val server = servers.first
		var found = false
		var allowed:Int = 100 // use ints to avoid subtraction weirdness with doubles

		while (!found && allowed >= 0) {
			(servers.size to max_replicas).foreach((num_replicas)=>{
				if ( !found && !violatesSLA(estimateSingleServerStats(server,num_replicas,allowed.toDouble/100.0,range,state)) )
					{ changes += range -> (num_replicas,allowed.toDouble/100.0); found = true; println("Need "+num_replicas+ " of "+range.minKey+" - "+ range.maxKey+ " with "+(allowed.toDouble/100.0)+" allowed puts") }
			})
			allowed -= 10
		}
		if (!found) println("Unable to fix range "+range.minKey+" - "+ range.maxKey)
		changes
	}

	/**
	* Translate changes to a single server (and its replicas) into actions on that server(s).
	* Inputted ranges in map are not necessarily in sorted order :(
	*/
	 def translateToActions(servers: List[String], changes:Map[DirectorKeyRange,(Int,Double)],state:SCADSState):List[Action] = {
		var actions = new scala.collection.mutable.ListBuffer[Action]()
		val actual_range = state.config.storageNodes(servers.first)
		val changesArray = changes.toList.sort(_._1.minKey < _._1.minKey).toArray

		(0 until changesArray.size).foreach((index)=>{
			val change = changesArray(index)
			val start = if (index==0) {actual_range.minKey} else {Math.max(actual_range.minKey,change._1.minKey)}
			val end = if (index==(changesArray.size-1)) {actual_range.maxKey} else {Math.min(actual_range.maxKey,change._1.maxKey)}
			actions += ReplicateFrom(servers.first,DirectorKeyRange(start,end),change._2._1)
		})
		actions.toList
	}
}
