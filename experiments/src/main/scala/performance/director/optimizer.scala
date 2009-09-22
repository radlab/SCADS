package scads.director

import java.util.Date

abstract class FullCostFunction {
	def addState(state:SCADSState)
	def cost():Double = cost(detailedCost)
	def cost(costs:List[Cost]):Double = costs.map(_.cost).reduceLeft(_+_)
	def detailedCost():List[Cost]
	def intervalSummary():String
}

case class MachineRunningStats(
	val server: String,
	val startTime: Long
) {
	var endTime = startTime
	var running = true
	
	def updateEndTime(time:Long) = { endTime=time }
	def stop = { running=false }
	override def toString() = { server+"  "+(new Date(startTime))+"  ->  "+(new Date(endTime)+(if (running) "  RUNNING" else "")) }
	def cost(nodeCost:Double, nodeInterval:Long) = { Math.ceil((endTime-startTime).toDouble/nodeInterval)*nodeCost }
}

case class MachineCost(
	val nodeCost: Double, 
	val nodeInterval: Long
) {
	var machines = Map[String, scala.collection.mutable.ListBuffer[MachineRunningStats]]()
	var wereRunning = Set[String]()

	def addNewInterval(time:Long, running:Set[String]) {
		var machinesToCheck = wereRunning
		machinesToCheck ++= running
		
		for (machine <- machinesToCheck) {
			if (wereRunning.contains(machine) && running.contains(machine))
				machines(machine)(machines(machine).size-1).updateEndTime(time)
			if (wereRunning.contains(machine) && !running.contains(machine)) {
				machines(machine)(machines(machine).size-1).updateEndTime(time)
				machines(machine)(machines(machine).size-1).stop
			}
			if (!wereRunning.contains(machine) && running.contains(machine)) {
				if (!machines.contains(machine)) machines += (machine -> new scala.collection.mutable.ListBuffer[MachineRunningStats]())
				machines(machine) += MachineRunningStats(machine,time)
			}
		}
		
		wereRunning = running
	}
	
	override def toString():String = {
		machines.map( ms=> ms._1+":\n" + ms._2.toList.map(m=>"%-30s".format(m.toString)+"  $"+m.cost(nodeCost,nodeInterval)).mkString("  ","\n  ","") ).mkString("\n")
	}
	
	def getCosts():List[Cost] = {
		List.flatten( machines.map( ms=> ms._2.toList.map(m=>Cost(new Date(m.endTime),"server "+m.server+" running from "+(new Date(m.startTime)+" to "+(new Date(m.endTime)) ),m.cost(nodeCost,nodeInterval))) ).toList )
	}
}

case class Cost(
	val time: Date,
	val description: String,
	val cost: Double
)

case class RequestCounts( val nSlow: Int, val nAll: Int ) { 
	def add(that:RequestCounts):RequestCounts = RequestCounts(nSlow+that.nSlow,nAll+that.nAll) 
	def fractionSlow():Double = { nSlow.toDouble / nAll }
	def fractionNormal():Double = { 1 - fractionSlow }
	override def toString() = { nSlow+"/"+nAll+"="+"%.3f".format(100*fractionSlow)+"%) req slow ("+"%.3f".format(100*fractionNormal)+"% normal)" }
}

case class FullSLACostFunction(
	getSLA:Int, 			// time in ms
	putSLA:Int, 			// time in ms
	slaPercentile:Double,	// what percentile to consider when evaluating possible violation
	slaInterval:Long,		// duration of SLA interval in milliseconds
	violationCost:Double, 	// cost for violating SLA
	nodeCost:Double,		// cost for adding new storage server
	nodeInterval:Long		// node cost interval in milliseconds
) extends FullCostFunction {
	var allStates = new scala.collection.mutable.ListBuffer[SCADSState]()
	
	assert(getSLA==50||getSLA==100||putSLA==50||putSLA==100,"only supporting SLA of 50ms or 100ms (see PerformanceStats)")

	def addState(state:SCADSState) { allStates+=state }
	
	def detailedCost():List[Cost] = {
		// compute SLA violation cost
		val getCosts = 
		allStates.map(s=>(s,s.time/slaInterval*slaInterval))
				 .foldLeft( scala.collection.mutable.Map[Long,RequestCounts]() )( (t,s)=>accumulateRequestStats(t,s,"get",getSLA) )
				 .map( x=>(x._1,x._2) )
				 .filter( x=>(1.0-x._2.nSlow.toDouble/x._2.nAll)<slaPercentile )
				 .map( x=>Cost(new Date(x._1), "get SLA violation ("+x._2.toString+")", violationCost) )

		val putCosts = 
		allStates.map(s=>(s,s.time/slaInterval*slaInterval))
				 .foldLeft( scala.collection.mutable.Map[Long,RequestCounts]() )( (t,s)=>accumulateRequestStats(t,s,"put",putSLA) )
				 .map( x=>(x._1,x._2) )
				 .filter( x=>(1.0-x._2.nSlow.toDouble/x._2.nAll)<slaPercentile )
				 .map( x=>Cost(new Date(x._1), "put SLA violation ("+x._2.toString+")", violationCost) )
				
		val machineCost = MachineCost(nodeCost,nodeInterval)
		for (state <- allStates) machineCost.addNewInterval(state.time, Set[String](state.config.storageNodes.keySet.toList:_*))
		Director.logger.debug(machineCost.toString)
		val machineCosts = machineCost.getCosts

		(getCosts++putCosts++machineCosts).toList.sort(_.time.getTime<_.time.getTime)
	}
	
	def intervalSummary():String = {
		"states groupped by SLA interval:\n"+
		allStates.map(s=>(s,s.time/slaInterval*slaInterval))
				.foldLeft( scala.collection.mutable.Map[Long,List[String]]() )( (t,s)=> {t(s._2)=t.getOrElse(s._2,List[String]())+s._1.time.toString; t} )
				.map( x=>(x._1,x._2) ).toList
				.sort( _._1<_._1 )
				.map( x=>x._1+"\n"+x._2.mkString("  ","\n  ","") )
				.mkString( "\n" )
	}
	
	private def accumulateRequestStats(stats:scala.collection.mutable.Map[Long,RequestCounts], state:Tuple2[SCADSState,Long], rtype:String, threshold:Double): scala.collection.mutable.Map[Long,RequestCounts] = {
		if (threshold==50) 			stats(state._2) = stats.getOrElse(state._2,RequestCounts(0,0)).add( RequestCounts(state._1.metricsByType(rtype).nSlowerThan50ms,state._1.metricsByType(rtype).nRequests) )
		else if (threshold==100) 	stats(state._2) = stats.getOrElse(state._2,RequestCounts(0,0)).add( RequestCounts(state._1.metricsByType(rtype).nSlowerThan100ms,state._1.metricsByType(rtype).nRequests) )
		else RequestCounts(0,0)
		stats
	}
}

abstract class CostFunction {
	def cost(state:SCADSState):Double
	def detailedCost(state:SCADSState):Map[String,Double]
}

class TestCostFunction extends CostFunction {
	def cost(state:SCADSState):Double = Director.rnd.nextDouble
	def detailedCost(state:SCADSState):Map[String,Double] = Map("rnd"->Director.rnd.nextDouble)
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
	def optimize(state:SCADSState, actionExecutor:ActionExecutor)
	def overlaps(servers1:Set[String], servers2:Set[String]):Boolean = servers1.intersect(servers2).size !=0
}
/* TODO: (1) make loop finite even if cost doesn't decrease, (2) explore multiple branches at depth,
* (3) consider periods where cost goes up a little bit
*/
case class DepthOptimizer(depth:Int, coster:CostFunction, selector:ActionSelector) extends Optimizer {

	def optimize(state:SCADSState, actionExecutor:ActionExecutor) {
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
		actions.foreach(actionExecutor.addAction(_))
	}
}

case class HeuristicOptimizer(performanceEstimator:PerformanceEstimator, getSLA:Int, putSLA:Int) extends Optimizer {
	val slaPercentile = 0.99
	val max_replicas = 5
	val min_puts_allowed:Int = 100 	// percentage of allowed puts

	def optimize(state:SCADSState, actionExecutor:ActionExecutor) {
		var actions = new scala.collection.mutable.ListBuffer[Action]()
		val overloaded:Map[String,PerformanceStats] = getOverloadedServers(state)
		println("Have "+overloaded.size+" overloaded servers")

		// for overloaded servers, determine their mini ranges and replicas
		val overloaded_config = new SCADSconfig(Map(overloaded.keys.toList map {s => (s, state.config.storageNodes(s))} : _*))
		val overloaded_ranges = PerformanceEstimator.getServerHistogramRanges(overloaded_config,state.workloadHistogram)

		// create mapping of List[replicas] -> List[histogram ranges]
		val overloaded_ranges_replicas = Map[List[String],List[DirectorKeyRange]](overloaded_ranges.toList map {entry =>
			(state.config.getReplicas(entry._1), entry._2)
			} : _*)

		// try splitting/replicating the overloaded servers
		overloaded_ranges_replicas.foreach((entry)=>{
			println("Attempting to optimze overloaded server "+entry._1.first + " with "+entry._1.size + " replicas")
			val changes = trySplitting(entry._1, entry._2,state)
			actions.insertAll(actions.size,translateToActions(entry._1,changes,state))
			//actions += Remove(entry._1) // remove original server(s)
		})

		// pick a replica to remove or merge to do, if possible
		// TODO: how conservative should this choice be?
		val overloadedservers = overloaded_config.getNodes
		val candidatesMap = state.config.rangeNodes.toList.sort(_._1.minKey < _._1.minKey).filter(_._2.intersect(overloadedservers).size==0)
		println("Have "+candidatesMap.size+" underloaded servers")
		actions.insertAll(actions.size,scaleDown(candidatesMap.toArray,1,state)) // how many scale down actions to produce

		actions.foreach(actionExecutor.addAction(_))
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
	def estimateServerStats(state:SCADSState):Map[String,PerformanceStats] = {
		Map[String,PerformanceStats](state.config.storageNodes.toList map {
			entry => (entry._1, estimateSingleServerStats(entry._1, state.config.getReplicas(entry._1).size, 1.0,DirectorKeyRange(entry._2.minKey,entry._2.maxKey), state)) 
			} : _*)
	}
	/**
	* Estimate a single server's workload stats using the predicted workload histogram
	*/
	def estimateSingleServerStats(server:String, num_replicas:Int, allowed_puts:Double, range:DirectorKeyRange, state:SCADSState):PerformanceStats = {
		performanceEstimator.estimatePerformance(new SCADSconfig( Map[String,DirectorKeyRange](server -> range)),state.workloadHistogramPrediction.divide(num_replicas,allowed_puts),10,null) 
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
				//println(rangeArray(startId).minKey+" - "+rangeArray(endId).maxKey +", size("+(endId-startId)+") ok")
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

		while (!found && allowed >= min_puts_allowed) {
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
		var removalStart:Int = -1

		(0 until changesArray.size).foreach((index)=>{
			val change = changesArray(index)
			val start = if (index==0) {actual_range.minKey} else {Math.max(actual_range.minKey,change._1.minKey)}
			val end = if (index==(changesArray.size-1)) {actual_range.maxKey} else {Math.min(actual_range.maxKey,change._1.maxKey)}
			// if this is the first range of this server, don't replicate from, just removefrom later
			if (index==0) removalStart = end
			if (changes.size < 2) actions += ReplicateFrom(servers.first,DirectorKeyRange(start,end),change._2._1-1) // no splitting, TODO make more efficient
			else if (index > 0) actions += ReplicateFrom(servers.first,DirectorKeyRange(start,end),change._2._1)
		})
		if (changes.size > 1) {
			assert(removalStart != -1, "beginning of removal range should have been set")
			actions += RemoveFrom(servers,DirectorKeyRange(removalStart,actual_range.maxKey))
		}
		actions.toList
	}
	/**
	* Given array of list of replicas representing unoverloaded servers
	* and upper bound on number of scale down actions to produce,
	* return list of actions to perform
	*/
	def scaleDown(candidates:Array[(DirectorKeyRange,List[String])],num:Int,state:SCADSState):List[Action] = {
		var actions = new scala.collection.mutable.ListBuffer[Action]()
		var choices = candidates.indices // which indices of the candidates array still available to try

		while (actions.size < num && choices.size > 0) {
			val chosen_index = choices(Director.rnd.nextInt(choices.size)) // select radom index of candidate array
			val chosen = candidates(chosen_index)

			val chosen_neighbor = if (choices.size < 2) {null} else if (chosen_index < candidates.size-1){ candidates(chosen_index+1) } else { candidates(chosen_index-1) }
			if (chosen._2.size > 1) { // try to remove a replica
				if (!violatesSLA(estimateSingleServerStats(chosen._2.first,chosen._2.size-1,1.0,chosen._1, state)))
					actions += Remove(List[String](chosen._2.first))
				else println("Thought about removing replica "+chosen._2.first+", but didn't!")
			}
			else if (chosen._2.size==1 && chosen_neighbor !=null && chosen_neighbor._2.size==1) { // attempt merge two
				val range = if (chosen._1.minKey < chosen_neighbor._1.minKey) { DirectorKeyRange(chosen._1.minKey,chosen_neighbor._1.maxKey) } else { DirectorKeyRange(chosen_neighbor._1.minKey,chosen._1.maxKey) }
				if (!violatesSLA(estimateSingleServerStats(chosen._2.first,1,1.0,range, state)))
					actions += MergeTwo(chosen._2.first,chosen_neighbor._2.first)
				else println("Thought about merging "+chosen._2.first+" and "+chosen_neighbor._2.first+", but didn't!")
			}
			choices = choices.filter(_ != chosen_index)
		}
		actions.toList
	}
}
