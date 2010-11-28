package edu.berkeley.cs.scads.director

import edu.berkeley.cs.scads.comm.{PartitionService,StorageService}
import scala.collection.mutable.ListBuffer
import net.lag.logging.Logger

// TODO: rewrite classes
/*
StateUpdater
ActionExecutor
Actions
*/

// TODO: methods
/*
abstract policy.perform
policy.getNewServer
policy.handlePartitioning
policy.handleAllocation
ClusterState stuff
perfestimate.countkeysperserver
*/

// TODO: missing functionality
/*
count keys in partitions? used to estimate action time, and how many keys per server 
policy shouldn't block when add/remove actions are still running
policy.periodicUpdate(state), possibly in a new StateUpdater
ghost server names as storageservice? val test = new edu.berkeley.cs.scads.comm.StorageService("blah",1,null)
new decision: which actions to schedule now? break 'em down, and need to estimate data transfer speed
*/

abstract class Policy(
	val workloadPredictor:WorkloadPrediction
	//val costFunction:FullCostFunction
){
	protected val logger = Logger("policy")
	protected var lastDecisionTime:Long = 0L
	protected var clientRefreshTime = 30*1000L

	protected def act(state:ClusterState, actionExecutor:ActionExecutor)

	def stateColumns(): List[String] = List[String]()
	def stateValues(): List[String] = List[String]()

	val dbname = "director"
	val dbtable = "policystate"
	
	// var connection = Director.connectToDatabase
	// initialize

	def initialize {
		// Policy.initializeLogger
		// 
		// 		logger = Logger.getLogger("scads.director.policy")
		// 		val logPath = Director.basedir+"/policy.txt"
		// 		logger.removeAllAppenders
		// 		logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
		// 		logger.setLevel(DEBUG)
		// 
		// 		highlightLogger = Logger.getLogger("scads.director.policyHighlights")
		// 		val logPath2 = Director.basedir+"/policy_highlights.txt"
		// 		highlightLogger.removeAllAppenders
		// 		highlightLogger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath2,false) )
		// 		highlightLogger.setLevel(DEBUG)
		// 
		// 		connection = Director.connectToDatabase
		// 		createTable
		// 		Action.initDatabase
		workloadPredictor.initialize
	}

	def getParams:Map[String,String] = workloadPredictor.getParams

	// def periodicUpdate(state:ClusterState) {
	// 	workloadPredictor.addHistogram( state.workloadHistogram, state.time )
	// }

	def perform(state:ClusterState, actionExecutor:ActionExecutor) {
		//try {
			// Action.currentInitTime = new java.util.Date().getTime
			// 
			// 			// update the configuration of SCADS
			// 			val newConfig = try { SCADSconfig.getConfigFromPlacement( Director.namespace, Director.placementIP, Director.xtrace_on ) }
			// 							catch { case e:Exception => {Policy.logger.warn("POLICY: config from placement threw exception "+e.toString); null} }
			// 
			// 			// update state with the new config
			// 			var newState = if (newConfig==null || state==null) state
			// 							else SCADSState(state.time, newConfig, state.storageNodes, state.metrics, state.metricsByType, state.workloadHistogram, state.nkeysInRange)
			// 
			// 			if (state==null)
			// 				Policy.logger.warn("POLICY: have empty state (not running policy)")
			// 			else if (newState.config.hasGaps)
			// 				Policy.logger.warn("POLICY: state config has gaps (not running policy)\n"+newState.config.toString)
			// 			else {
			// 				Policy.logger.info("POLICY START ("+state.time+")")
			// 				Policy.logger.info("running policy with the following config:"+ (if (newState==null) " <null>" else "\n"+newState.config.toString) )
			// 				var workloadPerServer = PerformanceEstimator.estimateServerWorkload(newState.config,this.workloadPredictor.getPrediction)
			// 				Policy.logger.debug("smoothed workload per server:\n"+workloadPerServer.toList.sort(_._1<_._1).map( p => p._1+" => "+p._2.toString ).mkString("\n"))
			// 				Policy.logger.info("action executor status:\n"+actionExecutor.status)
			// 				act(newState,actionExecutor)
			if (state == null) { logger.warning("null state, not acting"); return }
			//if (System.currentTimeMillis < (lastDecisionTime + clientRefreshTime)) { logger.warning("state hasn't propgated fully"); return }
			logger.info("acting on state %s",new java.util.Date(state.time).toString)
			val lag = (System.currentTimeMillis-state.time)/1000
			logger.info("state is %s seconds behind",lag.toString)
			if (lag > 5*60) { logger.warning("state is way behind, not acting"); return }
			act(state,actionExecutor)
			
		// } catch {
		// 			case e:Exception => logger.warning("exception in policy.act: %s",e.toString)
		// 		}
		//storeState
	}

	def periodicUpdate(state:ClusterState) {
		workloadPredictor.addHistogram( state.workloadRaw, state.time )
	}

	override def toString:String = getParams.toList.sort(_._1<_._1).map( p => p._1+"="+p._2 ).mkString("",",","")
}

class EmptyPolicy(
	val test:Int,
	override val workloadPredictor:WorkloadPrediction
) extends Policy(workloadPredictor) {
	def act(config:ClusterState, actionExecutor:ActionExecutor) {
		logger.debug("test=%s",test)
		logger.debug("noop")
		val smoothedWorkload = workloadPredictor.getPrediction
		logger.debug("raw workload:\n%s",config.workloadRaw.toString)
		logger.debug("smoothed workload:\n%s",smoothedWorkload.toString)
	}
}

class BestFitPolicy(
	val performanceModel:PerformanceModel,
	val getSLA:Double,
	val putSLA:Double,
	val slaQuantile:Double,
	val blocking:Boolean,
	val machineInterval:Long,
	val serverRemoveTime:Long,
	//override val costFunction:FullCostFunction,
	override val workloadPredictor:WorkloadPrediction,
	val doReplication:Boolean,
	val doServerAllocation:Boolean,
	val nHotStandbys:Int,
	val reads:Int	
) extends Policy(workloadPredictor) {
	val performanceEstimator = ConstantThresholdPerformanceEstimator(1000,getSLA, putSLA, slaQuantile, reads)
	//val performanceEstimator = SimplePerformanceEstimator( performanceModel, getSLA, putSLA, slaQuantile, reads )
	
	var currentTime:Long = 0L
	var lastIterationState = ""
	var startingConfig:ClusterState = null
	//var startingState:SCADSState = null

	//// state of policy during a single pass
	var currentConfig:ClusterState = null
	var runningActions:List[Action] = null
	var actions:ListBuffer[Action] = null
	var ghostActions:ListBuffer[Action] = null
	var receivers:scala.collection.mutable.Set[StorageService] = null
	var ghosts:scala.collection.mutable.Set[StorageService] = null
	var serverReplicas:scala.collection.mutable.Set[StorageService] = null
	// temp place to coalesce per-range actions that are going to/from the same target/source pair
	var groupActions:scala.collection.mutable.HashMap[(StorageService,StorageService,String),ListBuffer[Action]] = null
	var groupGhostActions:scala.collection.mutable.HashMap[(StorageService,StorageService,String),ListBuffer[Action]] = null
	//var actionOrdering:scala.collection.mutable.HashMap[(String,String),Long] = null

	var workload:WorkloadHistogram = null

	val MIN_R = reads
	val MAX_R = 10

	// action notes
	val REP_CLEAN = "rep_clean"
	val REP_ADD = "rep_add"
	val REP_REDUCE = "rep_reduce"
	val MOVE_OVERLOAD = "move_overload"
	val MOVE_COALESCE = "move_coalesce"
	val SERVER_REMOVE = "server_remove"
	
	val maxKeysPerServer = 1000000
	
	def act(config:ClusterState, actionExecutor:ActionExecutor) {
		// TODO: grab when this workload information is from
		currentTime = config.time
		
		// do blocking execution
		if (actionExecutor.allActionsCompleted) {
			logger.info("smoothed workload:\n%s",workloadPredictor.getPrediction.toString)
			logger.info(config.serverWorkloadString)
			runPolicy( config, workloadPredictor.getPrediction, List[Action]() )
			logger.debug("# actions for executor: %d",actions.size)
			for (action <- actions) {
				//action.computeNBytesToCopy(state)
				actionExecutor.addAction(action)
			}
			if (!actions.isEmpty) lastDecisionTime = System.currentTimeMillis
			lastIterationState = "act"
		} else {
			lastIterationState = "blocked"
			logger.info("blocked on action completion")
		}
	}
	
	def runPolicy(_config:ClusterState, _workload:WorkloadHistogram, _runningActions:List[Action]) {
		logger.debug("starting runPolicy")
		startingConfig = _config.clone
		currentConfig = _config.clone
		workload = _workload
		
		// initialize state of policy
		runningActions = _runningActions
		actions = new ListBuffer[Action]()
		ghostActions = new ListBuffer[Action]()
		receivers = scala.collection.mutable.Set[StorageService]()
		ghosts = scala.collection.mutable.Set[StorageService]()
		serverReplicas = scala.collection.mutable.Set[StorageService]()
		groupActions = new scala.collection.mutable.HashMap[(StorageService,StorageService,String),ListBuffer[Action]]()
		groupGhostActions = new scala.collection.mutable.HashMap[(StorageService,StorageService,String),ListBuffer[Action]]()
		//actionOrdering = new scala.collection.mutable.HashMap[(String,String),Long]()
		
		// apply running actions to config?
		//applyRunningActions(runningActions)
		
		// STEP 1: add/remove replicas
		if (doReplication)
			handleReplication
			
		// STEP 2: move data
		handleDataMovement
		
		// STEP 3: add/remove servers
		handleServerAllocation
	}
	
	/**
	* Add replicas of overloaded ranges that can't be handled on a single server. Remove replicas that we don't need any more.
	* Make sure we don't ask for new servers if 'canAddRemoveServers==false'.
	* Put servers that handle replicas in 'serverReplicas' so we don't touch them in 'handleDataMovement'
	*/
	def handleReplication {
		
		logger.debug("**** REPLICATION stage")

		// step 1: increase replication
		// list of overloaded servers (sorted by workload, high to low)
		val violationsPerServer = performanceEstimator.perServerViolations(currentConfig,workload,getSLA,putSLA,slaQuantile)
 		val workloadPerServer = PerformanceEstimator.estimateServerWorkloadReads(currentConfig,workload,reads)
		val overloadedServers = violationsPerServer.filter( p => p._2 ).map( p => p._1 ).toList.sort( workloadPerServer(_)>workloadPerServer(_) )
		val binsOnOverloaded = currentConfig.partitionsOnServers(overloadedServers).toList
		
		// initialize server replicas
		val partitionsWithReplicas = currentConfig.partitionsWithMoreThanKReplicas(MIN_R).toList
		serverReplicas ++= (for (partition <- partitionsWithReplicas) yield currentConfig.serversForKey(partition).toList).flatten(r=>r)
		
		// take individual bins on overloaded servers, keep the ones that can't be handled on 1 server, and add more replicas to each
		val binsToReplicate = binsOnOverloaded.filter( b => !performanceEstimator.handleOnOneServer(workload.rangeStats(b),getSLA,putSLA,slaQuantile) )
		if (!binsToReplicate.isEmpty) {
			logger.debug("bins to replicate: "+binsToReplicate.mkString(", "))
			logger.debug("** increasing replication")
		}
		for (range <- binsToReplicate)
			replicate( range )
	
		// step 2: reduce replication
		// find ranges that have >MIN_R replica and try reducing replication
		val binsToReduce = currentConfig.partitionsWithMoreThanKReplicas(MIN_R) -- binsToReplicate
		if (!binsToReduce.isEmpty){
			logger.debug("** reducing replication")
			logger.debug("bins to reduce: "+binsToReduce.mkString(", "))
		}
		for (range <- binsToReduce ) {
			logger.debug("  reducing replication of "+range)
			var done = false
			var serversWithRange = currentConfig.serversForKey( range )
			
			while (serversWithRange.size>MIN_R && !done) {
				val server = serversWithRange.toList.first
				val action = DeletePartition(currentConfig.partitionOnServer(range,server), server, REP_REDUCE)//DeleteRange( range, server, REP_REDUCE )
				val tmpConfig = action.preview(currentConfig)
				if (!performanceEstimator.violationOnServers(tmpConfig, workload, serversWithRange) ) {
					addAction( action )
					serverReplicas -= server
					serversWithRange = currentConfig.serversForKey( range ) // get updated list of servers with this range
				} 
				else done = true
			}
		}
		logger.debug("serverReplicas: "+serverReplicas.mkString(", "))
	}
	
	/**
	* replicate 'range' until we can handle its workload
	* if necessary, range will be moved to an empty server from every server it's currently on
	*/
	def replicate( range:Option[org.apache.avro.generic.GenericData.Record] ) {
		var hadToCleanServers = false
		var serversWithRange = currentConfig.serversForKey( range )
		for (s <- serversWithRange) { // move range to empty server from each server the range is currently on
			val serverRanges = currentConfig.serversToPartitions(s)
			val partition = currentConfig.partitionOnServer(range,s)
			
			if (serverRanges.size > 1 || serverRanges.toList.head != partition) {
				hadToCleanServers = true
				logger.debug("cleaning server %s",s)
				
				val emptyServer = getEmptyServer(REP_CLEAN)
				if (emptyServer!=null) {
					val copyAction = ReplicatePartition(partition,s,emptyServer,REP_CLEAN) //MoveRanges(List(range),s,emptyServer,REP_CLEAN)
					val deleteAction = DeletePartition(partition,s,REP_CLEAN)
					serverReplicas += emptyServer

					// add the action as a ghost if the server is a ghost
					if (ghosts.contains(emptyServer)) { addGhostAction( copyAction ); addGhostAction( deleteAction ) }
					else { addAction( copyAction ); addAction( deleteAction ) }
				}
			}
			
		}
		
		serversWithRange = currentConfig.serversForKey( range )
		serverReplicas ++= serversWithRange
		val nReplicas = serversWithRange.size
		logger.debug("  after isolating range (if necessary), have %d replicas for %s",nReplicas,range.toString)
		val source = serversWithRange.toList.head
		var done = false
		// start adding replicas
		for (n <- (nReplicas+1) to MAX_R; if !done) {
			logger.debug("  trying %d replicas for %s",n,range.toString)

			val emptyServer = getEmptyServer(REP_ADD)
			if (emptyServer!=null) {
	 			val action = ReplicatePartition(currentConfig.partitionOnServer(range,source),source,emptyServer,REP_ADD)//CopyRanges(List(range),source,emptyServer,REP_ADD)
				serverReplicas += emptyServer
				// add the action as a ghost if the server is a ghost or I had to clean servers
				if (ghosts.contains(emptyServer) || hadToCleanServers) addGhostAction( action )
				else addAction( action )

				// if we replicate to this server, will there still be a violation? (i.e. have we spread the load enough)
				if (!performanceEstimator.violationOnServer(currentConfig,workload,emptyServer)) {
					done = true
					logger.debug("done")
				}
			}
		}

		if (!done)
			logger.warning("adding up to "+MAX_R+" replicas for range "+range+" didn't help!!")
		
	}
	
	/**
	* Move data from overloaded servers to less loaded servers and coalesce underutilized servers.
	* Make sure we don't move data to servers storing replicas.
	* Make sure we don't ask for new servers if 'canAddRemoveServers==false'
	*/
	def handleDataMovement {
		// take overloaded servers (except 'serverReplicas' -- these were handled before)
		// and move data to less loaded servers
		logger.debug("**** DATA MOVEMENT stage")
		
		// list of overloaded servers (sorted by workload, high to low) excluding 'serverReplicas'
		var workloadPerServer = PerformanceEstimator.estimateServerWorkloadReads(currentConfig,workload,reads)
		val violationsPerServer = performanceEstimator.perServerViolations(currentConfig,workload,getSLA,putSLA,slaQuantile)
		var overloadedServers = ( violationsPerServer.filter( p => p._2 ).map( p => p._1 ).toList -- serverReplicas.toList ).
										toList.
										sort( workloadPerServer(_)>workloadPerServer(_) )
		
		// step 1: handle overloaded servers
		if (!overloadedServers.isEmpty) logger.debug("** moving data from overloaded servers")
		for (server <- overloadedServers) {
			logger.debug("  fixing "+server)
			var continue = true
			//val histogramBins = workload.rangeStats.keys.toList
			//val ranges = List.flatten( currentConfig.getRangesForServer(server).toList.map( _.split(histogramBins) ) )
			val sortedRanges = currentConfig.partitionsOnServers(List(server)).toList.sort( workload.rangeStats(_).sum > workload.rangeStats(_).sum )
			for (range <- sortedRanges)
				if (continue) {
					if (!performanceEstimator.violationOnServer( currentConfig, workload, server ))
						// if no more violations, done with this server
						continue = false
					else
						// server still violates SLAs, keep moving ranges
						// 'move' returns 'true' on successful move.
						// TODO: should we stop if we couldn't move 'range'? there might be other ranges we could move
						continue = move( range, server )
				} // end if-continue
		}
		
		// step 2: try coalescing servers
		workloadPerServer = PerformanceEstimator.estimateServerWorkloadReads(currentConfig,workload,reads)

		// order merge candidates by increasing workload
		var mergeCandidates = (currentConfig.servers -- overloadedServers -- receivers -- serverReplicas).
									toList.sort( workloadPerServer(_)<workloadPerServer(_) )

		if (!mergeCandidates.isEmpty) logger.debug("** coalescing data")
		// get order of potential targets
		var orderedPotentialTargets = 
			currentConfig.servers.
				filter( s => currentConfig.serversToPartitions(s).size>0 && !ghosts.contains(s) && !serverReplicas.contains(s)).
				toList.
				sort( workloadPerServer(_)>workloadPerServer(_) )
		
		var minRangeCouldntMove:Option[org.apache.avro.generic.GenericData.Record] = null // track the range with smallest workload that we couldn't move, avoid trying to move something larger
		for (server <- mergeCandidates) {
			// only keep pontential targets that have higher workload than 'server'
			orderedPotentialTargets = orderedPotentialTargets.filter( workloadPerServer(_)>=workloadPerServer(server) )
			
			if (!overloadedServers.contains(server) && !receivers.contains(server)) {
				val ranges = currentConfig.partitionsOnServers(List(server))
				var successfulMove = false
				for (range <- ranges) {
					if (minRangeCouldntMove == null || workload.rangeStats(range).compare(workload.rangeStats(minRangeCouldntMove)) < 0) {
						logger.debug("  trying to move range "+range+": "+workload.rangeStats(range))
						successfulMove = tryMoving(range,server,orderedPotentialTargets)
						if (!successfulMove) minRangeCouldntMove = range
					}
				} // end for
			}
		} // end for
	}
	
	private def move( range:Option[org.apache.avro.generic.GenericData.Record], sourceServer:StorageService ):Boolean = {
		logger.debug("move("+range+","+sourceServer+")")
		
		// can't handle this range on even on server
		if ( !performanceEstimator.handleOnOneServer(workload.rangeStats(range),getSLA,putSLA,slaQuantile) ) { logger.debug("  can't handle this range on a single server, giving up"); false }
		
		// range is the only one on 'server', do nothing
		else if ( (currentConfig.serversToPartitions(sourceServer) -- Set(currentConfig.partitionOnServer(range,sourceServer))).isEmpty ) { logger.debug("  range is already alone on a server, giving up"); false }
		
		else {
			// get order of potential targets for move and try moving 'range' to them
			val workloadPerServer = PerformanceEstimator.estimateServerWorkloadReads(currentConfig,workload,reads)
			val violationsPerServer = performanceEstimator.perServerViolations(currentConfig,workload,getSLA,putSLA,slaQuantile)
			var orderedPotentialTargets =violationsPerServer.filter( p => !p._2 && !serverReplicas.contains(p._1) ).map( p => p._1 ).toList.sort( workloadPerServer(_)>workloadPerServer(_) )
			logger.debug("  orderedPotentialTargets = "+orderedPotentialTargets.mkString(","))
			
			// remove from potential targets servers that already have this range, and don't have too much data
			val serversWithRange = currentConfig.serversForKey(range)
			val serverKeyCount = 0//PerformanceEstimator.countKeysPerServer(startingState,currentConfig)
			orderedPotentialTargets = orderedPotentialTargets.filter(s => !serversWithRange.contains(s)/* && serverKeyCount(s) < maxKeysPerServer*/)
			logger.debug("  orderedPotentialTargets (filtered)= {%s}",orderedPotentialTargets.mkString(","))
			
			// try moving the range to one of the potential targets
			var moved = false
			var triedEmpty = false
			val part =  currentConfig.partitionOnServer(range,sourceServer)
			for (target <- orderedPotentialTargets) {
				if (!moved) {
					// try moving 'range' to 'target' and create new config
					val replicateAction = ReplicatePartition(part,sourceServer,target,MOVE_OVERLOAD)
					val deleteAction = DeletePartition(part,sourceServer,MOVE_OVERLOAD)
					var tmpConfig = replicateAction.preview(currentConfig)
					tmpConfig = deleteAction.preview(tmpConfig)

					// no violation on 'target' after the move, so we're done
					if (!performanceEstimator.violationOnServer(tmpConfig,workload,target)) {
						moved = true
						if (!ghosts.contains(target)) {
							addAction(replicateAction)
							addAction(deleteAction)
							receivers += target
						} else {
							addGhostAction(replicateAction)
							addGhostAction(deleteAction)
							receivers += target
						}

					// violation and 'range' was the only range on 'target' => need to replicate 'range'
					// this should only happen if we don't do replication, in which case we can't help anyway
					} else if ( tmpConfig.serversToPartitions(target).size==1 ) triedEmpty = true
				}
			} // end for
		
			// 'range' didn't fit on any server; try adding a new server
			if (!moved && !triedEmpty) {
				val newServer = getEmptyServer(MOVE_OVERLOAD)
				if (newServer!=null) {
					val replicateAction = ReplicatePartition(part,sourceServer,newServer,MOVE_OVERLOAD)
					val deleteAction = DeletePartition(part,sourceServer,MOVE_OVERLOAD)
					addGhostAction(replicateAction)
					addGhostAction(deleteAction)
					
					ghosts += newServer
					receivers += newServer
					moved = true

					if (performanceEstimator.violationOnServer(currentConfig,workload,newServer)) {
						// 'range' doesn't fit on the new server => need to replicate it
						// this should only happen if we don't do replication, in which case we can't help anyway
						logger.debug("  can't fit range on an empty server; I shouldn't be here")
					}
				}
			}
			moved
		}
		
	}
	private def tryMoving( range:Option[org.apache.avro.generic.GenericData.Record], sourceServer:StorageService, orderedPotentialTargets:List[StorageService] ):Boolean = {
		logger.debug("  tryMoving(%s from %s)",range.toString,sourceServer.toString)
		logger.debug("  orderedPotentialTargets = %s",orderedPotentialTargets.mkString(","))
		// filter out potential targets that already have this range, or have too much data to consume this range
		val serverKeyCount = 0//PerformanceEstimator.countKeysPerServer(startingState,currentConfig)
	
		val serversWithRange = currentConfig.serversForKey(range)
		val workloadPerServer = PerformanceEstimator.estimateServerWorkloadReads(currentConfig,workload,reads)
		val filtered_orderedPotentialTargets = orderedPotentialTargets.filter(s => !serversWithRange.contains(s) && workloadPerServer(s) >= workloadPerServer(sourceServer)/* && serverKeyCount(s) < maxKeysPerServer*/)
		logger.debug("  orderedPotentialTargets (filtered) = {%s}",filtered_orderedPotentialTargets.mkString(","))
		if (filtered_orderedPotentialTargets.contains(sourceServer)) logger.fatal("  orderedPotentialTargets (filtered) contains sourceServer")
		
		// try moving
		var moved = false
		val part =  currentConfig.partitionOnServer(range,sourceServer)
		for (target <- filtered_orderedPotentialTargets; if !moved && target!=sourceServer) {
			if (!moved && target!=sourceServer) {
				// try moving 'range' to 'target' and create new config
				val replicateAction = ReplicatePartition(part,sourceServer,target,MOVE_COALESCE)
				val deleteAction = DeletePartition(part,sourceServer,MOVE_COALESCE)
				var tmpConfig = replicateAction.preview(currentConfig)
				tmpConfig = deleteAction.preview(tmpConfig)

				if (!performanceEstimator.violationOnServer(tmpConfig,workload,target)) {
					// no violation on 'target' after the move, so we're done
					moved = true
					if (!ghosts.contains(target)) {
						addAction(replicateAction)
						addAction(deleteAction)
						receivers += target
					} else {
						addGhostAction(replicateAction)
						addGhostAction(deleteAction)
						receivers += target
					}
				}
			}
		}
		moved
	}

	/**
	* Return servers if we have more than we need.
	*/
	def handleServerAllocation {
		// count the number of empty servers in the 'currentConfig'
		// (this is the number we'll have after all actions execute, including ghosts)
		val nEmptyServers = currentConfig.getEmptyServers.size
		logger.debug("**** SERVER ALLOCATION stage ")
		logger.debug("** checking the size of hot-standby pool (will have "+nEmptyServers+" empty servers, want "+nHotStandbys+")")
		
		if (nEmptyServers < nHotStandbys) { // need to add more servers
			logger.debug("  adding %d hot-standby servers",(nHotStandbys - nEmptyServers))
			//addAction( AddServers(nHotStandbys - nEmptyServers,"server_allocation") )
			(nEmptyServers until nHotStandbys).toList.foreach(s=> addAction(AddServer(ClusterState.getRandomServerNames(currentConfig,1).head,"server_allocation")) )
		} 
		else if (nEmptyServers > nHotStandbys) { // can remove some servers, but need to make sure no action is using them
			// get servers that participate in any of the actions and ghost actions
			val participants = Set( (runningActions ++ actions.toList ++ ghostActions.toList).map(a => a.participants).flatten(p => p) :_*)//Set( List.flatten( (runningActions++actions.toList++ghostActions.toList).map(a => a.participants.toList) ).removeDuplicates :_* )
			logger.debug("  trying to remove some hot-standbys")
			logger.debug("  can't remove: "+participants.mkString(","))
			
			// candidates for removing = empty servers at the beginning - servers that participate in any actions
			val candidates = startingConfig.getEmptyServers -- participants
			logger.debug("  removal candidates: "+candidates.mkString(","))
			val serverRemoveCandidates = candidates.toList.take( nEmptyServers-nHotStandbys )
			val serversToRemove = new scala.collection.mutable.ListBuffer[StorageService]()
			for(s <- serverRemoveCandidates) {
				try {
					val bootupTime = Director.bootupTimes.getBootupTime(s).get
					val timeLeft = machineInterval - (currentTime-bootupTime)%machineInterval
					if (timeLeft < serverRemoveTime) {
						logger.debug("  server %s has %d seconds left. REMOVING",s,(timeLeft/1000))
						serversToRemove += s
					} else
						logger.debug("  server %s has %d seconds left. WAITING",s,(timeLeft/1000))
				} catch { case e => logger.warning("Couldn't get boot up time for %s: %s",s.toString,e.toString)}
			}
			if (serversToRemove.size > 0) addAction( RemoveServers( serversToRemove.toList, SERVER_REMOVE ) )
		} // end else if
	}
	
	private def addAction(action:Action) {
		logger.info("ADD ACTION: %s",action)
		currentConfig = action.preview(currentConfig)
		actions += action
	}
	private def addGhostAction(action:Action) {
		logger.debug("ADD GHOST ACTION: "+action)
		currentConfig = action.preview(currentConfig)
		ghostActions += action
	}
	private def getEmptyServer(note:String):StorageService = {
		var found = false
		var emptyServer:StorageService = null

		// try to find an empty server among the current servers
		for (server <- currentConfig.servers; if emptyServer==null)
			if (currentConfig.serversToPartitions(server).size==0)
				emptyServer = server

		if (emptyServer!=null)
			emptyServer
		else if (!doServerAllocation) {
			logger.debug("RAN OUT OF SERVERS")
			null
		} else {
			// don't have an empty server, so add one
			val addServerAction = AddServer(ClusterState.getRandomServerNames(currentConfig,1).head,note)//AddServers(1,note)
			addAction( addServerAction )
			emptyServer = addServerAction.participants.head//ClusterState.getRandomServerNames(currentConfig,1).first
			ghosts += emptyServer
			emptyServer
		}
	}
} // end policy class