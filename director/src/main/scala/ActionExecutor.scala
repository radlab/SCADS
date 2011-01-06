package edu.berkeley.cs.scads.director

import edu.berkeley.cs.scads.storage.{GenericNamespace, ManagedScadsCluster}
import edu.berkeley.cs.scads.comm._
import net.lag.logging.Logger

/**
* Run a thread that periodically checks for actions to execute, and executes them
*/
abstract class ActionExecutor(val execDelay:Long) extends Runnable {
	// TODO: turn this into ListBuffer so action manipulation is more efficient
	var actions = List[Action]()
	protected val logger = Logger("executor")
	var running = false
	val executorThread = new Thread(this, "ActionExecutor")
	
	initialize

	def addAction(action:Action) {
		// if ((action.isInstanceOf[MoveRanges]) || (action.isInstanceOf[CopyRanges])) {
		// 			Action.nDataActionsScheduled += 1
		// 			Action.nKeysScheduled += action.nKeysToCopy
		// 		}
		
		actions = actions ::: List(action)
		logger.info("adding action: "+action)
	}
	/**
	* remove any actions that aren't running (remove completed and ready)
	* returns list of actions that were canceled (the ready ones)
	*/
	// def cancelUnscheduledActions:List[Action] = {
	// 	val canceled = actions.filter(_.ready)
	// 	actions = actions.filter(_.running)
	// 
	// 	if (canceled.size==0) logger.debug("NOT CANCELLING ANY ACTIONS (none in 'ready' state)")
	// 	else logger.debug("CANCELLING ACTIONS: "+canceled.size/*mkString(", ")*/)
	// 
	// 	for (a <- canceled) {
	// 		a.cancel
	// 		Action.store(a)
	// 
	// 		if ((a.isInstanceOf[MoveRanges]) || (a.isInstanceOf[CopyRanges])) {
	// 			Action.nDataActionsCancelled += 1
	// 			Action.nKeysCancelled += a.nKeysToCopy
	// 		}
	// 	}
	// 
	// 	canceled
	// }
	def getRunningActions:List[Action] = actions.filter(_.running)
	def getAllActions:List[Action] = actions
	def allActionsCompleted():Boolean = if (actions.size==0) true else actions.forall(_.completed)
	def status:String = actions.map(a=> "%-10s".format("("+a.state+")") + "  " + a.toString).mkString("\n")

	/*
	* abstract methods
	*/
	def execute()

	def initialize() {
		running = true
		/*
		logger.removeAllAppenders
		logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),Director.basedir+"/actionexecutor.txt",false) )
		logger.setLevel(DEBUG)
		*/
	}

	/**
	* Look periodically and call execute()
	*/
	def run() = {
		while (running) {
			try {
				execute()
			} catch {
				case e:Exception => logger.warning("EXCEPTION: %s",e.toString)
			}
			Thread.sleep(execDelay)
		}
	}
	def start() = executorThread.start
	def stop() = { running=false }

}

/**
* Run all actions of the same type at the same time
* Only executes if all actions have completed
* Replication precedes Deletion precedes Removal
* TODO: where in order whould add/remove servers be run?
*/
class GroupingExecutor(namespace:GenericNamespace, val scheduler:ScadsServerScheduler = null, override val execDelay:Long = 1000) extends ActionExecutor(execDelay) {
	def execute() = {
		if (actions.filter(a => a match { 			// only run if all previous Replicate or Delete actions finished
			case r:ReplicatePartition => r.running
			case d:DeletePartition => d.running
			case _ => false
			}).isEmpty) {
			// if have replicate actions, execute them, marking each action as started
			val replicate = getReplicateActions()
			logger.debug("%d replicate actions",replicate.size)
			if (!replicate.isEmpty) namespace.replicatePartitions( replicate.map(a=> 
				a match { case r:ReplicatePartition =>{ r.setStart; (r.partition, r.target) } }))
			replicate.foreach(a => a.setComplete) // TODO: check for errors

			// if have delete actions, execute them: assume this will run after completion of replication actions!
			val delete = getDeleteActions()
			logger.debug("%d delete actions",delete.size)
			if (!delete.isEmpty) namespace.deletePartitions( delete.map(a=> 
					a match { case d:DeletePartition =>{ d.setStart; d.partition } }))
			delete.foreach(a => a.setComplete) // TODO: check for errors
		
			// if have remove actions, turn off their storage services
			val remove = getRemoveActions()
			logger.debug("%d removal actions",delete.size)
			if (!remove.isEmpty) delete.foreach(a => a match {  case r:RemoveServers => {r.setStart; r.servers.foreach( _ !! ShutdownStorageHandler()) }	})
			remove.foreach(a => a.setComplete) // TODO: was above call blocking? does it matter?
			
			//if have add actions, scheulde them with mesos, then wait for their appearance in the cluster
			val add = getAddActions()
			val prefix = if (namespace == null) "" else namespace.namespace
			logger.debug("%d add server actions",add.size)
			if (!add.isEmpty && scheduler != null) {
				scheduler.addServers( add.map(a => a match{ case ad:AddServer => prefix+ad.fakeServer.host }) )
				//logger.debug("sleeping right now instead of waiting on child")
				//Thread.sleep(20*1000)
				add.foreach{ a => a match { 
					case ad:AddServer => { scheduler.cluster.root.awaitChild("availableServers/"+prefix+ad.fakeServer.host); ad.setComplete }
				} }
			}
			
		}
		
	}
	def getReplicateActions():List[Action] = actions.filter(a=> a match { case r:ReplicatePartition => r.ready; case _ => false } )
	
	def getDeleteActions():List[Action] = actions.filter(a=> a match { case d:DeletePartition => d.ready; case _ => false } )
	
	def getRemoveActions():List[Action] = actions.filter(a=> a match { case r:RemoveServers => r.ready; case _ => false } )
	
	def getAddActions():List[Action] = actions.filter(a=> a match { case a:AddServer => a.ready; case _ => false } )
}

class TestGroupingExecutor(namespace:GenericNamespace) extends GroupingExecutor(namespace,null) {
	//import edu.berkeley.cs.scads.storage.TestScalaEngine
	
	override def execute() {
		if (actions.filter(a => a match { 			// only run if all previous Replicate or Delete actions finished
			case r:ReplicatePartition => r.running
			case d:DeletePartition => d.running
			case _ => false
			}).isEmpty) {
			// if have replicate actions, execute them, marking each action as started
			val replicate = getReplicateActions()
			logger.debug("%d replicate actions",replicate.size)
			if (!replicate.isEmpty) namespace.replicatePartitions( replicate.map(a=> 
				a match { case r:ReplicatePartition =>{ r.setStart; (r.partition, r.target) } }))
			replicate.foreach(a => a.setComplete) // TODO: check for errors

			// if have delete actions, execute them: assume this will run after completion of replication actions!
			val delete = getDeleteActions()
			logger.debug("%d delete actions",delete.size)
			if (!delete.isEmpty) namespace.deletePartitions( delete.map(a=> 
					a match { case d:DeletePartition =>{ d.setStart; d.partition } }))
			delete.foreach(a => a.setComplete) // TODO: check for errors
		
			//if have add actions, create another test handler
			val add = getAddActions()
			logger.debug("%d add server actions",add.size)
			Director.cluster match { case m:ManagedScadsCluster => {
				add.foreach(a => { val servername = a match { case ad:AddServer => ad.fakeServer.host; case _ => "NoName" }; m.addNamedNode(namespace.namespace + servername); a.setComplete})
			}
			case _ => logger.warning("can't add servers")}
			
			// only say remove actions are complete, since can't really kill them
			val remove = getRemoveActions()
			logger.debug("%d removal actions",delete.size)
			remove.foreach(a => a.setComplete) 
		}
	}
}

/**
* TODO: executor that consults copy duration model to change how actions are grouped?
*/
