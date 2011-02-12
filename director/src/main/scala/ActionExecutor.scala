package edu.berkeley.cs
package scads
package director

import storage.{GenericNamespace, ManagedScadsCluster, ScalaEngineTask}
import comm._
import deploylib.mesos._
import net.lag.logging.Logger

/**
* Run a thread that periodically checks for actions to execute, and executes them
*/
abstract class ActionExecutor(val execDelay:Long) extends Runnable {
	// TODO: turn this into ListBuffer so action manipulation is more efficient
	val actions = new scala.collection.mutable.SynchronizedQueue[Action]()//List[Action]()
	protected val logger = Logger("executor")
	var running = false
	val executorThread = new Thread(this, "ActionExecutor")
	def namespace:GenericNamespace
	
	initialize

	def addAction(action:Action) {
		// if ((action.isInstanceOf[MoveRanges]) || (action.isInstanceOf[CopyRanges])) {
		// 			Action.nDataActionsScheduled += 1
		// 			Action.nKeysScheduled += action.nKeysToCopy
		// 		}
		
		actions += action//actions = actions ::: List(action)
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
	def getRunningActions:Seq[Action] = actions.filter(_.running)
	def getAllActions:Seq[Action] = actions
	def getUncompleteServerActions:Seq[Action] = actions.filter(a=> a match { case r:AddServer => r.ready || r.running; case r:RemoveServers => r.ready || r.running; case _ => false })
	def allActionsCompleted():Boolean = if (actions.size==0) true else actions.forall(a => a.completed || a.cancelled)
	def allMovementActionsCompleted():Boolean = if (actions.size==0) true else actions.filter(a=> a match { case r:ReplicatePartition => true; case r:DeletePartition => true; case r:SplitPartition => true; case r:MergePartition => true; case _ => false }).forall(a => a.completed || a.cancelled)
	def partitionChangesRunning():Boolean = if (actions.size==0) false else !actions.filter(a=> a match { case r:SplitPartition => r.running; case r:MergePartition => r.running; case _ => false }).isEmpty
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
			  logger.debug("executor to run")
				execute()
				actions.dequeueAll(a => a.cancelled || a.completed)
			} catch {
				case e:Exception => {
				  logger.warning(e,"exception running director action, cancelling actions")
				  
			  }
			}
			//logger.debug("executor %s sleeping for: %d ms", Thread.currentThread.getName, execDelay )
			Thread.sleep(execDelay)
			logger.debug("executor waking up")
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
class GroupingExecutor(val namespace:GenericNamespace, val scheduler:RemoteActorProxy = null, override val execDelay:Long = 1000) extends ActionExecutor(execDelay) {
	def execute() = {
		if (actions.filter(a => a match { 			// only run if all previous Replicate or Delete actions finished
			case r:ReplicatePartition => r.running
			case d:DeletePartition => d.running
			case _ => false
			}).isEmpty) {
			// if have replicate actions, execute them, marking each action as started
			doReplication()

			// if have delete actions, execute them: assume this will run after completion of replication actions!
			doDeletion()
		
			// if have remove actions, turn off their storage services
			doRemove()
			
			//if have add actions, scheulde them with mesos, then wait for their appearance in the cluster
      doAdd()
		}
		
	}
	def getReplicateActions():Seq[Action] = actions.filter(a=> a match { case r:ReplicatePartition => r.ready; case _ => false } )
	
	def doReplication():Unit = {
	  val replicate = getReplicateActions()
	  try {
  		logger.debug("%d replicate actions",replicate.size)
  		if (!replicate.isEmpty) namespace.replicatePartitions( replicate.map(a=> 
  			a match { case r:ReplicatePartition =>{ r.setStart; (r.partition, r.target) } }))
  		replicate.foreach(a => a.setComplete)
  		
  	} catch { case e:RuntimeException => {
		  logger.warning("replicate failed, so need to cancel them")
		  replicate.foreach(a => a.cancel)
		}}
	}
	
	def getDeleteActions():Seq[Action] = actions.filter(a=> a match { case d:DeletePartition => d.ready; case _ => false } )
	
	def doDeletion():Unit = {
	  val deleteTotal = getDeleteActions()
		//logger.debug("%d delete actions",deleteTotal.size)
		val delete = deleteTotal.filter(_.parentsCompleted)
		deleteTotal.diff(delete).foreach(a => a.cancel)
		if (delete.size != deleteTotal.size) logger.warning("not doing all scheduled deletes since some had their replicates fail")
		if (!delete.isEmpty) namespace.deletePartitions( delete.map(a=> 
				a match { case d:DeletePartition =>{ d.setStart; d.partition } }))
		delete.foreach(a => a.setComplete) // TODO: check for errors
	}
	
	def getRemoveActions():Seq[Action] = actions.filter(a=> a match { case r:RemoveServers => r.ready; case _ => false } )
	
	def doRemove():Unit = {
	  val remove = getRemoveActions()
		logger.debug("%d removal actions",remove.size)
		if (!remove.isEmpty) remove.foreach(a => a match {  case r:RemoveServers => {r.setStart; r.servers.foreach( _ !! ShutdownStorageHandler()) }	})
		remove.foreach(a => a.setComplete) // TODO: was above call blocking? does it matter?
	}
	
	def getAddActions():Seq[Action] = actions.filter(a=> a match { case a:AddServer => a.ready; case _ => false } )
	
	def doAdd():Unit = {
	  val add = getAddActions()
		val prefix = if (namespace == null) "" else namespace.namespace
		logger.debug("%d add server actions",add.size)
		if (!add.isEmpty && scheduler != null) {
			//scheduler.addServers( add.map(a => a match{ case ad:AddServer => prefix+ad.fakeServer.host }) )
			scheduler !? RunExperimentRequest(add.toList.map(a => a match{ case ad:AddServer => serverProcess(prefix+ad.fakeServer.host) }) )
			//logger.debug("sleeping right now instead of waiting on child")
			//Thread.sleep(20*1000)
			add.foreach{ a => a match { 
				case ad:AddServer => { ad.setStart; /*namespace*/Director.cluster.root.awaitChild("availableServers/"+prefix+ad.fakeServer.host); logger.info("server %s should be available",prefix+ad.fakeServer.host); ad.setComplete } // TODO: stop waiting eventually
			} }
		}
	}
	
	//HACK
	implicit val classSource = MesosEC2.classSource
	private def serverProcess(name: String): JvmMainTask =
    ScalaEngineTask(clusterAddress = /*namespace*/Director.cluster.root.canonicalAddress,
		    name = Option(name)).toJvmTask
	
}

class SplittingGroupingExecutor(namespace:GenericNamespace, splitMaps:java.util.concurrent.LinkedBlockingQueue[(Option[org.apache.avro.generic.GenericRecord],Seq[Option[org.apache.avro.generic.GenericRecord]])] = null, mergeMaps:java.util.concurrent.LinkedBlockingQueue[(Seq[Option[org.apache.avro.generic.GenericRecord]],Option[org.apache.avro.generic.GenericRecord])] = null,sched:RemoteActorProxy = null) extends GroupingExecutor(namespace, sched) {
  override def execute() = {
    if (actions.filter(a => a match { 			// only run if all previous Replicate or Delete actions finished
			case r:ReplicatePartition => r.running
			case d:DeletePartition => d.running
			case s:SplitPartition => s.running
			case m:MergePartition => m.running
			case _ => false
			}).isEmpty) {
			  
			// if have partitions to split, do all them
			doSplit()
			doMerge()

			// if have replicate actions, execute them, marking each action as started
			doReplication()

			// if have delete actions, execute them: assume this will run after completion of replication actions!
			doDeletion()
		
			// if have remove actions, turn off their storage services
			doRemove()
			
			//if have add actions, scheulde them with mesos, then wait for their appearance in the cluster
      doAdd()
		  
		  logger.debug("executor done acting")
		}
		else logger.debug("executor can't run due to running actions")
  }
  
  def getSplitActions():Seq[Action] = actions.filter(a=> a match { case r:SplitPartition => r.ready; case _ => false } )
  
  def doSplit():Unit = {
    val splits = getSplitActions()
    logger.debug("%d split actions", splits.size)
    val splitMapTodo = new scala.collection.mutable.ListBuffer[(Option[org.apache.avro.generic.GenericRecord],Seq[Option[org.apache.avro.generic.GenericRecord]])]()
    
    
    // get split keys for each partiton to split, and make a combined list of all splits to perform
    try {
      if (!splits.isEmpty) namespace.splitPartition(
        splits.map(a => a match { case split:SplitPartition => { split.setStart; val keys = namespace.getSplitKeys(split.partition, split.parts); splitMapTodo.append((split.partition,keys.map(Some(_))))/*splitMaps.put((split.partition,keys.map(Some(_))))*/; keys} }).flatten(a=>a)
        )
      splits.foreach(a => a.setComplete) // TODO: check for errors  
      splitMapTodo.foreach(splitMaps.put(_))
    } catch { case e:Exception => { logger.warning(e,"splits failed, cancelling"); splits.foreach(a => a.cancel)}}
  } // end doSplit
  
  def getMergeActions():Seq[Action] = actions.filter(a=> a match { case r:MergePartition => r.ready; case _ => false } )
  
  def doMerge():Unit = {
    val merges = getMergeActions()
    logger.debug("%d merge actions", merges.size)
    //val mergeMapTodo = new scala.collection.mutable.ListBuffer[(Seq[Option[org.apache.avro.generic.GenericRecord]],Option[org.apache.avro.generic.GenericRecord])]()
    
    try {
      if (!merges.isEmpty) { val mergeBounds = namespace.mergePartitions(
        merges.map(a => a match { case merge:MergePartition => { merge.setStart; /*val keys = namespace.getMergeKeys(merge.partition); mergeMapTodo.append((Seq(keys._1, merge.partition), keys._1));*/ merge.partition.get } })
        )
       mergeBounds.foreach(e => mergeMaps.put( (Seq(e._1, e._2), e._1) )) 
      }
      merges.foreach(a => a.setComplete)
    } catch { case e:Exception => { logger.warning("merges failed, cancelling"); merges.foreach(a => a.cancel)}}
    //mergeMapTodo.foreach(mergeMaps.put(_))
  }
}

class TestGroupingExecutor(namespace:GenericNamespace,s:java.util.concurrent.LinkedBlockingQueue[(Option[org.apache.avro.generic.GenericRecord],Seq[Option[org.apache.avro.generic.GenericRecord]])], m:java.util.concurrent.LinkedBlockingQueue[(Seq[Option[org.apache.avro.generic.GenericRecord]],Option[org.apache.avro.generic.GenericRecord])]) extends SplittingGroupingExecutor(namespace,s,m,null) {
	//import edu.berkeley.cs.scads.storage.TestScalaEngine
	
	override def execute() {
		if (actions.filter(a => a match { 			// only run if all previous Replicate or Delete actions finished
			case r:ReplicatePartition => r.running
			case d:DeletePartition => d.running
			case s:SplitPartition => s.running
			case m:MergePartition => m.running
			case _ => false
			}).isEmpty) {
			// if have partitions to split, do all them
			doSplit()
			doMerge()
			  
			// if have replicate actions, execute them, marking each action as started
			/*val replicate = getReplicateActions()
			logger.debug("%d replicate actions",replicate.size)
			if (!replicate.isEmpty) namespace.replicatePartitions( replicate.map(a=> 
				a match { case r:ReplicatePartition =>{ r.setStart; (r.partition, r.target) } }))
			replicate.foreach(a => a.setComplete) // TODO: check for errors
      */
      doReplication()
			// if have delete actions, execute them: assume this will run after completion of replication actions!
			/*val delete = getDeleteActions()
			logger.debug("%d delete actions",delete.size)
			if (!delete.isEmpty) namespace.deletePartitions( delete.map(a=> 
					a match { case d:DeletePartition =>{ d.setStart; d.partition } }))
			delete.foreach(a => a.setComplete) // TODO: check for errors
			*/
			doDeletion()
		
			//if have add actions, create another test handler
			val add = getAddActions()
			logger.debug("%d add server actions",add.size)
			if (!add.isEmpty) {
			  Director.cluster match { case m:ManagedScadsCluster => {
  			  Thread.sleep(60*1000) // mimic real delay
  				add.foreach(a => { val servername = a match { case ad:AddServer => ad.fakeServer.host; case _ => "NoName" }; m.addNamedNode(namespace.namespace + servername); a.setComplete})
  			}
  			case _ => logger.warning("can't add servers")}
		  }
			
			// only say remove actions are complete, since can't really kill them
			val remove = getRemoveActions()
			logger.debug("%d removal actions",remove.size)
			remove.foreach(a => a.setComplete) 
		}
	}
}

/**
* TODO: executor that consults copy duration model to change how actions are grouped?
*/
