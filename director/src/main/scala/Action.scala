package edu.berkeley.cs.scads.director

import edu.berkeley.cs.scads.comm.{PartitionService,StorageService}
import net.lag.logging.Logger
import java.util.Date

abstract class Action(
	val actionShortName:String,
	val note:String
) extends Runnable {
	object ActionState extends Enumeration("Ready","Running","Completed","Cancelled") {
	  type ActionState = Value
	  val Ready, Running, Completed, Cancelled = Value
	}
	import ActionState._
	
	//val logger = Logger.getLogger("scads.director.action."+actionShortName)
	//private val logPath = Director.basedir+"/actions/"+Director.dateFormatMillisecond.format(new Date)+"_"+actionShortName+".txt"
	
	protected val logger = Logger()
	val sleepTime = 0 // how long to sleep after performing an action, simulating clients' ttl on mapping
	var createsConsistentConfig = true // does this action depend on any others in order to be consistent
	//var initTime: Long = Director.director.policy.currentInterval
	var initTime: Long = Action.currentInitTime
	var startTime: Long = -1
	var endTime: Long = -1

	var _state = ActionState.Ready
	var successful = false
	def state():ActionState = _state
	def completed():Boolean = state()==ActionState.Completed
	def cancelled():Boolean = state()==ActionState.Cancelled
	def running():Boolean = state()==ActionState.Running
	def ready():Boolean = state()==ActionState.Ready

	def setSuccess(succ:Boolean):Unit = { successful = succ }
	def cancel { _state=ActionState.Cancelled }
	def complete { _state=ActionState.Completed }

	var executionThread: Thread = null

	//var dbID: Int = -1
	//try { Action.store(this) } catch { case e:Exception => Director.summaryLogger.warn("Couldn't store action "+this.toString) }

	val parents = new scala.collection.mutable.ListBuffer[Action]()

	def addParent(action:Action) { parents += action }
	def addParents(actions:List[Action]) { parents.insertAll(parents.size,actions) }
	def parentsCompleted():Boolean = if (parents.size==0) true else parents.forall(_.completed)

	def startExecuting() {
		//if (Director.LOG_ACTIONS) logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
		/*if (Director.LOG_ACTIONS) {
			logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
			logger.setLevel(DEBUG)
		} else logger.setLevel(OFF)
		*/
		//Action.store(this)
		executionThread = new Thread(this)
		try {
			executionThread.start
		} catch { case e => logger.warning("Exception executing action "+actionShortName)}
	}
	//def updateConfigAndActivity(time0:Long, time1:Long, config:SCADSconfig, activity:SCADSActivity):Tuple2[SCADSconfig,SCADSActivity] = { this.complete; (this.preview(config),activity) }

	def csvArgs():String = ""
	
	// not used
	override def run() {
		_state = ActionState.Running
		logger.info("starting action execution")
		startTime = new Date().getTime
		//Action.store(this)
		try { execute() } 
		catch { case e => {
		 	val sw = new java.io.StringWriter()
			val pw = new java.io.PrintWriter(sw, true)
			e.printStackTrace(pw)
			pw.flush(); sw.flush()
			//Policy.logger.warn("Action execution failure: "+sw.toString)
			}
		}
		_state = ActionState.Completed
		endTime = new Date().getTime
		logger.info("action execution completed: "+successful)
		//Policy.logger.info("done executing "+toString)
		//Director.summaryLogger.info("done executing "+toString)
		//Action.store(this)
	}

	override def toString(): String = { actionShortName+"#"+note }

	// not used
	def execute():Unit = {}
	def setStart:Unit = { startTime = new Date().getTime; _state = ActionState.Running }
	def setComplete:Unit = { endTime = new Date().getTime; _state = ActionState.Completed }
	def preview(config:ClusterState):ClusterState
	def participants: Set[StorageService]
	
	// number of bytes of data the action has to copy (this should probably include sync)
	def nBytesToCopy(): Long = -1L
	def nKeysToCopy(): Long = -1L
}

object Action {
	var currentInitTime = -1L
}

case class ReplicatePartition(val partition:PartitionService, val source:StorageService, val target:StorageService, override val note:String)
	extends Action("Replicate("+partition.toString+") to "+ target.toString,note) {
		
	override def preview(config:ClusterState):ClusterState = config.replicate(partition,target)
	
	def participants:Set[StorageService] = Set(source,target)	
}

case class DeletePartition(val partition:PartitionService, val source:StorageService, override val note:String)
	extends Action("Delete("+partition.toString+") from "+ source.toString,note) {
		
	override def preview(config:ClusterState):ClusterState = config.delete(partition,source)
	
	def participants:Set[StorageService] = Set(source)	
}

case class SplitPartition(val partition:Option[org.apache.avro.generic.GenericRecord], val parts:Int, override val note:String)
  extends Action("Split("+partition.toString+") into "+parts+"parts ",note) {
    
  override def preview(config:ClusterState):ClusterState = config.split(partition, parts)

  def participants:Set[StorageService] = Set() // TODO: get list of servers that this partition is on
}

case class MergePartition(val partition:Option[org.apache.avro.generic.GenericRecord], override val note:String)
  extends Action("Merge("+partition.toString+")",note) {
    
  override def preview(config:ClusterState):ClusterState = config.merge(partition)

  def participants:Set[StorageService] = Set() // TODO: get list of servers that this partition is on
}

@deprecated
case class AddServers(nServers:Int,override val note:String) extends Action("AddServers("+nServers+")",note) {
	var servers:List[StorageService] = null
	
	override def preview(config:ClusterState):ClusterState = {
		val (newstate,newservers) = config.addServers(nServers)
		servers = newservers
		newstate
	}
	override def setComplete:Unit = {
		val now = new java.util.Date().getTime
		servers.foreach( s => Director.bootupTimes.setBootupTime(s,now))
		super.setComplete
	}
	
	def setServers(newservers:List[StorageService]) = servers = newservers
	
	def participants:Set[StorageService] = Set(servers:_*)
}

case class AddServer(fakeServer:StorageService, override val note:String) extends Action("AddServer("+fakeServer+")",note) {
	
	override def preview(config:ClusterState):ClusterState = {
		val newstate = config.addServer(fakeServer)
		newstate
	}
	override def setComplete:Unit = {
		//val now = new java.util.Date().getTime
		//servers.foreach( s => Director.bootupTimes.setBootupTime(s,now))
		super.setComplete
	}
	
	
	def participants:Set[StorageService] = Set(fakeServer)
}


case class RemoveServers(servers:List[StorageService],override val note:String) extends Action("RemoveServers("+servers.mkString(",")+")",note) {
	
	override def preview(config:ClusterState):ClusterState = config.removeServers(servers)
	override def setComplete:Unit = {
		servers.foreach((server)=> Director.bootupTimes.removeBootupTime(server) )
		super.setComplete
	}
	
	def participants:Set[StorageService] = Set(servers:_*)
}

class BootupTimes {
	// keep track of when the servers were booted up
	val bootupTimes = scala.collection.mutable.Map[StorageService,Long]()

	def setBootupTime(server:StorageService, time:Long) { bootupTimes += server -> time }
	def getBootupTime(server:StorageService):Option[Long] = {
		if (bootupTimes.contains(server)) Some(bootupTimes(server))
		else None
	}
	def removeBootupTime(server:StorageService) { bootupTimes -= server }
	def reset { bootupTimes.clear }
}