package edu.berkeley.cs.scads.director

import edu.berkeley.cs.scads.storage.{GenericNamespace,ScadsCluster}
import edu.berkeley.cs.scads.comm.{PartitionService,StorageService}
import net.lag.logging.Logger

object ScadsState {
	val logger = Logger("scadsstate")
	/*
	* get updated workload stats and cluster state some time after specified time-period
	* if there is no data for that time, return null
	*/
	def refreshAtTime(namespace:GenericNamespace, time:Long,period:Long):ClusterState = {
		logger.debug("refreshing state at time %s",time.toString)
		val workload = namespace.getWorkloadStats(time)
		if (workload !=null) { // assemble ClusterState object
			var workloadRaw = WorkloadHistogram( workload.map(wl => (wl._1,WorkloadFeatures(wl._2._1,wl._2._2,0))) )
			logger.debug("raw histogram:\n%s",workloadRaw.toString)
			workloadRaw = workloadRaw * (1.0/(period.toDouble/1000.0)) // TODO: awkward conversion
			logger.debug("raw histogram divided by period:\n%s",workloadRaw.toString)
			val kToP = Map( namespace.partitions.rTable.map(entry => (entry.startKey -> Set(entry.values:_*))):_* )
			
			// construct the server->partitions and partition->key mappings
			val sToP = new scala.collection.mutable.HashMap[StorageService,scala.collection.mutable.ListBuffer[PartitionService]]()
			val pToK = new scala.collection.mutable.HashMap[PartitionService,Option[org.apache.avro.generic.GenericData.Record]]()
			kToP.foreach(entry => // key -> set(partitions)
				entry._2.foreach(partition =>{
					pToK += (partition -> entry._1)
					val serverparts = sToP.getOrElse(partition.storageService,new scala.collection.mutable.ListBuffer[PartitionService]())
					serverparts += partition
					sToP(partition.storageService) = serverparts
				})
			)
			// attempt to get "empty" servers, i.e. have no partitions but registered with cluster
			// TODO: don't use available servers?!
			if (Director.cluster != null) {
				val existingServers = Director.cluster.getAvailableServers(/*namespace.namespace*/)
				logger.debug("existing servers: %d, servers with partitions: %d",existingServers.size,sToP.keySet.size)
				val blankServers = existingServers.filter(s => !sToP.keySet.contains(s))
				val now = new java.util.Date().getTime
				blankServers.foreach(s => {
					sToP(s) = new scala.collection.mutable.ListBuffer[PartitionService]()
					if (Director.bootupTimes.getBootupTime(s) == None) Director.bootupTimes.setBootupTime(s,now)
				})
			} else logger.warning("Director cluster null, not getting empty servers")
			
			new ClusterState(
				Map(sToP.toList.map(entry => (entry._1,Set(entry._2:_*))):_*),
				kToP,
				Map(pToK.toList.map(entry => (entry._1, entry._2)):_*),
				workloadRaw,time)
		}
		else null
	}
	def refresh(namespace:GenericNamespace, workloadRaw:WorkloadHistogram, time:Long):ClusterState = {
		// construct the server->partitions and partition->key mappings
		val kToP = Map( namespace.partitions.rTable.map(entry => (entry.startKey -> Set(entry.values:_*))):_* )
		val sToP = new scala.collection.mutable.HashMap[StorageService,scala.collection.mutable.ListBuffer[PartitionService]]()
		val pToK = new scala.collection.mutable.HashMap[PartitionService,Option[org.apache.avro.generic.GenericData.Record]]()
		kToP.foreach(entry => // key -> set(partitions)
			entry._2.foreach(partition =>{
				pToK += (partition -> entry._1)
				val serverparts = sToP.getOrElse(partition.storageService,new scala.collection.mutable.ListBuffer[PartitionService]())
				serverparts += partition
				sToP(partition.storageService) = serverparts
			})
		)
		// attempt to get "empty" servers, i.e. have no partitions but registered with cluster
		// TODO: don't use available servers?!
		if (Director.cluster != null) {
			val existingServers = Director.cluster.getAvailableServers(/*namespace.namespace*/)
			logger.debug("existing servers: %d, servers with partitions: %d",existingServers.size,sToP.keySet.size)
			val blankServers = existingServers.filter(s => !sToP.keySet.contains(s))
			val now = new java.util.Date().getTime
			blankServers.foreach(s => {
				sToP(s) = new scala.collection.mutable.ListBuffer[PartitionService]()
				if (Director.bootupTimes.getBootupTime(s) == None) Director.bootupTimes.setBootupTime(s,now)
			})
		} else logger.warning("Director cluster null, not getting empty servers")
		
		new ClusterState(
			Map(sToP.toList.map(entry => (entry._1,Set(entry._2:_*))):_*),
			kToP,
			Map(pToK.toList.map(entry => (entry._1, entry._2)):_*),
			workloadRaw,time)
	}
}

case class StateHistory(
	period:Long, // right now this assumes it is the same as what is set in partition handler!
	val namespace:GenericNamespace,
	policy:Policy
) {
	val logger = Logger("scadsstate")
	val history = new scala.collection.mutable.HashMap[Long,ClusterState] with scala.collection.mutable.SynchronizedMap[Long,ClusterState]
	var lastInterval:Long = -1
	var updaterThread:Thread = null
	val maxLag = 60*1000
	
	def getHistory():Map[Long,ClusterState] = Map( history.toList.map(entry => (entry._1,entry._2)):_* )
	def getMostRecentState():ClusterState = if (lastInterval== -1) null else history(lastInterval)
	
	def startUpdating {
		val updater = StateUpdater()
		updaterThread = new Thread(updater)
		updaterThread.start
	}

	def stopUpdating { if (updaterThread!=null) updaterThread.stop }
	
	case class StateUpdater() extends Runnable {
		def run() {
			var nextUpdateTime = new java.util.Date().getTime/period*period + period
				while(true) {
					val state = ScadsState.refreshAtTime(namespace,nextUpdateTime,period) // could be null if have no data for that time
					if (state != null) {
						//if (/*state is not well formed*/) logger.fatal("state is shmetted")
						history += nextUpdateTime -> state
						policy.periodicUpdate(state)
						lastInterval = nextUpdateTime
						nextUpdateTime += period
						logger.debug("next update: %s",nextUpdateTime.toString)
					}
					else { logger.debug("state null, sleeping") ;Thread.sleep(period/3)}
					
					if ( (new java.util.Date().getTime - nextUpdateTime) > maxLag ) nextUpdateTime += period
				} // end while true
		}
	}
}