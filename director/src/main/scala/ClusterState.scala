package edu.berkeley.cs.scads.director

import edu.berkeley.cs.scads.comm.{PartitionService,StorageService}
import net.lag.logging.Logger

object ClusterState {
	val pastServers = scala.collection.mutable.HashSet[String]()
	val pastPartitions = scala.collection.mutable.HashSet[String]()
	val nameSuffix = new java.util.concurrent.atomic.AtomicInteger()
	
	def getRandomServerNames(cfg:ClusterState,n:Int):List[StorageService] = {
	  val newNames = (0 until n).toList.map(i=> "!s"+ java.util.UUID.randomUUID.toString/*nameSuffix.getAndIncrement*/)
	  newNames.map( name=> new StorageService(name,1,null))
	}
	def getRandomPartitionNames(cfg:ClusterState,n:Int):List[PartitionService] = {
	  val newNames = (0 until n).toList.map(i=> "!p"+ java.util.UUID.randomUUID.toString/*nameSuffix.getAndIncrement*/)
	  newNames.map( name=> new PartitionService(name,0,null,null,null))
	}
	def getRandomServerNamesOld(cfg:ClusterState,n:Int):List[StorageService] = {
		//val rnd = new java.util.Random(7)

		val newNames = scala.collection.mutable.HashSet[String]()
		var name = ""
		for (i <- 1 to n) {
			var ps = pastServers.clone
			ps--=newNames
			if (cfg!=null) ps--=cfg.servers.map(s=>s.host)
			if (ps.size>0)
				name = ps.toList(0)
			else
				do {
					name = "s"+"%03d".format(Director.nextRndInt(999))
				} while ( (cfg!=null&&cfg.servers.map(s=>s.host).contains(name))||newNames.contains(name) )
			newNames+=name
			pastServers+=name
		}
		newNames.map( name=> new StorageService(name,1,null)).toList
	}
	def getRandomPartitionNamesOld(cfg:ClusterState,n:Int):List[PartitionService] = {
		val newNames = scala.collection.mutable.HashSet[String]()
		var name = ""
		for (i <- 1 to n) {
			var ps = pastPartitions.clone
			ps--=newNames
			if (cfg!=null) ps--=cfg.partitionsToKeys.keys.map(s=>s.host)
			if (ps.size>0)
				name = ps.toList(0)
			else
				do {
					name = "p"+"%03d".format(Director.nextRndInt(999))
				} while ( (cfg!=null&&cfg.partitionsToKeys.keys.toList.map(s=>s.host).contains(name))||newNames.contains(name) )
			newNames+=name
			pastPartitions+=name
		}
		newNames.map( name=> new PartitionService(name,0,null,null,null)).toList
	}
}

class ClusterState(
	val serversToPartitions:Map[StorageService,Set[PartitionService]], // server -> set(partition)
	val keysToPartitions:Map[Option[org.apache.avro.generic.GenericRecord], Set[PartitionService]], // startkey -> set(partition)
	val partitionsToKeys:Map[PartitionService,Option[org.apache.avro.generic.GenericRecord]], // partition -> startkey
	val partitionsToKeyLimit:Map[Option[org.apache.avro.generic.GenericRecord],Boolean], // startkey -> isKeySizeLimit (currently size set in ScadsState)
	val workloadRaw:WorkloadHistogram,
	val time:Long
) {
  val logger = Logger("clusterstate")
  //val wlKeys = workloadRaw.rangeStats.keySet
  keysToPartitions.keys.foreach(key => assert(workloadRaw.rangeStats.contains(key), "cluster state workload doesn't contain "+key))
  //assert(keysToPartitions.keys.sameElements(workloadRaw.rangeStats.keys), "cluster state must have same partitions as in the workload")
  
	def servers:Set[StorageService] = Set( serversToPartitions.keys.toList :_* )

	def partitionsOnServers(servers:List[StorageService]):Set[Option[org.apache.avro.generic.GenericRecord]] = 
		Set(serversToPartitions.filter(s=>servers.contains(s._1)).values.toList.flatten(r=>r).map(r=>partitionsToKeys(r)):_*)

	def partitionsWithMoreThanKReplicas(k:Int):Set[Option[org.apache.avro.generic.GenericRecord]] =
		Set(keysToPartitions.filter(entry => entry._2.size > k).keys.toList:_*)

  def partitionsWithLessThanKReplicas(k:Int):Set[Option[org.apache.avro.generic.GenericRecord]] =
		Set(keysToPartitions.filter(entry => entry._2.size < k).keys.toList:_*)

	def serversForKey(startkey:Option[org.apache.avro.generic.GenericRecord]):Set[StorageService] = 
		Set(serversToPartitions.filter(entry => entry._2.intersect(keysToPartitions(startkey)).size > 0).keys.toList:_*)
	
	def partitionOnServer(startkey:Option[org.apache.avro.generic.GenericRecord], server:StorageService):PartitionService = {
		val result = serversToPartitions(server).intersect(keysToPartitions(startkey))
		if (result.size == 1) logger.warning("have multiple partitions (%s) for same key (%s) on server %s",result,startkey,server)
		result.toList.head
	}
	
	def getEmptyServers():Set[StorageService] = Set(serversToPartitions.filter(entry => entry._2.size == 0).keySet.toList:_*)
	
	def replicate(part:PartitionService, server:StorageService):ClusterState = {
		val key = partitionsToKeys(part)
		val fakepart = ClusterState.getRandomPartitionNames(this,1).head
		val sToP = serversToPartitions(server) += fakepart
		val kToP = keysToPartitions(key) += fakepart
		val pToK = partitionsToKeys + (fakepart -> key)
		new ClusterState(sToP,kToP,pToK,partitionsToKeyLimit,workloadRaw,time)
	}
	
	def delete(part:PartitionService, server:StorageService):ClusterState = {
		val key = partitionsToKeys(part)
		val sToP = serversToPartitions(server) -= part
		val kToP = keysToPartitions(key) -= part
		val pToK = partitionsToKeys - part
		new ClusterState(sToP,kToP,pToK,partitionsToKeyLimit,workloadRaw,time)
	}
	
	def split(part:Option[org.apache.avro.generic.GenericRecord], numSplits:Int):ClusterState = {
	  //val key = partitionsToKeys(part)
		//val fakeparts = ClusterState.getRandomPartitionNames(this,numSplits)
		clone
	}
	
	def merge(part:Option[org.apache.avro.generic.GenericRecord]):ClusterState = {

		clone
	}
	
	def addServers(num:Int):(ClusterState,List[StorageService]) = {
		val newservers = ClusterState.getRandomServerNames(this,num)
		val sToP = serversToPartitions ++ Map( newservers.map(s => (s,Set[PartitionService]())):_* )
		(new ClusterState(sToP,
			Map(keysToPartitions.toList.map( p=> (p._1,p._2) ):_*),
			Map(partitionsToKeys.toList.map( p=> (p._1,p._2) ):_*),
			partitionsToKeyLimit,workloadRaw,time), newservers)
	}
	
	def addServer(newserver:StorageService):ClusterState = {
		val sToP = serversToPartitions ++ Map( List(newserver).map(s => (s,Set[PartitionService]())):_* )
		new ClusterState(sToP,
			Map(keysToPartitions.toList.map( p=> (p._1,p._2) ):_*),
			Map(partitionsToKeys.toList.map( p=> (p._1,p._2) ):_*),
			partitionsToKeyLimit,workloadRaw,time)
	}
	
	def removeServers(servers:List[StorageService]):ClusterState = {
		servers.foreach(s => assert(serversToPartitions(s).size == 0) ) // there should be no partitions left on these servers...
		new ClusterState(serversToPartitions -- servers,
			Map(keysToPartitions.toList.map( p=> (p._1,p._2) ):_*),
			Map(partitionsToKeys.toList.map( p=> (p._1,p._2) ):_*) ,
			partitionsToKeyLimit,workloadRaw,time)
	}
	
	// TODO: meaningless as immutable?
	override def clone():ClusterState = {
		new ClusterState(
			Map(serversToPartitions.toList.map( p=> (p._1,p._2) ):_*),
			Map(keysToPartitions.toList.map( p=> (p._1,p._2) ):_*),
			Map(partitionsToKeys.toList.map( p=> (p._1,p._2) ):_*) ,
			partitionsToKeyLimit,workloadRaw,time
		)
	}
	
	def serverWorkloadString:String = {
		serversToPartitions.map(entry =>{
			"server " + entry._1 + ":\n" + // server
			entry._2.map(part => {
				val key = partitionsToKeys(part)
				"\t"+part.toString +": "+ key+": "+workloadRaw.rangeStats(key).toString
			}).mkString("{","\n","}")
		}).mkString("ClusterState-------\n","\n","\n----------")
	}
	
	override def toString:String = serverWorkloadString
}
