package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.test._
import edu.berkeley.cs.scads.comm.Conversions._
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.util.Utf8
import scala.actors._
import scala.actors.Actor._
import org.apache.log4j.Logger
import org.apache.zookeeper.{ZooKeeper, Watcher, WatchedEvent, CreateMode, ZooDefs}

case class PolicyRange(val minKey:Int, val maxKey:Int) {
	def contains(needle:Int) = (needle >= minKey && needle < maxKey)
}

/**
* Abstract class that sets watches on partitions
* to get server lists and policies as they change
* I don't think it works with deleted stuff...
*/
abstract class PartitionWatcher(server:String) extends ZooKeeperProxy(server) {
	def logger:Logger
	val ns_pattern = java.util.regex.Pattern.compile("namespaces/(.+)/partitions")
	val partition_pattern = java.util.regex.Pattern.compile("partitions/(.+)/servers")
	
	initialize
	
	/**
	* Set up watches on existing namespaces and partitions
	*/
	def initialize:Unit = {
		val namespaces = root("scads/namespaces").updateChildren(true)
		if (!namespaces.isEmpty) {
			namespaces.foreach(ns=>{ // /scads/namespaces/ns
				val ns_children = ns._2.updateChildren(false)
				if (!ns_children.isEmpty)
					ns_children.foreach(ns_child => { // e.g. /scads/namespaces/ns/partitons
						val partitionsList = if (ns_child._1 == "partitions") ns_child._2.updateChildren(true) else null
						if (partitionsList != null)
							partitionsList.foreach(partition=> { // e.g. /scads/namespaces/ns/partitions/1
								val servers_and_policy = partition._2.updateChildren(true)
								if (servers_and_policy.contains("servers")) { 
									val servers = servers_and_policy("servers").updateChildren(true)
									actOnNewServers(partition._2.prefix+"servers",servers.map(s=>s._1).toList,ns._1,partition._1)
								} 
								if (servers_and_policy.contains("policy")) {
									val policy = servers_and_policy("policy").updateData(true)
									actOnNewPolicy(partition._2.prefix+"policy",policy,ns._1)
								}
							})
					})
			})
		}
	}
	/**
	* Watch for added namespaces and partitions
	* Set watches on the policy and servers
	*/
	override def process(event: WatchedEvent): Unit = {
		val etype = event.getType
	  if(event.getPath != null) {
			val path = event.getPath.substring(1)
			val parentpath = new java.io.File(path).getParent
			println(" -- event: "+path+", "+etype)
			
			if (
				path.endsWith("namespaces") || 
				parentpath.endsWith("namespaces") || 
				path.endsWith("partitions") ||
				parentpath.endsWith("partitions")
				) {
				val children = root(path).updateChildren(true)
				children.foreach(child => {
					if (child._1 == "policy") child._2.updateData(true)
					else child._2.updateChildren(true)
				})
			}
		
			else {
				// figure out namespace
				val m = ns_pattern.matcher(path)
				val ns:String = if (m.find) m.group(1) else null
				if (ns == null) { logger.warn("Couldn't identify namespace"); return }
				
				// if policy event, update the policy for all its servers, if there are any servers
				if (path.endsWith("policy")) {
					val policy = root(path).updateData(true)
					actOnNewPolicy(path,policy,ns)
				}
		
				// if servers event, update the policy and all the servers, since both could have changed
				if (path.endsWith("servers")) {
						val servers = root(path).updateChildren(true)
						val m = partition_pattern.matcher(path)
						val partition:String = if (m.find) m.group(1) else null
						actOnNewServers(path,servers.map(s=>s._1).toList,ns,partition)
				}
			}
			//super.process(event)
		}
	}
	def actOnNewPolicy(path:String,policy:Array[Byte],ns:String)
	def actOnNewServers(path:String,servers:List[String],ns:String,partition:String)
}


/**
* Partition watcher that'll hold mapping of ranges --> list of servers
* Meant for clients making requests to servers
*/
class ClientMapping(server:String) extends PartitionWatcher(server) {
	val logger = Logger.getLogger("client.mapping")
	val tempmapping = new scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,List[PolicyRange]]]()
	var mapping = Map[String,Map[PolicyRange,List[String]]]()
	
	def actOnNewPolicy(path:String,policy:Array[Byte],ns:String) = {
		val servers = root(path.replaceAll("/policy","/servers")).updateChildren(false)
		if (!servers.isEmpty) { // update the policy for all the servers
			//updateNodesInfo(ns,servers.map(s=>s._1).toList,parsePolicy(policy)) // TODO
			mapping = convertFromTemp // set the read-only version of the mapping
		}
	}
	def actOnNewServers(path:String,servers:List[String],ns:String,partition:String) = {
		val policy = root(path.replaceAll("/servers","/policy")).updateData(false)
		//updateNodesInfo(ns,servers,parsePolicy(policy)) //	TODO
		mapping = convertFromTemp // set the read-only version of the mapping
	}
	def updateNodesInfo(ns:String,servers:List[String],policy:List[PolicyRange]):Unit = {
		val entry = tempmapping.getOrElse(ns,new scala.collection.mutable.HashMap[String,List[PolicyRange]]())
		servers.foreach(server => entry(server) = policy)
		tempmapping(ns) = entry
	}
	
	//	TODO: change this to not use PolicyRange?
	def parsePolicy(policy:Array[Byte]):List[PolicyRange] = {
		val pdata = new PartitionedPolicy
		pdata.parse(policy)
		val iter = pdata.partitions.iterator
		val ranges = new scala.collection.mutable.ListBuffer[PolicyRange]()
		val (kmin,kmax) = (new IntRec,new IntRec)
	
		while (iter.hasNext) {
			val part = iter.next
			if (part.minKey!=null) kmin.parse(part.minKey)
			if (part.maxKey!=null) kmax.parse(part.maxKey)
			ranges += PolicyRange(kmin.f1,kmax.f1)
		}
		ranges.toList
	}
	
	def convertFromTemp:Map[String,Map[PolicyRange,List[String]]] = {
		var ranges:scala.collection.mutable.HashMap[PolicyRange,scala.collection.mutable.ListBuffer[String]] = null
		Map[String,Map[PolicyRange,List[String]]](
			tempmapping.keySet.toSeq.map(ns=>{
				ranges = new scala.collection.mutable.HashMap[PolicyRange,scala.collection.mutable.ListBuffer[String]]()
				tempmapping(ns).foreach(entry => entry._2.foreach(r=>{
					val cur=ranges.getOrElse(r, new scala.collection.mutable.ListBuffer[String]); cur+=entry._1; ranges(r)=cur
				}))
				(ns -> Map[PolicyRange,List[String]]( ranges.toSeq.map(entry=>(entry._1 -> entry._2.toList)):_* ) )
			}):_*)
	}
}

/**
* Partition watcher that'll hold mapping of which partition a server is in
* Meant for entities, e.g. the Director, that will frequently need to look up
* a particular server's policy
*/
class ClusterManipulation(server:String) extends PartitionWatcher(server) {
	val logger = Logger.getLogger("cluster.manipulation")
	
	val server_partition_map = new java.util.concurrent.ConcurrentHashMap[String,String]
	
	def actOnNewPolicy(path:String,policy:Array[Byte],ns:String) = { /*do nothing */ }
	def actOnNewServers(path:String,servers:List[String],ns:String,partition:String) = {
		if (partition == null) logger.warn("Can't update server to partition mapping without partition id")
		else {
			servers.foreach(server=> server_partition_map.put(server,partition))
		}
	}
}

trait ReadZooKeeperMapping {
	def root:ZooKeeperProxy#ZooKeeperNode
	val namespaces = root.get("namespaces")
	
	/**
	* Fetch the server responsibilities from the zookeeper
	* Returns mapping of  server hostname -> list of [range start, range end)
	*/
	def getServerRanges(namespace:String):Map[String,List[(SpecificRecordBase,SpecificRecordBase)]] = {
		// for each partition, get the policy and the servers responsible for that policy
		val partitions = namespaces.get(namespace+"/partitions").updateChildren(false)
		//var mapping = new scala.collection.mutable.
		Map[String,List[(SpecificRecordBase,SpecificRecordBase)]] (
			partitions.map(part=>{
				val (serverList,policyData) = getServersAndPolicy(namespace+"/partitions/"+part._1)
				val policy = parsePolicy(policyData)
				serverList.map(server=> (server,policy))
			}).toList.flatten(s=>s)
		:_*)
	}
	/**
	* Fetch the server responsibilities from the zookeeper
	* Returns mapping of [range start, range end) -> list of server hostnames
	*/
	def getRangeServers(namespace:String):Map[(SpecificRecordBase,SpecificRecordBase),List[String]] = {
		// for each partition, get the policy and the servers responsible for that policy
		val partitions = namespaces.get(namespace+"/partitions").updateChildren(false)
		Map[(SpecificRecordBase,SpecificRecordBase),List[String]] (
			partitions.map(part=>{
				val (serverList,policyData) = getServersAndPolicy(namespace+"/partitions/"+part._1)
				parsePolicy(policyData).map(range=> (range,serverList))
			}).toList.flatten(r=>r)
		:_*)
	}
	protected def getServersAndPolicy(path:String):(List[String],Array[Byte]) = {
		(
			namespaces.get(path+"/servers").updateChildren(false).toList.map(entry=>entry._1),
			namespaces.get(path+"/policy").updateData(false)
		)
	}
	protected def parsePolicy(policy:Array[Byte]):List[(SpecificRecordBase,SpecificRecordBase)] = {
		val pdata = new PartitionedPolicy
		pdata.parse(policy)
		val iter = pdata.partitions.iterator
		val ranges = new scala.collection.mutable.ListBuffer[(SpecificRecordBase,SpecificRecordBase)]()
		val (kmin,kmax) = (new IntRec,new IntRec)
	
		while (iter.hasNext) {
			val part = iter.next
			if (part.minKey!=null) kmin.parse(part.minKey)
			if (part.maxKey!=null) kmax.parse(part.maxKey)
			ranges += (kmin,kmax)
		}
		ranges.toList
	}
}

trait ClusterChanger {
	def root:ZooKeeperProxy#ZooKeeperNode
	def waitSeconds:Int
	val namespaces = root.get("namespaces")
	
	/**
	* Copy some ranges from a source server to a target server
	* Update zookeeper placement mapping such that target also has data
	*/
	def copy(src:String, target:String, ranges:List[(SpecificRecordBase,SpecificRecordBase)], recPerSec:Int) = {
		val copyrequest = new CopyRangesRequest
		copyrequest.destinationHost = target
		copyrequest.destinationPort = 9991
		copyrequest.ranges = rangesToKeyRanges(ranges)
		copyrequest.rateLimit = recPerSec
		
		// send actor with request and wait for response
		val copier = new CopyActor(RemoteNode(src,9991),copyrequest,5*1000)
		copier.start
		
		// once it's back, sync the two nodes with each other
		// TODO
		
		// get each server's policy. if target's policy will now be same as source, update its partition to be that of source. 
		// else just update its policy
		
		// wait until clients should have synced with placement
		Thread.sleep(waitSeconds)
	}
	def move(src:String, target:String, ranges:List[(SpecificRecordBase,SpecificRecordBase)], recPerSec:Int) = {
		// do copy and wait for response
		
		// sync the nodes with each other
		
		// get each server's current policy, make changes. assume won't "accidentally" make a replica
		// update both server's partition's policies
		
		// wait until clients should have synced with placement
		Thread.sleep(waitSeconds)
	}
	protected def rangesToKeyRanges(ranges: List[(SpecificRecordBase,SpecificRecordBase)]):List[KeyRange] = {
		ranges.map(range=>{
			val keyrange = new KeyRange
			keyrange.backwards = false
			keyrange.minKey = range._1.toBytes
			keyrange.maxKey = range._2.toBytes
			keyrange
		})
	}
}

class CopyActor(host:RemoteNode ,scads_req:Object, waitTime:Int) extends Actor {
	val logger = Logger.getLogger("scads.copy")
	var success = false
	var done = false
	
	def act = {
		val req = new Message
		req.body = scads_req // get or put request
		req.dest = new Utf8("Storage")
		val id = MessageHandler.registerActor(self)
		req.src = new java.lang.Long(id)
		makeRequest(req,id)
	}
	def makeRequest(req:Message,id:Long):Object = {
		// send the request
		MessageHandler.sendMessage(host, req) // send to source node
		// wait for response
		reactWithin(waitTime+1000) {
			case (RemoteNode(hostname, port), msg: Message) => msg.body match {
				case started:TransferStarted => { // confirmation of start, now wait for finish
					reactWithin(waitTime) {
						case (RemoteNode(hostname2, port2), msg2: Message) => msg2.body match {
							case fail:TransferFailed => { logger.warn("Transfer started but failed: "+fail); done=true; success=false } 
							case good:TransferSucceeded => { done=true; success=true }
						}
						case TIMEOUT => { logger.warn("Transfer started but timed out"); done=true; success=false; }
					}
					MessageHandler.unregisterActor(id)
				}
			}
			case TIMEOUT => { logger.warn("Transfer timed out"); done=true; success=false; MessageHandler.unregisterActor(id) }
			case msg => { logger.warn("Unexpected message: " + msg); done=true; success=false; MessageHandler.unregisterActor(id) }
		}
	}
}