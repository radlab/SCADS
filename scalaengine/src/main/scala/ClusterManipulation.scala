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

case class IntegerPolicyRange(val minKey:Int, val maxKey:Int) {
	def contains(needle:Int) = (needle >= minKey && needle < maxKey)
}
trait PolicyParse {
	/**
	* Assumes elements of ranges in policy are IntRec, even though
	* return type is SpecificRecordBase
	* May return single item list that is (0,0), signifying EmptyRange
	*/
	def parsePolicy(policy:Array[Byte]):List[(SpecificRecordBase,SpecificRecordBase)] = {
		val pdata = new PartitionedPolicy
		pdata.parse(policy)
		val iter = pdata.partitions.iterator
		val ranges = new scala.collection.mutable.ListBuffer[(SpecificRecordBase,SpecificRecordBase)]()
		
		while (iter.hasNext) {
			val (kmin,kmax) = (new IntRec,new IntRec)
			kmin.f1 = Integer.MIN_VALUE; kmax.f1 = Integer.MAX_VALUE
			val part = iter.next
			if (part.minKey!=null) kmin.parse(part.minKey)
			if (part.maxKey!=null) kmax.parse(part.maxKey)
			ranges += (kmin,kmax)
		}
		ranges.toList
	}
	def parseIntegerPolicy(policy:Array[Byte]):List[IntegerPolicyRange] = {
			val pdata = new PartitionedPolicy
			pdata.parse(policy)
			val iter = pdata.partitions.iterator
			val ranges = new scala.collection.mutable.ListBuffer[IntegerPolicyRange]()
			val (kmin,kmax) = (new IntRec,new IntRec)
			kmin.f1 = Integer.MIN_VALUE; kmax.f1 = Integer.MAX_VALUE

			while (iter.hasNext) {
				val part = iter.next
				if (part.minKey!=null) kmin.parse(part.minKey)
				if (part.maxKey!=null) kmax.parse(part.maxKey)
				ranges += IntegerPolicyRange(kmin.f1,kmax.f1)
			}
			ranges.toList
		}
}

/**
* Abstract class that sets watches on partitions
* to get server lists and policies as they change
* I don't think it works with deleted stuff...
*/
abstract class PartitionWatcher(server:String) extends ZooKeeperProxy(server) with PolicyParse {
	def logger:Logger
	val ns_pattern = java.util.regex.Pattern.compile("namespaces/(.+)/partitions")
	val partition_pattern = java.util.regex.Pattern.compile("partitions/(.+)/servers")
	
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
									actOnNewServers(partition._2.prefix.substring(1)+"servers",servers.map(s=>s._1).toList,ns._1,partition._1)
								} 
								if (servers_and_policy.contains("policy")) {
									val policy = servers_and_policy("policy").updateData(true)
									actOnNewPolicy(partition._2.prefix.substring(1)+"policy",policy,ns._1)
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
class ClientMapping(server:String) extends PartitionWatcher(server) with ReadZooKeeperMapping {
	val logger = Logger.getLogger("client.mapping")
	val tempmapping = new scala.collection.mutable.HashMap[String,scala.collection.mutable.HashMap[String,List[IntegerPolicyRange]]]()
	var mapping = Map[String,Map[IntegerPolicyRange,List[String]]]()
	
	initialize
	
	def locate(namespace:String, needle:Int):List[String] = {
		val ns_map = mapping.getOrElse(namespace,null)
		if (ns_map != null) {
			val potentials = ns_map.filter(e=>e._1.contains(needle)).toList // should only be one entry in map since ranges don't overlap!
			if (!potentials.isEmpty) potentials(0)._2
			else { println("Couldn't find key "+needle); logger.warn("Couldn't find any servers for key");List[String]() } // TODO: warning? exception?
		}
		else { println("Couldn't find key "+needle); logger.warn("Couldn't find any servers for key");List[String]() } // TODO: warning? exception?
	}
	
	def actOnNewPolicy(path:String,policy:Array[Byte],ns:String) = {
		val servers = root(path.replaceAll("/policy","/servers")).updateChildren(false)
		if (!servers.isEmpty) { // update the policy for all the servers
			updateNodesInfo(ns,servers.map(s=>s._1).toList,parseIntegerPolicy(policy))
			//mapping = convertFromTemp // set the read-only version of the mapping
		}
	}
	def actOnNewServers(path:String,servers:List[String],ns:String,partition:String) = {
		val policy = root(path.replaceAll("/servers","/policy")).updateData(false)
		updateNodesInfo(ns,servers,parseIntegerPolicy(policy))
		//mapping = convertFromTemp // set the read-only version of the mapping
	}
	/**
	* I don't think this protects against if the server partition changed multiple times before the watch was reset?!
	*/
	def updateNodesInfo(ns:String,servers:List[String],policy:List[IntegerPolicyRange]):Unit = {
		/*val entry = tempmapping.getOrElse(ns,new scala.collection.mutable.HashMap[String,List[IntegerPolicyRange]]())
		//val entry = new scala.collection.mutable.HashMap[String,List[IntegerPolicyRange]]()
		servers.foreach(server => entry(server) = policy)
		tempmapping(ns) = entry
		*/
		val startt = System.nanoTime
		var temp_ns = Map[IntegerPolicyRange,List[String]]( getRangeServers(ns).toList.map(entry=> {
			entry._1 match {
				case (e1:IntRec, e2:IntRec) => (IntegerPolicyRange(e1.f1, e2.f1), entry._2)
				case e => { logger.warn("Invalid type from getRangeServers()");null }
			}
		}):_*)
		
		synchronized { // set the read-only version of the mapping
			mapping = mapping.update(ns,temp_ns)
		}
		logger.info("total map convert time: "+((System.nanoTime-startt)/1000000.0)+" ms")
	}
	
	def convertFromTemp:Map[String,Map[IntegerPolicyRange,List[String]]] = {
		val startt = System.nanoTime
		var ranges:scala.collection.mutable.HashMap[IntegerPolicyRange,scala.collection.mutable.ListBuffer[String]] = null
		val ret = Map[String,Map[IntegerPolicyRange,List[String]]](
			tempmapping.keySet.toSeq.map(ns=>{ // get mapping for each namespace
				ranges = new scala.collection.mutable.HashMap[IntegerPolicyRange,scala.collection.mutable.ListBuffer[String]]()
				tempmapping(ns).foreach(entry => {val startserver = System.nanoTime; entry._2.foreach(r=>{ // for each server, for each range
					val cur = ranges.getOrElse(r, new scala.collection.mutable.ListBuffer[String]); cur+=entry._1; ranges(r) = cur
					})
					println("single server convert time: "+((System.nanoTime-startserver)/1000000.0)+" ms")
				})
				(ns -> Map[IntegerPolicyRange,List[String]]( ranges.toSeq.map(entry=>(entry._1 -> entry._2.toList)):_* ) )
			}):_*)
		println("total temp map convert time: "+((System.nanoTime-startt)/1000000.0)+" ms")
		ret
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
	
	initialize
	
	def actOnNewPolicy(path:String,policy:Array[Byte],ns:String) = { /*do nothing */ }
	def actOnNewServers(path:String,servers:List[String],ns:String,partition:String) = {
		if (partition == null) logger.warn("Can't update server to partition mapping without partition id")
		else {
			servers.foreach(server=> server_partition_map.put(server,partition))
		}
	}
}

class DirectorManipulation(server:String) extends ClusterManipulation(server) with ClusterChanger with ReadZooKeeperMapping { val waitSeconds = 3 }

trait ReadZooKeeperMapping extends PolicyParse {
	def root:ZooKeeperProxy#ZooKeeperNode
	var namespaces = root.get("scads/namespaces")
	
	/**
	* Fetch the server responsibilities from the zookeeper
	* Returns mapping of  server hostname -> list of [range start, range end)
	*/
	def getServerRanges(namespace:String):Map[String,List[(SpecificRecordBase,SpecificRecordBase)]] = {
		// for each partition, get the policy and the servers responsible for that policy
		val partitions = namespaces.get(namespace+"/partitions").children

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
	* Doesn't remove ranges that have no associated servers
	*/
	def getRangeServers(namespace:String):Map[(SpecificRecordBase,SpecificRecordBase),List[String]] = {
		// for each partition, get the policy and the servers responsible for that policy
		val partitions = namespaces.get(namespace+"/partitions").children
		Map[(SpecificRecordBase,SpecificRecordBase),List[String]] (
			partitions.map(part=>{
				val (serverList,policyData) = getServersAndPolicy(namespace+"/partitions/"+part._1)
				parsePolicy(policyData).map(range=> (range,serverList))
			}).toList.flatten(r=>r)
		:_*)
	}
	protected def getServersAndPolicy(path:String):(List[String],Array[Byte]) = {
		(
			namespaces.get(path+"/servers").children.toList.map(entry=>entry._1),
			namespaces.get(path+"/policy").data
		)
	}
}

trait ClusterChanger {
	def logger:Logger
	def root:ZooKeeperProxy#ZooKeeperNode
	def namespaces:ZooKeeperProxy#ZooKeeperNode
	def waitSeconds:Int
	def server_partition_map:java.util.concurrent.ConcurrentHashMap[String,String]
	def parsePolicy(policy:Array[Byte]):List[(SpecificRecordBase,SpecificRecordBase)]
	
	val EMPTY = "empty"
	val FAKE = "fake"
	
	/**
	* Copy some ranges from a source server to a target server
	* Update zookeeper placement mapping such that target also has data
	* Assumes one thread at a time is manipulating either of these two servers
	*/
	def copy(ns:String,src:String, target:String, ranges:List[(SpecificRecordBase,SpecificRecordBase)]):Unit = copy(ns,src,target,ranges,0)
	def copy(ns:String, src:String, target:String, ranges:List[(SpecificRecordBase,SpecificRecordBase)], recPerSec:Int):Unit = {
		val success = doCopy(ns,src,target,ranges,recPerSec,true)
		if (!success) logger.warn("Failed to copy")
		
		// wait until clients should have synced with placement
		Thread.sleep(waitSeconds)
	}
	/**
	* Move some ranges from a source server to a target server
	* Update zookeeper placement mapping such that target has data and src does not
	* Assumes one thread at a time is manipulating either of these two servers
	*/
	def move(ns:String, src:String, target:String, ranges:List[(SpecificRecordBase,SpecificRecordBase)]):Unit = move(ns,src,target,ranges,0)
	def move(ns:String, src:String, target:String, ranges:List[(SpecificRecordBase,SpecificRecordBase)], recPerSec:Int):Unit = {
		val startt = System.currentTimeMillis
		// copy data and change target policy to have new data
		val success = doCopy(ns,src,target,ranges,recPerSec,false)
		if (!success) logger.warn("Failed to copy for move")
		else {
			// make changes to src's policy
			// doesn't check if this action will make it the replica of an existing server
			val src_policy = getServerPolicy(ns,src)
			var src_policy_new = src_policy
			ranges.foreach(range => src_policy_new = RangeManipulation.removeRange(range,src_policy_new))
			setServerPolicy(ns,src,src_policy_new)
			
			// remove data
			// TODO
		}
		
		// wait until clients should have synced with placement
		Thread.sleep(waitSeconds)
	}
	def remove(ns:String, src:String, range:(SpecificRecordBase,SpecificRecordBase)):Unit = {
		val src_policy = getServerPolicy(ns,src)
		var src_policy_new = RangeManipulation.removeRange(range,src_policy)
		
		// change the policy, which will move it to the EmptyRange partition if no more data left
		setServerPolicy(ns,src,src_policy_new)
		
		// remove data
		// TODO
		
		// wait until clients should have synced with placement
		Thread.sleep(waitSeconds)
	}
	def addStandby(ns:String,server:String):Unit = {
		assert(server_partition_map.get(server)==FAKE,"Must add standby from \"fake\" partition")
		namespaces.get(ns+"/partitions/"+FAKE+"/servers").deleteChild(server)
		namespaces.get(ns+"/partitions/"+EMPTY+"/servers").createChild(server, "", CreateMode.PERSISTENT)
		
		// wait until clients should have synced with placement
		Thread.sleep(waitSeconds)
	}
	def removeStandby(ns:String,server:String):Unit = {
		assert(server_partition_map.get(server)==EMPTY, "Can only remove standby from \"empty\" partition")
		namespaces.get(ns+"/partitions/"+EMPTY+"/servers").deleteChild(server)
		namespaces.get(ns+"/partitions/"+FAKE+"/servers").createChild(server, "", CreateMode.PERSISTENT)
		
		// wait until clients should have synced with placement
		Thread.sleep(waitSeconds)
	}
	protected def doCopy(ns:String, src:String, target:String, ranges:List[(SpecificRecordBase,SpecificRecordBase)], recPerSec:Int,justCopy:Boolean):Boolean = {
		val copyrequest = new CopyRangesRequest
		copyrequest.namespace = ns
		copyrequest.destinationHost = target
		copyrequest.destinationPort = 9991
		copyrequest.ranges = rangesToKeyRanges(ranges)
		copyrequest.rateLimit = recPerSec
		
		// make sure we have record of src and target servers
		assert(server_partition_map.containsKey(src), "Must have partition for src server")
		assert(server_partition_map.containsKey(target), "Must have partition for target server")
		
		// send actor with request and wait for response
		val copier = new CopyActor(RemoteNode(src,9991),copyrequest,60*1000)
		copier.start
		while (!copier.done) Thread.sleep(100)
		
		// once it's back, sync, change mappings
		if( copier.done && copier.success) {
			// sync the two nodes with each other
			// TODO
			
			// get each server's policy
			val src_policy = getServerPolicy(ns,src)
			val target_policy = getServerPolicy(ns,target)
			
			var target_policy_new = target_policy
			ranges.foreach(range => target_policy_new = RangeManipulation.addRange(range,target_policy_new))
			
			// if target's new policy will now be same as source, update its partition to be that of source. 
			// else just update its policy
			if (justCopy) {
				if (RangeManipulation.sortRanges(target_policy_new) == RangeManipulation.sortRanges(src_policy)) {
					server_partition_map.remove(target)
					namespaces.get(ns+"/partitions/"+server_partition_map.get(src)+"/servers").createChild(target, "", CreateMode.PERSISTENT)
					namespaces.get(ns+"/partitions/"+server_partition_map.get(target)+"/servers").deleteChild(target)
				}
				else setServerPolicy(ns,target,target_policy_new)
			}
			else setServerPolicy(ns,target,target_policy_new)
			true
		}
		else false
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
	protected def rangesToPartitionedPolicy(ranges: List[(SpecificRecordBase,SpecificRecordBase)]):PartitionedPolicy = {
		val partitions:List[KeyPartition] = ranges.map(range=>{
			val partition = new KeyPartition
			partition.minKey = range._1.toBytes
			partition.maxKey = range._2.toBytes
			partition
		})
		val policy = new PartitionedPolicy; policy.partitions = partitions
		policy
	}
	protected def getServerPolicy(ns:String,server:String):List[(SpecificRecordBase,SpecificRecordBase)] = {
		val partition = server_partition_map.get(server)
		if (partition == null) return List[(SpecificRecordBase,SpecificRecordBase)]()
		else {
			val policy = parsePolicy(namespaces(ns+"/partitions/"+partition+"/policy").updateData(false))
			// return policy, or empty list if server is a standby
			logger.info("Got policy for "+server+": "+policy+" in partition "+partition)
			if (policy == List(RangeManipulation.EmptyRange)) List[(SpecificRecordBase,SpecificRecordBase)]()
			else policy
		}
	}
	protected def setServerPolicy(ns:String, server:String, ranges:List[(SpecificRecordBase,SpecificRecordBase)]):Boolean = {
		val partition = server_partition_map.get(server)
		if (partition == null) false
		else {
			logger.info("Setting policy for "+server+": "+ranges+" in partition "+partition)
			val policy = if (ranges.isEmpty) rangesToPartitionedPolicy(List(RangeManipulation.EmptyRange)) else rangesToPartitionedPolicy(ranges)
			
			// if this server will now have just EmptyRanges, move it to that partition
			if (ranges.isEmpty) {
				namespaces.get(ns+"/partitions/"+partition+"/servers").deleteChild(server)
				if (parsePolicy(namespaces(ns+"/partitions/"+EMPTY+"/policy").data) != RangeManipulation.EmptyRange) {
					namespaces(ns+"/partitions/"+EMPTY+"/policy").data = policy.toBytes
					logger.warn("Empty policy partition wasn't empty!")
				}
				namespaces.get(ns+"/partitions/"+EMPTY+"/servers").createChild(server, "", CreateMode.PERSISTENT)
			}
			// server's policy used to be empty, now has stuff, so need a new partition
			else if (partition == EMPTY) {
				synchronized { // one thread at a time, since we're getting a new partition id
					val new_partition = getNewPartition(ns)
					// check if new partition exists, if not, create it
					val exists = namespaces.get(ns+"/partitions").children.contains(new_partition)
					if (!exists) createPartition(ns,new_partition,policy) 
					else namespaces(ns+"/partitions/"+new_partition+"/policy").data = policy.toBytes
					// add server to this new partition, removing first from old one
					namespaces.get(ns+"/partitions/"+EMPTY+"/servers").deleteChild(server)
					namespaces.get(ns+"/partitions/"+new_partition+"/servers").createChild(server, "", CreateMode.PERSISTENT)
				}
			}
			// else just update its policy in its existing partition
			else {
				val currentChildren = namespaces(ns+"/partitions/"+partition+"/servers").updateChildren(false)
				assert (currentChildren.size == 1, "Shouldn't change policy when there's more than one server under it")
				namespaces(ns+"/partitions/"+partition+"/policy").data = policy.toBytes
			}
			true
		}
	}
	protected def createPartition(ns:String, name:String, policy:PartitionedPolicy) = {
		val partition = namespaces.getOrCreate(ns+"/partitions/"+name)
		partition.createChild("policy", policy.toBytes, CreateMode.PERSISTENT)
		partition.createChild("servers", "", CreateMode.PERSISTENT)
	}
	protected def getNewPartition(ns:String):String = {
		var max = 0
		var available:String = null
		val partitions = namespaces.get(ns+"/partitions").children
		partitions.foreach(part=>{
			try { if (part._1.toInt > max) max = part._1.toInt
			} catch { case e:java.lang.NumberFormatException => {}}
			if (namespaces.get(ns+"/partitions/"+part._1+"/servers").children.isEmpty) available = part._1
		})
		if (available != null) available.toString
		else (max+1).toString
	}
}

class CopyActor(host:RemoteNode ,scads_req:Object, waitTime:Int) extends Actor {
	val logger = Logger.getLogger("scads.copy")
	var success = false
	var done = false
	scads_req match { case cr:CopyRangesRequest => logger.info("CopyActor will copy at "+cr.rateLimit) }
	
	def act = {
		val req = new Message
		req.body = scads_req
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
							case good:TransferSucceeded => { logger.info("records: "+good.recordsSent+", time: "+good.milliseconds); done=true; success=true }
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