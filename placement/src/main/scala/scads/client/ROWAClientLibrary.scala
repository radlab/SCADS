package edu.berkeley.cs.scads.client

import edu.berkeley.xtrace._
import edu.berkeley.cs.scads.thrift.{Record,RecordSet,ExistingValue,KeyStore,NotResponsible,NotImplemented,RangeConversion}
import edu.berkeley.cs.scads.placement.{SimpleDataPlacementService,LocalDataPlacementProvider,RemoteDataPlacementProvider}
import edu.berkeley.cs.scads.nodes.StorageNode
import edu.berkeley.cs.scads.keys._
import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator

import java.util.Comparator
import scala.collection.mutable.{HashMap, HashSet}

class RecordComparator extends java.util.Comparator[Record] {
	def compare(o1: Record, o2: Record): Int = {
		o1.key compareTo o2.key
	}
}

class LocalROWAClientLibrary extends ROWAClientLibrary with LocalDataPlacementProvider

class SCADSClient(h: String, p: Int) extends ROWAClientLibrary with RemoteDataPlacementProvider {
	val port = p
	val host = h
}

abstract class ROWAClientLibrary extends ClientLibrary with SimpleDataPlacementService with RangeConversion with AutoKey {
	import java.util.Random
	import java.text.ParsePosition
	val logger = Logger.getLogger("client.rowa")

	val retries = 5
	val ttl = 30000 + (new Random().nextInt(11000)) // cached mapping ttl somewhere 30-40 seconds
	var lastRefresh:Long = System.currentTimeMillis()
	var destinationIP:String=null
	val thread_name = Thread.currentThread().getName()
	var deploy_name = "unnamed" // which test deployment is client a part of

	private def doRefresh = {
		this.refreshPlacement
		lastRefresh = System.currentTimeMillis()
	}

	def setDeployName(name:String) = {
		deploy_name = name
	}

	/**
	* Read value from one node. Uses local map. 
	* Does update from KeySpaceProvider if local copy is out of date.
	*/
	def get(namespace: String, key: String): Record = {
		val startt = System.nanoTime()
		if ( (lastRefresh+ttl) < System.currentTimeMillis() ) doRefresh
		val ret = this.get_retry(namespace,key,retries) // destinationIP is set here
		var latency = System.nanoTime()-startt
		XTraceContext.logEvent(thread_name,"ROWAClientLibrary","RequestDetails",",get,"+destinationIP+","+(latency/1000000.0)+","+deploy_name)
		ret
	}
	private def get_retry(namespace: String, key: String, count: Int):Record = {
		//val ns_keyspace = getKeySpace(namespace)
		val potentials = lookup(namespace,key).toList
		logger.debug("Lookup for key "+key+ " yielded "+potentials.length+" nodes.")
		val serialized_key ="'" + key + "'" // serialize before sending over thrift
		try {
			if ( potentials.length >0  ) {
				val node = potentials(new Random().nextInt(potentials.length)) // use random one
				destinationIP = node.host
				val record = node.useConnection((c) => c.get(namespace,serialized_key))
				//record
				new Record(StringKey.deserialize_toString(record.getKey,new ParsePosition(0)),record.getValue)
			}
			else throw new NoNodeResponsibleException
		} catch {
			case e:NotResponsible => {
				doRefresh
				logger.debug("Refreshed placement for namespace "+namespace)
				if (count >0)
					this.get_retry(namespace,key,count-1)
				else {
					logger.warn("Client library failed refresh attempts on [" +namespace+"]"+key+": "+retries)
					throw e // TODO: throw more meaningful exception
				}
			}
			case e => {
				logger.debug("Client library exception in get(): "+e)
				throw e
			}
		}
	}
	
	/**
	* Read values from as many nodes as needed. Uses local map.
	* Does update from KeySpaceProvider if local copy is out of date.
	*/
	def get_set(namespace: String, keys: RecordSet): java.util.List[Record] = {
		val startt = System.nanoTime()
		if ( (lastRefresh+ttl) < System.currentTimeMillis() ) doRefresh

		var count = retries
		var records = new HashSet[Record]
		//val ns_keyspace = getKeySpace(namespace)
		val target_range = this.rangeSetToKeyRange(keys.range) //new KeyRange(keys.range.start_key, keys.range.end_key)
		var ranges = Set[KeyRange]()

		var offset = 0
		if (keys.range.isSetOffset()) { offset = keys.range.getOffset() }
		var limit = 0
		val haveLimit = if (keys.range.isSetLimit()) { limit = keys.range.getLimit(); true } else { false }

		// determine which ranges to ask from which nodes
		// assumes no gaps in range, but someone should tell user if entire range isn't covered
		val potentials = lookup(namespace,target_range) //ns_keyspace.lookup(target_range)
		val query_nodes = this.get_set_queries(potentials,target_range)		

		// now do the getting
		var node_record_count = 0
		query_nodes.foreach( {case (node,keyrange)=> {			
			ranges += keyrange
			val rset = this.keyRangeToScadsRangeSet(keyrange)

			if (offset > 0) { // have to compensate for offset
				
				try {
					node_record_count = node.useConnection((c) => c.count_set(namespace,rset))
				} catch {
					case e:NotResponsible => {
						doRefresh
						logger.debug("Refreshed placement for namespace "+namespace)
						if (count>0) {
							node_record_count = node.useConnection((c) => c.count_set(namespace,rset))
							count -=1
						}
						else {
							logger.warn("Client library failed refresh attempts on [" +namespace+"]: "+retries)
							throw e // TODO: throw more meaningful exception
						}
					}
				}
			}

			if (node_record_count >= offset && ( (haveLimit && limit > 0) || !haveLimit) ) {
				if (offset > 0) { rset.range.setOffset(offset) }
				if (haveLimit) { rset.range.setLimit(limit) }
				var r:Record = null
				try {
					destinationIP = node.host
					val startt = System.nanoTime()
					val records_subset = node.useConnection((c) => c.get_set(namespace,rset))
					var latency = System.nanoTime()-startt
					XTraceContext.logEvent(thread_name,"ROWAClientLibrary","RequestDetails","get_set,"+destinationIP+","+(latency/1000000.0)+","+deploy_name)
					val iter:java.util.Iterator[Record] = records_subset.iterator()
					while (iter.hasNext()) {
						//records += iter.next(); limit -=1}
						r = iter.next().asInstanceOf[Record];
						records += new Record(StringKey.deserialize_toString(r.getKey,new ParsePosition(0)),r.getValue);
						limit -= 1 }
				} catch {
					case e:NotResponsible => {
						doRefresh
						logger.debug("Refreshed placement for namespace "+namespace)
						if (count>0) {
							destinationIP = node.host
							val startt = System.nanoTime()
							val records_subset = node.useConnection((c) => c.get_set(namespace,rset))
							var latency = System.nanoTime()-startt
							XTraceContext.logEvent(thread_name,"ROWAClientLibrary","RequestDetails","get_set,"+destinationIP+","+(latency/1000000.0)+","+deploy_name)
							val iter = records_subset.iterator()
							while (iter.hasNext()) { records += iter.next(); limit -= 1 }
							count-=1
						}
						else {
							logger.warn("Client library failed refresh attempts on [" +namespace+"]: "+retries)
							throw e // TODO: throw more meaningful exception
						}
					}
					case e => {
						logger.debug("Client library exception in get_set(): "+e)
						throw e
					}
				}
			} // end if
			offset -= node_record_count // when both are zero, does nothing
		}
		})
		
		// make sure desired range was actually covered by what we gots
		if ( !KeyRange.isCovered(target_range,ranges) ) {
			throw new NonCoveredRangeException // do we ever reach here?
		}	 
		// sort an array
		val records_array = records.toArray
		java.util.Arrays.sort(records_array,new RecordComparator()) 
		val ret = java.util.Arrays.asList(records_array: _*) // shitty, but convert to java array
		var latency = System.nanoTime()-startt
		XTraceContext.logEvent(thread_name,"ROWAClientLibrary","RequestDetails","get_set_total,ALL,"+(latency/1000000.0)+","+deploy_name)
		ret
	}
	
	private def get_set_queries(nodes: Map[StorageNode, KeyRange], target_range: KeyRange): HashMap[StorageNode, KeyRange] = {
		var resultmap = new HashMap[StorageNode, KeyRange]
		
		var start = target_range.start
		val end = target_range.end
		
		var done = false // have we found everything we can get?
		var nodes_used = Set[StorageNode]() // which nodes we've checked so far, assumes nodes have only one range
		while (!done) {
			val node_tuple = this.find_node_at_start(nodes.filter((entry)=> !nodes_used.contains(entry._1)),start)
			if (node_tuple._2.end==null || (end != null && node_tuple._2.end > end)) 
				resultmap += node_tuple._1 -> KeyRange(node_tuple._2.start,end)
			else resultmap += node_tuple._1 -> node_tuple._2
			start = node_tuple._2.end
			nodes_used += node_tuple._1
			if ( (start==null ) || (end !=null && start >= end) ) { done = true } 
			// even if start was null to begin with, will only be again if get to an end being null
		}
		resultmap
	}

	private def find_node_at_start(nodes: Map[StorageNode,KeyRange], start: Key): (StorageNode,KeyRange) = {
		// nodes that start at or before target start, null target start needs a null start
		var potential_nodes = Map[StorageNode,KeyRange]()
		if (start == null) { potential_nodes = nodes.filter((entry) => entry._2.start==null) }
		else { potential_nodes = nodes.filter((entry) => entry._2.start==null || entry._2.start <= start) } 
	
		if ( !potential_nodes.elements.hasNext ) throw new NonCoveredRangeException
		var chosen_node = potential_nodes.elements.next
		val chosen_index = new Random().nextInt(potential_nodes.size)
		for(i<- 0 until chosen_index+1) {
			assert(potential_nodes.elements.hasNext)
			chosen_node = potential_nodes.elements.next
		}
		var end = chosen_node._2.end
		
		potential_nodes.foreach((entry) => {
			if (entry._2.end==null || entry._2.end > end) {
				chosen_node = entry
				end = chosen_node._2.end
			}
		})
		val range_covered = new KeyRange(start,end)
		(chosen_node._1, range_covered)
	}
	
	/**
	* Write records to all responsible nodes.
	* Does update from KeySpaceProvider if local copy is out of date.
	*/
	def put(namespace: String, rec:Record): Boolean = {
		val startt = System.nanoTime()
		if ( (lastRefresh+ttl) < System.currentTimeMillis() ) doRefresh
		var ret = this.put_retry(namespace,rec,retries)  // destinationIP is set here
		var latency = System.nanoTime()-startt
		XTraceContext.logEvent(thread_name,"ROWAClientLibrary","RequestDetails","put,"+destinationIP+","+(latency/1000000.0)+","+deploy_name)
		ret
	}

	private def put_retry(namespace: String, rec:Record,count:Int): Boolean = {
		val key = rec.getKey()
		//val ns_keyspace = getKeySpace(namespace)
		val put_nodes = lookup(namespace,key)//ns_keyspace.lookup(key)
		if ( !(put_nodes.length > 0) ) throw new NoNodeResponsibleException
		var total_success = true
		
		put_nodes.foreach({ case(node)=>{
			try {
				destinationIP = node.host
				val success = node.useConnection((c) => c.put(namespace,new Record("'" + rec.getKey() + "'",rec.getValue())))
				total_success && success 
			} catch {
				case e:NotResponsible => {
					doRefresh
					logger.debug("Refreshed placement for namespace "+namespace)
					if (count>0) {
						val success = this.put_retry(namespace,rec,count-1) // recursion may redo some work
						total_success && success
					}
					else {
						logger.warn("Client library failed refresh attempts on [" +namespace+"]: "+retries)
						throw e // TODO: throw more meaningful exception
					}
				}
				case e => {
					logger.debug("Client library exception in put(): "+e)
					throw e
				}
			}
		}})
		total_success
	}

	def count_set(namespace: String ,keys: RecordSet): Int = {
		this.count_set_retry(namespace,keys,retries)
	}
	private def count_set_retry(namespace: String ,keys: RecordSet, count: Int): Int = {
		throw new NotImplemented("ROWAClientLibrary.count_set()") // TODO
	}
	def test_and_set(namespace: String, rec: Record, existing: ExistingValue): Boolean = {
		this.test_and_set(namespace,rec,existing,retries)
	}
	private def test_and_set(namespace: String, rec: Record, existing: ExistingValue, count: Int): Boolean = {
		throw new NotImplemented("ROWAClientLibrary.count_set()") // TODO
	}
}