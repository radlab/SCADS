package edu.berkeley.cs.scads.placement

import org.apache.thrift.server.TNonblockingServer
import org.apache.thrift.transport.TNonblockingServerSocket
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}
import edu.berkeley.cs.scads.thrift.{DataPlacementServer, KnobbedDataPlacementServer,DataPlacement, RangeConversion, NotImplemented, RecordSet,RangeSet,ConflictPolicy, ConflictPolicyType}
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.nodes.StorageNode
import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator

trait SimpleKnobbedDataPlacementServer extends KnobbedDataPlacementServer.Iface with AutoKey with RangeConversion{
	val conflictPolicy = new ConflictPolicy()
	conflictPolicy.setType(ConflictPolicyType.CPT_GREATER)

	import java.text.ParsePosition
	val logger = Logger.getLogger("placement.dataplacementserver")
	var spaces = new scala.collection.mutable.HashMap[String, java.util.List[DataPlacement]]

	def lookup_namespace(ns: String): java.util.List[DataPlacement] = {
		logger.info("Do I have namespace "+ns+ "? "+spaces.contains(ns))
		if (spaces.contains(ns)) { logger.info("Namespace "+ns+" has "+spaces(ns).size +"entries"); spaces(ns) } else { new java.util.LinkedList[DataPlacement] }
	}
	def lookup_node(ns: String, host: String, thriftPort: Int, syncPort: Int): DataPlacement = {
		var ret:DataPlacement = null
		if (spaces.contains(ns)) {
			val entries = lookup_namespace(ns)
			val iter = entries.iterator
			var entry:DataPlacement = null
			while (iter.hasNext) { // better way than linear scan?
				entry = iter.next
				if (entry.node==host && entry.thriftPort==thriftPort && entry.syncPort==syncPort) ret = entry
			}
		}
		logger.info("Node "+host+" has entry? "+(ret==null))
		ret
	}
	def lookup_key(ns: String, key: String): java.util.List[DataPlacement] = {
		var ret = new java.util.ArrayList[DataPlacement]()
		if (spaces.contains(ns)) {
			val entries = lookup_namespace(ns)
			val iter = entries.iterator
			var entry:DataPlacement = null
			while (iter.hasNext) {
				entry = iter.next
				if (entry.rset.range.includes(key)) ret.add(entry)
			}
		}
		logger.info("Key "+key+" available from "+ ret.size+" nodes.")
		ret
	}
	def lookup_range(ns: String, range: RangeSet): java.util.List[DataPlacement] = {
		var ret = new java.util.ArrayList[DataPlacement]()
		if (spaces.contains(ns)) {
			val entries = lookup_namespace(ns)
			val iter = entries.iterator
			//var entry:DataPlacement = null // make this val?
			while (iter.hasNext) {
				val entry:DataPlacement = iter.next
				if ( (rangeSetToKeyRange(entry.rset.range) & range) != KeyRange.EmptyRange ) { ret.add(entry) }
			}
		}
		logger.info("Range "+range.start_key+" - "+range.end_key+" available from "+ ret.size+" nodes.")
		ret
	}

	private def add(ns: String, entry: DataPlacement) {
		logger.info("Adding "+ entry.node + ":"+entry.syncPort)
		val prev_entry:DataPlacement = lookup_node(ns, entry.node, entry.thriftPort, entry.syncPort)
		if (prev_entry != null) prev_entry.setRset(entry.rset)
		else spaces(ns).add(entry)
		assign(ns, entry.node, entry.thriftPort, entry.syncPort, entry.rset)
		logger.debug("Namespace entry size for "+ ns+ " : "+spaces(ns).size)
	}

	private def addAll(ns: String, entries: java.util.List[DataPlacement]): Boolean = {
		spaces += (ns -> entries)
		val iter = entries.iterator
		var entry:DataPlacement = null
		while (iter.hasNext) {
			entry = iter.next
			logger.info("Adding "+ entry.node + ":"+entry.syncPort)
			assign(ns, entry.node, entry.thriftPort, entry.syncPort, entry.rset)
		}
		logger.debug("Namespace entry size for "+ ns+ " : "+spaces(ns).size)
		spaces.contains(ns)
	}

	private def assign(ns:String, host: String, thrift: Int, sync: Int, rset: RecordSet) {
		val node = new StorageNode(host,thrift, sync)
		node.useConnection((c) => c.set_responsibility_policy(ns, rset))
	}

	def add(ns: String, entries:java.util.List[DataPlacement]):Boolean = {
		if (!spaces.contains(ns)) addAll(ns,entries)
		else {
			val iter = entries.iterator
			while (iter.hasNext)  add(ns, iter.next)
		}
		spaces.contains(ns)
	}

	def copy(ns: String, rset: RecordSet, src_host: String, src_thrift: Int, src_sync: Int, dest_host: String, dest_thrift: Int, dest_sync: Int) = {
		val target_range = rangeSetToKeyRange(rset.range)
		val oldDestRange = if ( lookup_node(ns,dest_host,dest_thrift,dest_sync) != null) {rangeSetToKeyRange(lookup_node(ns,dest_host,dest_thrift,dest_sync).rset.range)} else {KeyRange.EmptyRange}
		val newDestRange = oldDestRange + target_range
		val src = new StorageNode(src_host,src_thrift,src_sync) // where copying from

		// Verify the src has our keyRange
		val compare_range = rangeSetToKeyRange(lookup_node(ns, src_host,src_thrift,src_sync).rset.range)
		logger.info("Verifying node "+src_host+" with range "+compare_range.start +" - "+compare_range.end+" contains "+ target_range.start +" - "+ target_range.end)
		assert( (compare_range & target_range) == target_range )

		// Tell the servers to copy the data
		logger.info("Copying "+ target_range.start + " - "+ target_range.end +" from "+src_host + " to "+ dest_host)
		src.useConnection((c) => c.copy_set(ns, rset, dest_host+":"+dest_sync))

		// Change the assignment
		logger.info("Changed assignments: "+dest_host +" gets: "+ newDestRange.start +" - "+newDestRange.end)
		val list = new java.util.ArrayList[DataPlacement]()
		list.add(new DataPlacement(dest_host,dest_thrift,dest_sync,newDestRange))
		add(ns, list)
		logger.debug("Namespace entry size for "+ ns+ " : "+spaces(ns).size)

		// Sync keys that might have changed
		src.useConnection((c) => c.sync_set(ns, rset, dest_host+":"+dest_sync, conflictPolicy))		
	}
	def move(ns: String, rset: RecordSet, src_host: String, src_thrift: Int, src_sync: Int, dest_host: String, dest_thrift: Int, dest_sync: Int) {
		val target_range = rangeSetToKeyRange(rset.range)
		val oldDestRange = if ( lookup_node(ns,dest_host,dest_thrift,dest_sync) != null) {rangeSetToKeyRange(lookup_node(ns,dest_host,dest_thrift,dest_sync).rset.range)} else {KeyRange.EmptyRange}
		val newDestRange = oldDestRange + target_range
		val src = new StorageNode(src_host,src_thrift,src_sync)

		// Verify the src has our keyRange and set up new range
		val compare_range = rangeSetToKeyRange(lookup_node(ns, src_host,src_thrift,src_sync).rset.range)
		logger.info("Verifying node "+src_host+" with range "+compare_range.start +" - "+compare_range.end+" contains "+ target_range.start +" - "+ target_range.end)
		assert( (compare_range & target_range) == target_range )
		val newSrcRange = compare_range - target_range

		// Tell the servers to move the data
		logger.info("Moving "+ target_range.start + " - "+ target_range.end +" from "+src_host + " to "+ dest_host)
		src.useConnection((c) => c.copy_set(ns, rset, dest_host+":"+dest_sync))

		// Change the assignment
		logger.info("Changed assignments: "+dest_host +" gets: "+ newDestRange.start +" - "+newDestRange.end+"\n"+src_host+ " gets: "+ newSrcRange.start +" - "+newSrcRange.end)
		val list = new java.util.ArrayList[DataPlacement]()
		list.add(new DataPlacement(dest_host,dest_thrift,dest_sync,newDestRange))
		list.add(new DataPlacement(src_host,src_thrift,src_sync,newSrcRange))
		add(ns, list)
		logger.debug("Namespace entry size for "+ ns+ " : "+spaces(ns).size)

		// Sync and remove moved range from source
		src.useConnection((c) => c.sync_set(ns, rset, dest_host+":"+dest_sync, conflictPolicy) )
		src.useConnection((c) => c.remove_set(ns, rset) )
	}

	def remove(ns: String, entries:java.util.List[DataPlacement]):Boolean = {
		if (!spaces.contains(ns)) return true
		val iter = entries.iterator
		var entry:DataPlacement = null
		var other_entry:DataPlacement = null
		var candidate:DataPlacement = null
		val toRemove = new java.util.LinkedList[DataPlacement]

		while (iter.hasNext) {
			entry = iter.next
			candidate = lookup_node(ns, entry.node, entry.thriftPort, entry.syncPort)
			if (candidate != null) {
				val node = new StorageNode(entry.node,entry.thriftPort,entry.syncPort)

				// before removing, sync range with other nodes that have overlapping range
				val others = lookup_range(ns, entry.rset.range)
				val others_iter = others.iterator
				while (others_iter.hasNext) {
					other_entry = others_iter.next
					node.useConnection((c) => c.sync_set(ns, entry.rset, other_entry.node+":"+other_entry.syncPort, conflictPolicy) )
				}

				// now remove range from the storage node
				node.useConnection((c) => c.remove_set(ns, rangeSetToKeyRange(entry.rset.range)))
			}
			toRemove.add(candidate)
			logger.info("Removing "+ toRemove.size +" entries from namespace "+ns)
		}
		spaces(ns).removeAll(toRemove)
	}
}

class KnobServer extends SimpleKnobbedDataPlacementServer

case class RunnableDataPlacementServer extends Runnable {
	val port = System.getProperty("placementport","8000").toInt
	val serverthread = new Thread(this, "DataPlacementServer-" + port)
	serverthread.start
	
	def run() {
		val logger = Logger.getLogger("placement.dataplacementserver")
		try {
			val serverTransport = new TNonblockingServerSocket(port)
	    	val processor = new KnobbedDataPlacementServer.Processor(new KnobServer())
			val protFactory = 
				if (System.getProperty("xtrace")!=null) {new XtBinaryProtocol.Factory(true, true)} else {new TBinaryProtocol.Factory(true, true)}
	    	val server = new TNonblockingServer(processor, serverTransport,protFactory)
    
			if (System.getProperty("xtrace")!=null) { logger.info("Starting data placement with xtrace enabled") }
			logger.info("Starting data placement server on "+port)
	    	server.serve()
	  	} catch { 
	    	case x: Exception => x.printStackTrace()
	  	}
	}
}

object SimpleDataPlacementApp extends Application {
	//def main(args:Array[String]) = {
		BasicConfigurator.configure()
		val dps = new RunnableDataPlacementServer
	//}
}
