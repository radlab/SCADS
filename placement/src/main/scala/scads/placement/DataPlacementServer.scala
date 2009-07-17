package edu.berkeley.cs.scads.placement

import org.apache.thrift.server.TNonblockingServer
import org.apache.thrift.transport.TNonblockingServerSocket
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}
import edu.berkeley.cs.scads.thrift.{DataPlacementServer, KnobbedDataPlacementServer,DataPlacement, RangeConversion, NotImplemented, RecordSet,RangeSet,ConflictPolicy, ConflictPolicyType}
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.nodes.StorageNode
import org.apache.log4j.Logger

trait SimpleDataPlacementServer extends DataPlacementServer.Iface {
	import java.text.ParsePosition
	val logger = Logger.getLogger("placement.dataplacementserver")
	var spaces = new scala.collection.mutable.HashMap[String, java.util.List[DataPlacement]]

	def lookup_namespace(ns: String): java.util.List[DataPlacement] = {
		if (spaces.contains(ns)) { spaces(ns) } else { null }
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
		ret
	}
	def lookup_key(ns: String, key: String): java.util.List[DataPlacement] = {
		throw new NotImplemented
	}
	def lookup_range(ns: String, range: RangeSet): java.util.List[DataPlacement] = {
		throw new NotImplemented
	}
}

trait SimpleKnobbedDataPlacementServer extends KnobbedDataPlacementServer.Iface with SimpleDataPlacementServer with RangeConversion {
	val conflictPolicy = new ConflictPolicy()
	conflictPolicy.setType(ConflictPolicyType.CPT_GREATER)
	
	def add(ns: String, entries:java.util.List[DataPlacement]):Boolean = {
		spaces += (ns -> entries)
		val iter = entries.iterator
		var entry:DataPlacement = null
		while (iter.hasNext) { // this could be parallelized
			entry = iter.next
			val node = new StorageNode(entry.node, entry.thriftPort, entry.syncPort)
			node.useConnection((c) => c.set_responsibility_policy(ns, entry.rset))
		}
		spaces.contains(ns)
	}

	def copy(ns: String, rset: RecordSet, src_host: String, src_thrift: Int, src_sync: Int, dest_host: String, dest_thrift: Int, dest_sync: Int) = throw new NotImplemented
	def move(ns: String, rset: RecordSet, src_host: String, src_thrift: Int, src_sync: Int, dest_host: String, dest_thrift: Int, dest_sync: Int) = throw new NotImplemented
	def remove(ns: String, entries:java.util.List[DataPlacement]):Boolean = throw new NotImplemented
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
    
	    	println("Starting data placement server on "+port)
	    	server.serve()
	  	} catch { 
	    	case x: Exception => x.printStackTrace()
	  	}
	}
}

object SimpleDataPlacementApp extends Application {
	val dps = new RunnableDataPlacementServer
}
