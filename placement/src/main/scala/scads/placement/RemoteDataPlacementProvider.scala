package edu.berkeley.cs.scads.placement

import org.apache.log4j.Logger
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}
import edu.berkeley.cs.scads.thrift.{DataPlacement, RangeConversion, DataPlacementServer}
import edu.berkeley.cs.scads.nodes.StorageNode
import edu.berkeley.cs.scads.keys.KeyRange
import edu.berkeley.cs.scads.keys._

import java.text.ParsePosition

trait RemoteDataPlacementProvider extends SimpleDataPlacementService with AutoKey with RangeConversion {
	def host: String 
	def port: Int
	var client: DataPlacementServer.Client = null
	val logger:Logger

	private def getClient():DataPlacementServer.Client  = {
		if(client == null) {
    		val transport = new TFramedTransport(new TSocket(host, port))
    		val protocol = if (System.getProperty("xtrace")!=null) {new XtBinaryProtocol(transport)} else {new TBinaryProtocol(transport)}
    		client = new DataPlacementServer.Client(protocol)
    		transport.open()
			println("New connection opened to DataPlacementServer at " + host+":"+port)
		}
    	return client
  	}
	private def getFromRemote(ns: String) {
		var mapping = Map[StorageNode, KeyRange]()
		val entries = getClient().lookup_namespace(ns)
		val iter = entries.iterator
		while (iter.hasNext) {
			val entry:DataPlacement = iter.next
			mapping += ( new StorageNode(entry.node,entry.thriftPort, entry.syncPort) -> entry.rset.range ) // implicit conversion
			logger.debug("Adding to namespace: "+entry.node +" gets "+ entry.rset.range.start_key +" - "+ entry.rset.range.end_key)
		}
		space += ( ns -> mapping )
		logger.info("Placement entries for "+ns+": \n"+super.printSpace(ns))
	}
	
	override def refreshPlacement = { space = Map[String, Map[StorageNode, KeyRange]]() }
	
	override def lookup(ns: String): Map[StorageNode, KeyRange] = {
		var ret = super.lookup(ns)
		if(ret.isEmpty) { getFromRemote(ns); ret = super.lookup(ns) }
		ret
	}
	
	override def lookup(ns: String, node: StorageNode): KeyRange = {
		var ret = super.lookup(ns, node)
		if(ret == KeyRange.EmptyRange) { getFromRemote(ns); ret = super.lookup(ns, node) }
		ret
	}
	override def lookup(ns: String, key: Key):List[StorageNode] = {
		var ret = super.lookup(ns, key)
		if(ret.isEmpty) { getFromRemote(ns); ret = super.lookup(ns, key) }
		ret
	}
	override def lookup(ns: String, range: KeyRange): Map[StorageNode, KeyRange] = {
		var ret = super.lookup(ns, range)
		if(ret.isEmpty) { getFromRemote(ns); ret = super.lookup(ns, range) }
		ret
	}

}