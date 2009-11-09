package edu.berkeley.cs.scads.deployment

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.Level._

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._
import edu.berkeley.cs.scads.keys.{MinKey,MaxKey,Key,KeyRange,StringKey}
import edu.berkeley.cs.scads.nodes._

import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TProtocol,TBinaryProtocol, XtBinaryProtocol}

import scala.collection.jcl.Conversions

object DPConnectionPool extends AbstractConnectionPool[KnobbedDataPlacementServer.Client] {
    val creator = (protocol:TProtocol) => { new KnobbedDataPlacementServer.Client(protocol) }
}

trait ThriftMachine extends ThriftService {
    def getMachine: RClusterNode
}

class StorageMachine(val machine: RClusterNode, thriftPort: Int, syncPort: Int) extends StorageNode(machine.hostname, thriftPort, syncPort) with ThriftMachine {
    def this(h:RClusterNode, p:Int) = this(h,p,p)
    override def getMachine = machine

    def getDataPlacement(rs:RecordSet):DataPlacement = {
        new DataPlacement(host,thriftPort,syncPort,rs)
    }

    def totalCount(ns:String):Int = { 
        val range = ScadsDeployUtil.getAllRangeRecordSet
        useConnection( (c) => c.count_set(ns,range) )
    }

    def totalSet(ns:String):List[Record] = {
        val range = ScadsDeployUtil.getAllRangeRecordSet
        Conversions.convertList(useConnection( (c) => c.get_set(ns,range) )).toList
    }

    def getNthRecord(ns:String,n:Int):Record = {
        val range = ScadsDeployUtil.getNthElemRS(n)
        val list = useConnection((c) => c.get_set(ns,range))
        if ( list.size == 0 ) {
            null
        } else { 
            list.get(0)
        }
    }

}

case class DataPlacementMachine(machine: RClusterNode, thriftPort: Int, syncPort: Int) extends AbstractServiceNode[KnobbedDataPlacementServer.Client] with ThriftMachine {
    def this(h:RClusterNode, p:Int) = this(h,p,p)
    override def getMachine = machine
    val host = machine.hostname
    val connectionPool = DPConnectionPool

    var lpNodeCache: LocalDataPlacementNode = null

    def getLocalNode() = lpNodeCache

    def getLocalNode(handler: UnknownNSHandler) = {
        if ( lpNodeCache == null ) {
            lpNodeCache = new LocalDataPlacementNode( host, thriftPort, handler)
        }   
        lpNodeCache
    }

    def getKnownNS():List[String] = lpNodeCache match {
        case null  => List[String]()
        case cache => cache.space.keySet.toList
    }
    
    def lookup_node(ns:String,smachine:StorageMachine):DataPlacement = {
        useConnection( (c) => c.lookup_node(ns, smachine.host, smachine.thriftPort, smachine.syncPort) )
    }

    def move(ns:String,rset:RecordSet,from:StorageMachine,to:StorageMachine):Unit = {
        useConnection( (c) => c.move( 
            ns, 
            rset, 
            from.host, 
            from.thriftPort, 
            from.syncPort, 
            to.host,
            to.thriftPort,
            to.syncPort) )
    }

}

case class LocalDataPlacementNode(host: String, port: Int, handler: UnknownNSHandler) extends TransparentRemoteDataPlacementProvider {
    val logger = Logger.getLogger("scads.placementNode")

    override def lookup(ns: String): Map[StorageNode, KeyRange] = { 
        var ret = super.lookup(ns)
        if(ret.isEmpty) { handler.handleUnknownNS(ns); ret = super.lookup(ns) }
        ret 
    }   

    override def lookup(ns: String, node: StorageNode): KeyRange = { 
        var ret = super.lookup(ns, node)
        if(ret == KeyRange.EmptyRange) { handler.handleUnknownNS(ns); ret = super.lookup(ns, node) }
        ret 
    }   
    override def lookup(ns: String, key: Key):List[StorageNode] = { 
        var ret = super.lookup(ns, key)
        if(ret.isEmpty) { handler.handleUnknownNS(ns); ret = super.lookup(ns, key) }
        ret 
    }   
    override def lookup(ns: String, range: KeyRange): Map[StorageNode, KeyRange] = { 
        var ret = super.lookup(ns, range)
        if(ret.isEmpty) { handler.handleUnknownNS(ns); ret = super.lookup(ns, range) }
        ret 
    } 
}

object StorageMachine {
    def fromTuple(tuple:Tuple2[RClusterNode,Int]):StorageMachine = {
        fromTuple((tuple._1,tuple._2,tuple._2))
    }
    def fromTuple(tuple:Tuple3[RClusterNode,Int,Int]):StorageMachine = {
        new StorageMachine(tuple._1,tuple._2,tuple._3)
    }
}

object DataPlacementMachine {
    def fromTuple(tuple:Tuple2[RClusterNode,Int]):DataPlacementMachine = {
        fromTuple((tuple._1,tuple._2,tuple._2))
    }
    def fromTuple(tuple:Tuple3[RClusterNode,Int,Int]):DataPlacementMachine = {
        new DataPlacementMachine(tuple._1,tuple._2,tuple._3)
    }
}
