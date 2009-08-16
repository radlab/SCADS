package scads.director

import java.util.Date

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

// ??
class DirectorKeyRange(
	val minKey: Int,
	val maxKey: Int
) {
	override def toString():String = "["+minKey+","+maxKey+")"
}


// add multiple namespaces per node?
class StorageNodeState(
	val ip: String,
	val metrics: PerformanceMetrics,
	val metricsByType: Map[String,PerformanceMetrics],
	val range: DirectorKeyRange
) {
	override def toString():String = "server@"+ip+" range="+range.toString+" \n   all=["+metrics.toString+"]\n"+metricsByType.map(e => "   "+e._1+"=["+e._2.toString+"]").mkString("","\n","")
	def toShortString():String = "server@"+"%-45s".format(ip)+" range="+"%-20s".format(range.toString)+
									" W="+"%-20s".format(metrics.workload.toInt+"/"+metricsByType("get").workload.toInt+"/"+metricsByType("put").workload.toInt)+ 
									" getL="+"%-15s".format(metricsByType("get").toShortLatencyString())+" putL="+"%-15s".format(metricsByType("put").toShortLatencyString())
}

class RangeState // TODO, holds metrics from histogram

case class SCADSconfig(
	val storageNodes: Map[String,DirectorKeyRange],
	val ranges: List[RangeState]
) {
	def getNodes:List[String] = storageNodes.keySet.toList
}

object SCADSState {
	import performance.Scads
	import java.util.Comparator
	import edu.berkeley.cs.scads.thrift.DataPlacement
	import edu.berkeley.cs.scads.keys._

	class DataPlacementComparator extends java.util.Comparator[DataPlacement] {
		def compare(o1: DataPlacement, o2: DataPlacement): Int = {
			o1.rset.range.start_key compareTo o2.rset.range.start_key
		}
	}

	def refresh(metricReader:MetricReader, placementServerIP:String): SCADSState = {
		val reqTypes = List("get","put")
		val dp = Scads.getDataPlacementHandle(placementServerIP,Director.xtrace_on)
		val placements = dp.lookup_namespace(Director.namespace)
		java.util.Collections.sort(placements,new DataPlacementComparator)
		
		// iterate through storage server info and get performance metrics
		var nodes = new scala.collection.mutable.ListBuffer[StorageNodeState]
		var ranges = new scala.collection.mutable.ListBuffer[RangeState]
		var nodeConfig = Map[String,DirectorKeyRange]()
		val iter = placements.iterator
		while (iter.hasNext) {
			val info = iter.next
			val ip = info.node

			val range = new DirectorKeyRange(
				Scads.getNumericKey( StringKey.deserialize_toString(info.rset.range.start_key,new java.text.ParsePosition(0)) ),
				Scads.getNumericKey( StringKey.deserialize_toString(info.rset.range.end_key,new java.text.ParsePosition(0)) )
			)
			val sMetrics = PerformanceMetrics.load(metricReader,ip,"ALL")
			val sMetricsByType = reqTypes.map( (t) => t -> PerformanceMetrics.load(metricReader,ip,t)).foldLeft(Map[String, PerformanceMetrics]())((x,y) => x + y)	
			nodes += new StorageNodeState(ip,sMetrics,sMetricsByType,range)
			ranges += new RangeState // TODO: fill in
			nodeConfig = nodeConfig.update(ip, range)
		}
		
		val metrics = PerformanceMetrics.load(metricReader,"ALL","ALL")
		val metricsByType = reqTypes.map( (t) => t -> PerformanceMetrics.load(metricReader,"ALL",t)).foldLeft(Map[String,PerformanceMetrics]())((x,y)=>x+y)
		new SCADSState(new Date(), SCADSconfig(nodeConfig,ranges.toList), nodes.toList, metrics, metricsByType)
	}
}

class SCADSState(
	val time: Date,
	val config: SCADSconfig,
	val storageNodes: List[StorageNodeState],
	val metrics: PerformanceMetrics,
	val metricsByType: Map[String,PerformanceMetrics]
) {	
	override def toString():String = {
		"STATE@"+time+"  metrics["+metrics.toString+"]\n"+
		metricsByType.map("   ["+_.toString+"]").mkString("","\n","")+
		"\nindividual servers:\n"+storageNodes.map(_.toString).mkString("","\n","")
	}
	def toShortString():String = {
		"STATE@"+time+"  W="+metrics.workload.toInt+"/"+metricsByType("get").workload.toInt+"/"+metricsByType("put").workload.toInt 	+ 
		"  getL="+metricsByType("get").toShortLatencyString()+"  putL="+metricsByType("put").toShortLatencyString() +
		storageNodes.map(_.toShortString()).mkString("\n  ","\n  ","")
	}
}


