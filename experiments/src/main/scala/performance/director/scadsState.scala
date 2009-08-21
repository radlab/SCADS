package scads.director

import performance._
import java.util.Date

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

// ??
case class DirectorKeyRange(
	val minKey: Int,
	val maxKey: Int
) {
	override def toString():String = "["+minKey+","+maxKey+")"
	
	def overlaps(that:DirectorKeyRange):Boolean = {
		if (that.maxKey<=this.minKey || this.maxKey<=that.minKey) false
		else true
	}
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

case class SCADSconfig(
	val storageNodes: Map[String,DirectorKeyRange]
) {
	val rangeNodes = arrangeReplicas
	// TODO: create mapping of ranges to put-ratio sampling

	def getNodes:List[String] = storageNodes.keySet.toList
	def getReplicas(server:String):List[String] = rangeNodes(storageNodes(server))
	private def arrangeReplicas:Map[DirectorKeyRange, List[String]] = {
		val mapping = scala.collection.mutable.Map[DirectorKeyRange,scala.collection.mutable.Buffer[String]]()
		storageNodes.foreach((entry)=> mapping(entry._2) = mapping.getOrElse(entry._2,new scala.collection.mutable.ListBuffer[String]())+entry._1)
		Map[DirectorKeyRange,List[String]](mapping.keys.toList map {s => (s, mapping(s).toList)} : _*)
	}
	override def toString():String = storageNodes.toList.sort(_._2.minKey<_._2.minKey).map( x=>"%-15s".format(x._2)+"  "+x._1 ).mkString("\n")
}

object SCADSconfig {
	def getInitialConfig(range:DirectorKeyRange):SCADSconfig = SCADSconfig( Map(getRandomServerNames(null,1).first->range) )

	def getRandomServerNames(cfg:SCADSconfig,n:Int):List[String] = {
		val rnd = new java.util.Random()
		val newNames = scala.collection.mutable.HashSet[String]()
		var name = ""
		for (i <- 1 to n) { do { name = "s"+"%05d".format(rnd.nextInt(10000)) } while ( (cfg!=null&&cfg.storageNodes.contains(name))||newNames.contains(name) ); newNames+=name }
		newNames.toList
	}
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
			nodeConfig = nodeConfig.update(ip, range)
		}
		
		val metrics = PerformanceMetrics.load(metricReader,"ALL","ALL")
		val metricsByType = reqTypes.map( (t) => t -> PerformanceMetrics.load(metricReader,"ALL",t)).foldLeft(Map[String,PerformanceMetrics]())((x,y)=>x+y)
		new SCADSState(new Date(), SCADSconfig(nodeConfig), nodes.toList, metrics, metricsByType, null)
	}
}

class SCADSState(
	val time: Date,
	val config: SCADSconfig,
	val storageNodes: List[StorageNodeState],
	val metrics: PerformanceMetrics,
	val metricsByType: Map[String,PerformanceMetrics],
	val workloadHistogram: WorkloadHistogram
) {	
	override def toString():String = {
		"STATE@"+time+"  metrics["+
		(if(metrics!=null)metrics.toString)+"]\n"+
		(if(metricsByType!=null)metricsByType.map("   ["+_.toString+"]").mkString("","\n",""))+
		"\nindividual servers:\n"+
		(if(storageNodes!=null)storageNodes.map(_.toString).mkString("","\n",""))
	}
	def toShortString():String = {
		"STATE@"+time+"  W="+
		(if(metrics!=null)metrics.workload.toInt)+"/"+
		(if(metricsByType!=null)(metricsByType("get").workload.toInt+"/"+metricsByType("put").workload.toInt 	+ 
		"  getL="+metricsByType("get").toShortLatencyString()+"  putL="+metricsByType("put").toShortLatencyString())) +
		(if(storageNodes!=null)storageNodes.map(_.toShortString()).mkString("\n  ","\n  ",""))
	}
	def changeConfig(new_config:SCADSconfig):SCADSState = new SCADSState(time,new_config,storageNodes,metrics,metricsByType,null)
	
	def preview(action:Action,performanceModel:PerformanceModel,updatePerformance:Boolean):SCADSState = {
		val newConfig = action.preview(config)
		new SCADSState(time,newConfig,null,null,null,workloadHistogram) // TODO update histogram capability
	}
}


case class WorkloadFeatures(
	getRate: Double,
	putRate: Double,
	getsetRate: Double
) {
	def add(that:WorkloadFeatures):WorkloadFeatures = {
		WorkloadFeatures(this.getRate+that.getRate,this.putRate+that.putRate,this.getsetRate+that.getsetRate)
	}
	
	// if this workload is running against n machines, each machine get 1/n of the gets
	def splitGets(n:Int):WorkloadFeatures = {
		WorkloadFeatures(this.getRate/n,this.putRate,this.getsetRate)
	}
}

/**
* Represents the histogram of keys in workload
*/
class WorkloadHistogram(
	val rangeStats: Map[DirectorKeyRange,WorkloadFeatures]
) {
	def divide(replicas:Int):WorkloadHistogram = {
		new WorkloadHistogram(
			Map[DirectorKeyRange,WorkloadFeatures](rangeStats.toList map {entry => (entry._1, entry._2.splitGets(replicas)) } : _*)
		)
	}
	override def toString():String = rangeStats.keySet.toList.sort(_.minKey<_.minKey).map( r=>r+"   "+rangeStats(r) ).mkString("\n")
}

object WorkloadHistogram {
	def create(interval:WorkloadIntervalDescription, rate:Double, nBins:Int): WorkloadHistogram = {
		val nRequests = nBins*100
		val rnd = new java.util.Random()
		val requests = (for (r <- 1 to nRequests) yield { interval.requestGenerator.generateRequest(null,0) }).toList
		val allkeys = for (r <- requests) yield { r match{ 
										case g:SCADSGetRequest => {g.key.toInt} 
										case p:SCADSPutRequest => {p.key.toInt}
										case _ => {-1} } }
		val keys = allkeys.filter(_!= -1).toList.sort(_<_).toList
		val boundaries = (List(keys.first)++(for (i <- 1 to nBins-1) yield { keys(rnd.nextInt(keys.size)) })++List(keys.last)).sort(_<_)
		val ranges = boundaries.take(nBins).zip(boundaries.tail).map(b=>new DirectorKeyRange(b._1,b._2))
		
		// have the ranges, now count the keys in each range
		val getCounts = scala.collection.mutable.Map[DirectorKeyRange,Int]()
		val putCounts = scala.collection.mutable.Map[DirectorKeyRange,Int]()
		var ri = 0
		var range = ranges(ri)

		val typesAndKeys = (for (r <- requests) yield { 
			r match {
				case g:SCADSGetRequest => { ("get",g.key.toInt) }
				case p:SCADSPutRequest => { ("put",p.key.toInt) }
				case _ => { ("na",-1) } 
			}			
		}).filter(_._2!= -1).toList.sort(_._2<_._2).toList
		
		for (tk <- typesAndKeys) { 
			val rtype = tk._1
			val key = tk._2
			while (key>range.maxKey) { ri = ri+1; /*println("next range: "+ri+" key="+key+"  max="+range.maxKey);*/ range = ranges(ri) }
			rtype match {
				case "get" => getCounts(range) = getCounts.getOrElse(range,0)+1
				case "put" => putCounts(range) = putCounts.getOrElse(range,0)+1
				case _ =>
			}
		}
		
		val rangeStats = scala.collection.mutable.Map[DirectorKeyRange,WorkloadFeatures]()
		for (range <- ranges) {
			val getRate = getCounts.getOrElse(range,0).toDouble / nRequests * rate //(interval.duration/1000)
			val putRate = putCounts.getOrElse(range,0).toDouble / nRequests * rate //(interval.duration/1000)
			rangeStats += range -> WorkloadFeatures(getRate,putRate,Double.NaN)
		}		
		new WorkloadHistogram(Map[DirectorKeyRange,WorkloadFeatures]()++rangeStats)
	}
}

