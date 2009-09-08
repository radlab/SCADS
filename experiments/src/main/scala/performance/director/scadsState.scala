package scads.director

import performance._
import java.util.Date

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

import org.apache.log4j._
import org.apache.log4j.Level._

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
case class StorageNodeState(
	val ip: String,
	val metrics: PerformanceMetrics,
	val metricsByType: Map[String,PerformanceMetrics]
) {
	override def toString():String = "server@"+ip+"\n   all=["+metrics.toString+"]\n"+metricsByType.map(e => "   "+e._1+"=["+e._2.toString+"]").mkString("","\n","")
	def toShortString():String = " W="+"%-20s".format(metrics.workload.toInt+"/"+metricsByType("get").workload.toInt+"/"+metricsByType("put").workload.toInt)+ 
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
	
	def splitAllInHalf():SCADSconfig = {
		var config = this
		for (server <- config.storageNodes) {
			val action = SplitInTwo(server._1)
			config = action.preview(config)
		}
		config
	}
	
	def toCSVString():String = "server,minKey,maxKey\n"+storageNodes.toList.sort(_._2.minKey<_._2.minKey).map( s=> s._1+","+s._2.minKey+","+s._2.maxKey ).mkString("\n")
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

	val logger = Logger.getLogger("scads.state")
	private val logPath = Director.basedir+"/state.txt"
	logger.setLevel(DEBUG)

	var metricDBConnection: ThriftMetricDBConnection = null
	var dbConnection: Connection = null

	def initLogging(metricDBHost:String, metricDBPort:Int) {
		metricDBConnection = ThriftMetricDBConnection(metricDBHost,metricDBPort)
		dbConnection = Director.connectToDatabase
		initTables
	}

	def initTables() {
        // create database if it doesn't exist and select it
		val dbname = "director"
        try {
            val statement = dbConnection.createStatement
            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbname)
            statement.executeUpdate("USE " + dbname)
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS scadsstate_config (`id` INT NOT NULL AUTO_INCREMENT, `time` BIGINT, `config` TEXT, PRIMARY KEY(`id`) ) ")
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS scadsstate_histogram (`id` INT NOT NULL AUTO_INCREMENT, `time` BIGINT, `histogram` TEXT, PRIMARY KEY(`id`) ) ")
			statement.close
       	} catch { case ex: SQLException => ex.printStackTrace() }

        println("initialized scadsstate_config and scadsstate_histogram tables")
	}
	
	def dumpState(state:SCADSState) {
		// dump config
        val statement = dbConnection.createStatement
		val configSQL = Director.createInsertStatement("scadsstate_config", Map("time"->state.time.getTime.toString,"config"->("'"+state.config.toCSVString+"'")))
		//logger.debug("storing config: "+configSQL)
		statement.executeUpdate(configSQL)
		statement.close
		
		// dump performance metrics
		val metricUpdates = 
			state.metrics.createMetricUpdates("ALL","ALL")++
			List.flatten(state.metricsByType.map(m=>m._2.createMetricUpdates("ALL",m._1)).toList)++
			List.flatten(state.storageNodes.map(n=> n.metrics.createMetricUpdates(n.ip,"ALL").toList++List.flatten(n.metricsByType.map(t=>t._2.createMetricUpdates(n.ip,t._1)).toList) ))
		metricDBConnection.metricService.update( s2jList(metricUpdates) )
		
		// dump histogram
        val statement2 = dbConnection.createStatement
		val histogramSQL = Director.createInsertStatement("scadsstate_histogram", Map("time"->state.time.getTime.toString,"histogram"->("'"+state.workloadHistogram.toCSVString+"'")))
		//logger.debug("storing histogram: "+histogramSQL)
		statement2.executeUpdate(histogramSQL)
		statement2.close
	}

	private def s2jList[T](list:List[T]): java.util.ArrayList[T] = {
		var jl = new java.util.ArrayList[T]()
		list.foreach(jl.add(_))
		jl
	}

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
			nodes += new StorageNodeState(ip,sMetrics,sMetricsByType)
			nodeConfig = nodeConfig.update(ip, range)
		}
		
		val metrics = PerformanceMetrics.load(metricReader,"ALL","ALL")
		val metricsByType = reqTypes.map( (t) => t -> PerformanceMetrics.load(metricReader,"ALL",t)).foldLeft(Map[String,PerformanceMetrics]())((x,y)=>x+y)
		// TODO: will need to load the workload histogram here
		new SCADSState(new Date(), SCADSconfig(nodeConfig), nodes.toList, metrics, metricsByType,null)
	}
	
	/**
	* create SCADSState by predicting the performance metrics (workload and latency) using the performance model (assuming it last for 'duration' seconds).
	* Also, save the performance metrics to the metric database
	*/
	def createFromPerfModel(time:Date, config:SCADSconfig, histogram:WorkloadHistogram, perfModel:PerformanceModel, duration:Int):SCADSState = {

		val allGets = new scala.collection.mutable.ListBuffer[Double]()
		val allPuts = new scala.collection.mutable.ListBuffer[Double]()
		
		val serverWorkload = PerformanceEstimator.estimateServerWorkload(config,histogram)
		val storageNodes = new scala.collection.mutable.ListBuffer[StorageNodeState]()

		var stats = PerformanceStats(duration,0,0,0,0,0,0,0,0,0)
		for (s <- config.storageNodes.keySet) {
			val w = serverWorkload(s)
			val getLatencies = perfModel.sample(Map("type"->"get","getw"->w.getRate.toString,"putw"->w.putRate.toString),(w.getRate*duration).toInt)
			val putLatencies = perfModel.sample(Map("type"->"put","getw"->w.getRate.toString,"putw"->w.putRate.toString),(w.putRate*duration).toInt)
			allGets++=getLatencies
			allPuts++=putLatencies
			
			val statsAll = PerformanceMetrics.estimateFromSamples(getLatencies++putLatencies,time,duration)
			val statsGet = PerformanceMetrics.estimateFromSamples(getLatencies,time,duration)
			val statsPut = PerformanceMetrics.estimateFromSamples(putLatencies,time,duration)
			
			storageNodes += StorageNodeState(s,statsAll,Map("get"->statsGet,"put"->statsPut))
		}
		
		val statsAll = PerformanceMetrics.estimateFromSamples(allGets.toList++allPuts,time,duration)
		val statsGet = PerformanceMetrics.estimateFromSamples(allGets.toList,time,duration)
		val statsPut = PerformanceMetrics.estimateFromSamples(allPuts.toList,time,duration)
		
		SCADSState(time,config,storageNodes.toList,statsAll,Map("get"->statsGet,"put"->statsPut),histogram)
	}
	
}

case class SCADSState(
	val time: Date,
	val config: SCADSconfig,
	val storageNodes: List[StorageNodeState],
	val metrics: PerformanceMetrics,
	val metricsByType: Map[String,PerformanceMetrics],
	val workloadHistogram: WorkloadHistogram
) {	
	override def toString():String = {
		"STATE@"+time+"  metrics["+
		(if(metrics!=null)metrics.toString else "")+"]\n"+
		(if(metricsByType!=null)metricsByType.map("   ["+_.toString+"]").mkString("","\n","") else "")+
		"\nindividual servers:\n"+
		(if(storageNodes!=null)storageNodes.map(_.toString).mkString("","\n","") else "")
	}
	def toShortString():String = {
		"STATE@"+time+"  W="+
		(if(metrics!=null)metrics.workload.toInt else "")+"/"+
		(if(metricsByType!=null)(metricsByType("get").workload.toInt+"/"+metricsByType("put").workload.toInt 	+ 
		"  getL="+metricsByType("get").toShortLatencyString()+"  putL="+metricsByType("put").toShortLatencyString()) else "") +
		(if(storageNodes!=null)storageNodes.sort( (s1,s2) => config.storageNodes(s1.ip).minKey<config.storageNodes(s2.ip).minKey )
											.map(s=>"  server@"+s.ip+"   "+"%-15s".format(config.storageNodes(s.ip))+ "      " + s.toShortString()).mkString("\n","\n","") else "")
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
) extends Ordered[WorkloadFeatures]{
	def add(that:WorkloadFeatures):WorkloadFeatures = {
		WorkloadFeatures(this.getRate+that.getRate,this.putRate+that.putRate,this.getsetRate+that.getsetRate)
	}
	
	// if this workload is running against n machines, each machine get 1/n of the gets
	def splitGets(n:Int):WorkloadFeatures = {
		WorkloadFeatures(this.getRate/n,this.putRate,this.getsetRate)
	}
	/**
	* Divde the gets amongst replicas, and indicate the amount of allowed puts (in range 0.0-1.0)
	* Workload increases are applied before dividing amongst replicas or restricting puts
	*/
	def restrictAndSplit(replicas:Int, allowed_puts:Double, percentIncrease:Double):WorkloadFeatures = {
		assert(allowed_puts <= 1.0 && allowed_puts >= 0.0, "Amount of allowed puts must be in range [0.0 - 1.0]")
		WorkloadFeatures((this.getRate*(1.0+percentIncrease))/replicas,(this.putRate*(1.0+percentIncrease))*allowed_puts,this.getsetRate*(1.0+percentIncrease))
	}
	def compare(that: WorkloadFeatures):Int = {
		val thistotal = this.sum
		val thattotal = that.sum
		if (thistotal < thattotal) -1
		else if (thistotal == thattotal) 0
		else 1
	}
	def sum:Double = this.getRate + this.putRate + (if (!this.getsetRate.isNaN) {this.getsetRate} else {0.0})
	def + (that:WorkloadFeatures):WorkloadFeatures = {
		WorkloadFeatures(this.getRate+that.getRate,this.putRate+that.putRate,this.getsetRate+that.getsetRate)
	}
	def - (that:WorkloadFeatures):WorkloadFeatures = {
		WorkloadFeatures(Math.abs(this.getRate-that.getRate),Math.abs(this.putRate-that.putRate),Math.abs(this.getsetRate+that.getsetRate))
	}
	def * (multiplier:Double):WorkloadFeatures = {
		WorkloadFeatures(this.getRate*multiplier,this.putRate*multiplier,this.getsetRate*multiplier)
	}
}

/**
* Represents the histogram of keys in workload
*/
class WorkloadHistogram (
	val rangeStats: Map[DirectorKeyRange,WorkloadFeatures]
) extends Ordered[WorkloadHistogram] {
	def divide(replicas:Int, allowed_puts:Double):WorkloadHistogram = {
		new WorkloadHistogram(
			Map[DirectorKeyRange,WorkloadFeatures](rangeStats.toList map {entry => (entry._1, entry._2.restrictAndSplit(replicas,allowed_puts,0.0)) } : _*)
		)
	}
	def modify(replicas:Int, allowed_puts:Double, percentIncrease:Double):WorkloadHistogram = {
		new WorkloadHistogram(
			Map[DirectorKeyRange,WorkloadFeatures](rangeStats.toList map {entry => (entry._1, entry._2.restrictAndSplit(replicas,allowed_puts,percentIncrease)) } : _*)
		)
	}
	override def toString():String = rangeStats.keySet.toList.sort(_.minKey<_.minKey).map( r=>r+"   "+rangeStats(r) ).mkString("\n")

	def compare(that:WorkloadHistogram):Int = { // do bin-wise comparison of workloadfeatures, return summation
		this.rangeStats.toList.map(entry=>entry._2.compare( that.rangeStats(entry._1) )).reduceLeft(_+_)
	}
	/**
	* Split this workload into ranges such that the workload is more or less balanced between them
	*/
	def split(pieces:Int):List[List[DirectorKeyRange]] = {
		val splits = new scala.collection.mutable.ListBuffer[List[DirectorKeyRange]]()
		val targetworkload = this.rangeStats.values.reduceLeft(_+_).sum.toInt/pieces // the workload each split should aim for
		var split = new scala.collection.mutable.ListBuffer[DirectorKeyRange]()
		var splitworkload = 0
		this.rangeStats.keys.toList.sort(_.minKey < _.minKey).foreach((range)=>{
			if (splitworkload < targetworkload) { split += range; splitworkload += this.rangeStats(range).sum.toInt }
			else { splits += split.toList; split = new scala.collection.mutable.ListBuffer[DirectorKeyRange](); splitworkload = 0 }
		})
		if (!split.isEmpty) { splits += split.toList } // add the last one split, if necessary
		splits.toList
	}
	def + (that:WorkloadHistogram):WorkloadHistogram = {
		new WorkloadHistogram(
			Map[DirectorKeyRange,WorkloadFeatures]( this.rangeStats.toList map {entry=>(entry._1, entry._2+that.rangeStats(entry._1))} : _*)
		)
	}
	def - (that:WorkloadHistogram):WorkloadHistogram = {
		new WorkloadHistogram(
			Map[DirectorKeyRange,WorkloadFeatures]( this.rangeStats.toList map {entry=>(entry._1, entry._2-that.rangeStats(entry._1))} : _*)
		)
	}
	def * (multiplier:Double):WorkloadHistogram = {
		new WorkloadHistogram(
			Map[DirectorKeyRange,WorkloadFeatures]( this.rangeStats.toList map {entry=>(entry._1, entry._2*multiplier)} : _*)
		)
	}

	def toShortString():String = {
		val getRate = rangeStats.map(_._2.getRate).reduceLeft(_+_)
		val putRate = rangeStats.map(_._2.putRate).reduceLeft(_+_)
		"WorkloadHistogram: (total rates) get="+getRate+"r/s, put="+putRate+"r/s"
	}
	
	def toCSVString():String = {
		"minKey,maxKey,getRate,putRate,getsetRate\n"+
		rangeStats.keySet.toList.sort(_.minKey<_.minKey)
			.map( r=>r.minKey+","+r.maxKey+","+rangeStats(r).getRate+","+rangeStats(r).putRate+","+rangeStats(r).getsetRate )
			.mkString("\n")
	}
}

object WorkloadHistogram {
	def generateRequests(interval:WorkloadIntervalDescription, rate:Double, nBins:Int):List[SCADSRequest] = {
		val nRequests = nBins*100
		val requests = (for (r <- 1 to nRequests) yield { interval.requestGenerator.generateRequest(null,0) }).toList
		requests
	}
	def createRanges(interval:WorkloadIntervalDescription, rate:Double, nBins:Int, maxKey:Int):List[DirectorKeyRange] = {
		val minKey = 0
		val requests = generateRequests(interval,rate,nBins)
		val rnd = new java.util.Random()
		val allkeys = for (r <- requests) yield { r match{
										case g:SCADSGetRequest => {g.key.toInt}
										case p:SCADSPutRequest => {p.key.toInt}
										case _ => {-1} } }
		val keys = allkeys.filter(_!= -1).toList.sort(_<_).toList
		val boundaries = (List(minKey)++((for (i <- 1 to nBins-1) yield { keys(rnd.nextInt(keys.size)) }).toList.removeDuplicates)++List(maxKey)).sort(_<_)
		val ranges = boundaries.take(nBins).zip(boundaries.tail).map(b=>new DirectorKeyRange(b._1,b._2))
		println("Histogram has ranges: \n"+ranges.sort(_.minKey<_.minKey).mkString("",",",""))
		ranges
	}
	def createEquiWidthRanges(interval:WorkloadIntervalDescription, rate:Double, nBins:Int, maxKey:Int):List[DirectorKeyRange] = {
		val binWidth = (maxKey-0)/nBins
		val ranges = List[DirectorKeyRange]((0 until nBins).map((id)=> DirectorKeyRange(id*binWidth,(id+1)*binWidth)):_*)
		println("Histogram has ranges: \n"+ranges.sort(_.minKey<_.minKey).mkString("",",",""))
		ranges
	}
	def createFromRanges(ranges:List[DirectorKeyRange],interval:WorkloadIntervalDescription, rate:Double, nBins:Int):WorkloadHistogram = {
		val requests = generateRequests(interval,rate,nBins)
		val nRequests = requests.size
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
	def create(interval:WorkloadIntervalDescription, rate:Double, nBins:Int,maxKey:Int): WorkloadHistogram = {
		val ranges = createRanges(interval,rate,nBins,maxKey)
		// have the ranges, now count the keys in each range
		createFromRanges(ranges,interval,rate,nBins)
	}
	def summarize(ranges: List[DirectorKeyRange], histograms:List[WorkloadHistogram]):WorkloadHistogram = {
		val index_choice = Math.floor(0.9*(histograms.size-1)).toInt
		// create map by summarizing the workload features from all the histograms for each range
		val rangeStats = Map[DirectorKeyRange,WorkloadFeatures](
			ranges.map {range => {
				var getRate = new scala.collection.mutable.ListBuffer[Double]()
				var putRate = new scala.collection.mutable.ListBuffer[Double]()
				var getsetRate = new scala.collection.mutable.ListBuffer[Double]()
				histograms.foreach((hist)=> {
					val features = hist.rangeStats(range)
					getRate += features.getRate
					putRate += features.putRate
					getsetRate += features.getsetRate
				})
				val getRateChoice = getRate.toList.sort(_<_).apply(index_choice)
				val putRateChoice = putRate.toList.sort(_<_).apply(index_choice)
				val getsetRateChoice = getsetRate.toList.sort(_<_).apply(index_choice)
				(range,WorkloadFeatures(getRateChoice, putRateChoice, getsetRateChoice)) }
			}:_*)
		new WorkloadHistogram(rangeStats)
	}
}

