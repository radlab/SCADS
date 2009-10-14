package scads.director

import performance._
import scads.deployment.ScadsDeploy
import java.util.Date
import java.io._

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

import org.apache.log4j._
import org.apache.log4j.Level._

// ??
@serializable
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
	val storageNodes: Map[String,DirectorKeyRange],
	val putRestrictions:Map[DirectorKeyRange,Double], // mapping of ranges to put-ratio sampling
	val standbys:List[String]
) {
	val rangeNodes = arrangeReplicas

	def getNodes:List[String] = storageNodes.keySet.toList
	def getReplicas(server:String):List[String] = rangeNodes(storageNodes(server))
	private def arrangeReplicas:Map[DirectorKeyRange, List[String]] = {
		val mapping = scala.collection.mutable.Map[DirectorKeyRange,scala.collection.mutable.Buffer[String]]()
		storageNodes.foreach((entry)=> mapping(entry._2) = mapping.getOrElse(entry._2,new scala.collection.mutable.ListBuffer[String]())+entry._1)
		Map[DirectorKeyRange,List[String]](mapping.keys.toList map {s => (s, mapping(s).toList)} : _*)
	}
	override def toString():String = { storageNodes.toList.sort(_._2.minKey<_._2.minKey).map( x=>"%-15s".format(x._2)+"  "+x._1 ).mkString("\n") +
		"\n"+ putRestrictions.toList.sort(_._1.minKey<_._1.minKey).map( x=>"%-15s".format(x._1)+"  "+x._2 ).mkString("\n")
	}

	def updateNodes(nodes: Map[String,DirectorKeyRange]):SCADSconfig = SCADSconfig(nodes,putRestrictions,standbys)
	def updateRestrictions(restricts:Map[DirectorKeyRange,Double]):SCADSconfig = SCADSconfig(storageNodes,restricts,standbys)
	def updateStandbys(new_standbys:List[String]):SCADSconfig = SCADSconfig(storageNodes,putRestrictions,new_standbys)
	
	def splitAllInHalf():SCADSconfig = {
		var config = this
		for (server <- config.storageNodes.keys.toList.sort(_<_)) {
			val action = SplitInTwo(server,-1)
			config = action.preview(config)
		}
		config
	}
	
	def toCSVString():String = "server,minKey,maxKey\n"+storageNodes.toList.sort(_._2.minKey<_._2.minKey).map( s=> s._1+","+s._2.minKey+","+s._2.maxKey ).mkString("\n")
}

object SCADSconfig {
	import edu.berkeley.cs.scads.WriteLock
	val lock = new WriteLock

	def getInitialConfig(range:DirectorKeyRange):SCADSconfig = {
		Director.resetRnd(7)
		SCADSconfig( Map(getRandomServerNames(null,1).first->range),Map[DirectorKeyRange,Double](),List[String]() )
	}

	var pastServers = scala.collection.mutable.HashSet[String]()
	private var standbys = List[String]() // assume accessed concurrently
	var standbyMaxPoolSize = 0
	def viewStandbys = {
		lock.lock
		try { val ret = standbys.take(standbyMaxPoolSize); lock.unlock; ret } // make a "copy" to return
		finally lock.unlock
	}
	def getStandbys(num:Int):List[String] = {
		lock.lock
		try {
			val from_standbys = standbys.take(num)
			standbys =  standbys -- from_standbys
			lock.unlock
			from_standbys
		} finally lock.unlock
	}
	/**
	* attempt to put servers back in standby pool
	* any servers that exceed the max pool size are returned
	*/
	def returnStandbys(returned:List[String]):List[String] = {
		lock.lock
		try {
			val taking = returned.take(standbyMaxPoolSize-standbys.size)
			standbys = standbys ++ taking
			val ret = returned -- taking
			lock.unlock
			ret
		} finally lock.unlock
	}
	def getRandomServerNames(cfg:SCADSconfig,n:Int):List[String] = {
		val newNames = scala.collection.mutable.HashSet[String]()
		var name = ""
		for (i <- 1 to n) {
			var ps = pastServers.clone
			ps--=newNames
			if (cfg!=null) ps--=cfg.storageNodes.keySet
			if (ps.size>0) 
				name = ps.toList(0)
			else
				do { 
					name = "s"+"%03d".format(Director.nextRndInt(999)) 
				} while ( (cfg!=null&&cfg.storageNodes.contains(name))||newNames.contains(name) )
			newNames+=name
			pastServers+=name
		}
		newNames.toList
	}
}

object SCADSState {
	import scads.deployment.ScadsDeploy
	import java.util.Comparator
	import edu.berkeley.cs.scads.thrift.{DataPlacement,PutRestriction}
	import edu.berkeley.cs.scads.keys._
	import scala.io.Source

	val logger = Logger.getLogger("scads.state")
	private val logPath = Director.basedir+"/state.txt"
	logger.addAppender( new FileAppender(new PatternLayout(Director.logPattern),logPath,false) )
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

	def dumpConfig(time:Long,config:SCADSconfig) {
        val statement = dbConnection.createStatement
		val configSQL = Director.createInsertStatement("scadsstate_config", Map("time"->time.toString,"config"->("'"+config.toCSVString+"'")))
		//logger.debug("storing config: "+configSQL)
		statement.executeUpdate(configSQL)
		statement.close		
	}

	def dumpState(state:SCADSState) {
		// dump config
        val statement = dbConnection.createStatement
		val configSQL = Director.createInsertStatement("scadsstate_config", Map("time"->state.time.toString,"config"->("'"+state.config.toCSVString+"'")))
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
		val histogramSQL = Director.createInsertStatement("scadsstate_histogram", Map("time"->state.time.toString,"histogram"->("'"+state.workloadHistogram.toCSVString+"'")))
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
	
	private def updateRestrictions(putRestrictFile:String):Map[DirectorKeyRange,Double] = {
		try {
			val urlsrc = Source.fromURL(putRestrictFile) // absolute URL
			val restrict_str = PutRestriction.stringToTuples(urlsrc.getLine(1))

			// set up map of restrictions
			Map[DirectorKeyRange,Double](restrict_str.map {entry => (new DirectorKeyRange( entry._1.toInt,entry._2.toInt) -> entry._3) }:_*)
		} catch { case e => logger.warn("Couldn't update put() restrictions"); Map[DirectorKeyRange,Double]()}
	}

	def refresh(metricReader:MetricReader, placementServerIP:String): SCADSState = {
		val reqTypes = List("get","put")
		val dp = ScadsDeploy.getDataPlacementHandle(placementServerIP,Director.xtrace_on)
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
				ScadsDeploy.getNumericKey( StringKey.deserialize_toString(info.rset.range.start_key,new java.text.ParsePosition(0)) ),
				ScadsDeploy.getNumericKey( StringKey.deserialize_toString(info.rset.range.end_key,new java.text.ParsePosition(0)) )
			)
			val sMetrics = PerformanceMetrics.load(metricReader,ip,"ALL")
			val sMetricsByType = reqTypes.map( (t) => t -> PerformanceMetrics.load(metricReader,ip,t)).foldLeft(Map[String, PerformanceMetrics]())((x,y) => x + y)	
			nodes += new StorageNodeState(ip,sMetrics,sMetricsByType)
			nodeConfig = nodeConfig.update(ip, range)
		}
		
		val metrics = PerformanceMetrics.load(metricReader,"ALL","ALL")
		val metricsByType = reqTypes.map( (t) => t -> PerformanceMetrics.load(metricReader,"ALL",t)).foldLeft(Map[String,PerformanceMetrics]())((x,y)=>x+y)
		
		// load and process workload histograms
		val histogramRaw = WorkloadHistogram.loadMostRecentFromDB(metricReader.report_prob)
		
		new SCADSState(new Date().getTime, SCADSconfig(nodeConfig,updateRestrictions(Director.putRestrictionURL),SCADSconfig.viewStandbys), nodes.toList, metrics, metricsByType, histogramRaw)
	}
	
	def refreshAtTime(metricReader:MetricReader, placementServerIP:String, time:Long): SCADSState = {
		val haveData = metricReader.haveDataForTime(time)

		if (!haveData) null
		else {
			//Director.logger.debug("refreshing state at time: "+new Date(time)+" ("+time+")")
			val reqTypes = List("get","put")
			val dp = ScadsDeploy.getDataPlacementHandle(placementServerIP,Director.xtrace_on)
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
					ScadsDeploy.getNumericKey( StringKey.deserialize_toString(info.rset.range.start_key,new java.text.ParsePosition(0)) ),
					ScadsDeploy.getNumericKey( StringKey.deserialize_toString(info.rset.range.end_key,new java.text.ParsePosition(0)) )
				)
				val sMetrics = PerformanceMetrics.load(metricReader,ip,"ALL",time)
				val sMetricsByType = reqTypes.map( (t) => t -> PerformanceMetrics.load(metricReader,ip,t,time)).foldLeft(Map[String, PerformanceMetrics]())((x,y) => x + y)	
				nodes += new StorageNodeState(ip,sMetrics,sMetricsByType)
				nodeConfig = nodeConfig.update(ip, range)
			}
		
			val metrics = PerformanceMetrics.load(metricReader,"ALL","ALL",time)
			val metricsByType = reqTypes.map( (t) => t -> PerformanceMetrics.load(metricReader,"ALL",t,time)).foldLeft(Map[String,PerformanceMetrics]())((x,y)=>x+y)
		
			// load and process workload histograms
			val histogramRaw = WorkloadHistogram.loadMostRecentFromDB(metricReader.report_prob)
		
			new SCADSState(time, SCADSconfig(nodeConfig,updateRestrictions(Director.putRestrictionURL),SCADSconfig.viewStandbys), nodes.toList, metrics, metricsByType, histogramRaw)
		}
	}
	
	/**
	* create SCADSState by predicting the performance metrics (workload and latency) using the performance model (assuming it last for 'duration' milliseconds).
	* Also, save the performance metrics to the metric database
	*/
	def createFromPerfModel(time:Long, config:SCADSconfig, histogramRaw:WorkloadHistogram, perfModel:PerformanceModel, duration:Long):SCADSState = {

		var t0,t1:Long = 0
		var T = scala.collection.mutable.Map[String,Long]("init"->0,"L1"->0,"L2"->0,"L3"->0,"L4"->0,"L5"->0,"XE"->0)
		
		t0 = new Date().getTime
		val allGets = new scala.collection.mutable.ListBuffer[Double]()
		val allPuts = new scala.collection.mutable.ListBuffer[Double]()
		
		val serverWorkload = PerformanceEstimator.estimateServerWorkload(config,histogramRaw)
		val storageNodes = new scala.collection.mutable.ListBuffer[StorageNodeState]()
		t1 = new Date().getTime; T("init")+=(t1-t0)

		var stats = PerformanceStats(duration,0,0,0,0,0,0,0,0,0)
		for (s <- config.storageNodes.keySet) {
			t0 = new Date().getTime
			val w = serverWorkload(s)
			t1 = new Date().getTime; T("L1")+=(t1-t0)
			
			t0 = new Date().getTime			
			val getLatencies = perfModel.sample(Map("type"->"get","getw"->w.getRate.toString,"putw"->w.putRate.toString),(w.getRate*duration/1000.0).toInt)
			val putLatencies = perfModel.sample(Map("type"->"put","getw"->w.getRate.toString,"putw"->w.putRate.toString),(w.putRate*duration/1000.0).toInt)
			t1 = new Date().getTime; T("L2")+=(t1-t0)
			
			t0 = new Date().getTime
			allGets++=getLatencies
			allPuts++=putLatencies
			t1 = new Date().getTime; T("L3")+=(t1-t0)
			
			t0 = new Date().getTime
			val statsAll = PerformanceMetrics.estimateFromSamples(getLatencies++putLatencies,time,duration,1.0)
			val statsGet = PerformanceMetrics.estimateFromSamples(getLatencies,time,duration,1.0)
			val statsPut = PerformanceMetrics.estimateFromSamples(putLatencies,time,duration,1.0)
			t1 = new Date().getTime; T("L4")+=(t1-t0)
			
			t0 = new Date().getTime
			storageNodes += StorageNodeState(s,statsAll,Map("get"->statsGet,"put"->statsPut))
			t1 = new Date().getTime; T("L5")+=(t1-t0)
		}
		
		t0 = new Date().getTime
		val statsAll = PerformanceMetrics.estimateFromSamples(allGets.toList++allPuts,time,duration,1.0)
		val statsGet = PerformanceMetrics.estimateFromSamples(allGets.toList,time,duration,1.0)
		val statsPut = PerformanceMetrics.estimateFromSamples(allPuts.toList,time,duration,1.0)
		t1 = new Date().getTime; T("XE")+=(t1-t0)
		
		T.keySet.toList.sort(_<_).foreach( t => Director.logger.debug("state-"+t+": "+ (T(t)/1000.0) + " sec" ))
		
		SCADSState(time,config,storageNodes.toList,statsAll,Map("get"->statsGet,"put"->statsPut),histogramRaw)
	}
	
	/**
	* create SCADSState by predicting the performance metrics (workload and latency) using the performance model (assuming it last for 'duration' milliseconds).
	* Also, save the performance metrics to the metric database. Only simulate 'fraction' of requests in the performance model
	*/
	def simulate(time:Long, config:SCADSconfig, histogramRaw:WorkloadHistogram, perfModel:PerformanceModel, duration:Long, activity:SCADSActivity, fraction:Double):SCADSState = {

		var t0,t1:Long = 0
		var T = scala.collection.mutable.Map[String,Long]("init"->0,"L1"->0,"L2"->0,"L3"->0,"L4"->0,"L5"->0,"XE"->0)
		
		t0 = new Date().getTime
		val allGets = new scala.collection.mutable.ListBuffer[Double]()
		val allPuts = new scala.collection.mutable.ListBuffer[Double]()
		
		val serverWorkload = PerformanceEstimator.estimateServerWorkload(config,histogramRaw)
		val storageNodes = new scala.collection.mutable.ListBuffer[StorageNodeState]()
		t1 = new Date().getTime; T("init")+=(t1-t0)

		var stats = PerformanceStats(duration,0,0,0,0,0,0,0,0,0)
		//for (s <- config.storageNodes.toList.sort(_._2.minKey<_._2.minKey).map(_._1)) {
		for (s <- config.storageNodes.keys.toList.sort(_<_)) {
			t0 = new Date().getTime
			val w = serverWorkload(s)
			t1 = new Date().getTime; T("L1")+=(t1-t0)
			
			t0 = new Date().getTime
			val (getLatencies,putLatencies) = 
			if (activity.copyRate.contains(s)&&activity.copyRate(s)>0)
				(perfModel.sample(Map("type"->"get","getw"->w.getRate.toString,"putw"->w.putRate.toString),(w.getRate*duration/1000.0*fraction).toInt).map(_*10),
				 perfModel.sample(Map("type"->"put","getw"->w.getRate.toString,"putw"->w.putRate.toString),(w.putRate*duration/1000.0*fraction).toInt).map(_*10))
			else 
				(perfModel.sample(Map("type"->"get","getw"->w.getRate.toString,"putw"->w.putRate.toString),(w.getRate*duration/1000.0*fraction).toInt),
				 perfModel.sample(Map("type"->"put","getw"->w.getRate.toString,"putw"->w.putRate.toString),(w.putRate*duration/1000.0*fraction).toInt))
			t1 = new Date().getTime; T("L2")+=(t1-t0)
			
			t0 = new Date().getTime
			allGets++=getLatencies
			allPuts++=putLatencies
			t1 = new Date().getTime; T("L3")+=(t1-t0)
			
			t0 = new Date().getTime
			val statsAll = PerformanceMetrics.estimateFromSamples(getLatencies++putLatencies,time,duration,fraction)
			val statsGet = PerformanceMetrics.estimateFromSamples(getLatencies,time,duration,fraction)
			val statsPut = PerformanceMetrics.estimateFromSamples(putLatencies,time,duration,fraction)
			t1 = new Date().getTime; T("L4")+=(t1-t0)
			
			t0 = new Date().getTime
			storageNodes += StorageNodeState(s,statsAll,Map("get"->statsGet,"put"->statsPut))
			t1 = new Date().getTime; T("L5")+=(t1-t0)
		}
		
		t0 = new Date().getTime
		val statsAll = PerformanceMetrics.estimateFromSamples(allGets.toList++allPuts,time,duration,fraction)
		val statsGet = PerformanceMetrics.estimateFromSamples(allGets.toList,time,duration,fraction)
		val statsPut = PerformanceMetrics.estimateFromSamples(allPuts.toList,time,duration,fraction)
		t1 = new Date().getTime; T("XE")+=(t1-t0)
		
		T.keySet.toList.sort(_<_).foreach( t => Director.logger.debug("state-"+t+": "+ (T(t)/1000.0) + " sec" ))
		
		SCADSState(time,config,storageNodes.toList,statsAll,Map("get"->statsGet,"put"->statsPut),histogramRaw)
	}
	
}

case class SCADSState(
	val time: Long,
	val config: SCADSconfig,
	val storageNodes: List[StorageNodeState],
	val metrics: PerformanceMetrics,
	val metricsByType: Map[String,PerformanceMetrics],
	val workloadHistogram: WorkloadHistogram
) {	
	override def toString():String = {
		"STATE@"+(new Date(time))+"  metrics["+
		(if(metrics!=null)metrics.toString else "")+"]\n"+
		(if(metricsByType!=null)metricsByType.map("   ["+_.toString+"]").mkString("","\n","") else "")+
		"\nindividual servers:\n"+
		(if(storageNodes!=null)storageNodes.map(_.toString).mkString("","\n","") else "")+
		"\nworkload: "+workloadHistogram.toShortString
	}
	def toShortString():String = {
		val nodesByIP = Map( storageNodes.map(n=>(n.ip,n)) :_* )
		
		"STATE@"+(new Date(time))+"  W="+
		(if(metrics!=null)metrics.workload.toInt else "")+"/"+
		(if(metricsByType!=null)(metricsByType("get").workload.toInt+"/"+metricsByType("put").workload.toInt 	+ 
		"  getL="+metricsByType("get").toShortLatencyString()+"  putL="+metricsByType("put").toShortLatencyString()) else "") +
		config.storageNodes.toList.
					sort(_._2.minKey<_._2.minKey).
					map( p => {val ip=p._1; val range=p._2; "  server@"+"%-50s".format(ip)+"   "+"%-15s".format(range)+"      "+ (if (nodesByIP.contains(ip)) nodesByIP(ip).toShortString else "")} ).
					mkString("\n","\n","") +
		"\nworkload: "+workloadHistogram.toShortString									
	}
	def toConfigString():String = config.toString
	def changeConfig(new_config:SCADSconfig):SCADSState = new SCADSState(time,new_config,storageNodes,metrics,metricsByType,null)
	
	def preview(action:Action,performanceModel:PerformanceModel,updatePerformance:Boolean):SCADSState = {
		val newConfig = action.preview(config)
		new SCADSState(time,newConfig,null,null,null,workloadHistogram) // TODO update histogram capability
	}
}


case class SCADSActivity {
	var copyRate = scala.collection.mutable.Map[String,Double]()
}


@serializable
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
	def sum:Double = (if (!this.getRate.isNaN) {this.getRate} else 0.0) + 
					 (if (!this.putRate.isNaN) {this.putRate} else 0.0) + 
					 (if (!this.getsetRate.isNaN) {this.getsetRate} else {0.0})
	
	def + (that:WorkloadFeatures):WorkloadFeatures = {
		WorkloadFeatures(this.getRate+that.getRate,this.putRate+that.putRate,this.getsetRate+that.getsetRate)
	}
	def - (that:WorkloadFeatures):WorkloadFeatures = {
/*		WorkloadFeatures(Math.abs(this.getRate-that.getRate),Math.abs(this.putRate-that.putRate),Math.abs(this.getsetRate-that.getsetRate))*/
		WorkloadFeatures(this.getRate-that.getRate,this.putRate-that.putRate,this.getsetRate-that.getsetRate)
	}
	def * (multiplier:Double):WorkloadFeatures = {
		WorkloadFeatures(this.getRate*multiplier,this.putRate*multiplier,this.getsetRate*multiplier)
	}
}

/**
* Represents the histogram of keys in workload
*/
@serializable
case class WorkloadHistogram (
	val rangeStats: Map[DirectorKeyRange,WorkloadFeatures]
) extends Ordered[WorkloadHistogram] {
	def divide(replicas:Int, allowed_puts:Double):WorkloadHistogram = {
		new WorkloadHistogram(
			Map[DirectorKeyRange,WorkloadFeatures](rangeStats.toList map {entry => (entry._1, entry._2.restrictAndSplit(replicas,allowed_puts,0.0)) } : _*)
		)
	}
	override def toString():String = rangeStats.toList.sort(_._1.minKey<_._1.minKey).map( r => "%-15s".format(r._1)+"   "+r._2 ).mkString("\n")

	def multiplyKeys(factor:Double):WorkloadHistogram = {
		WorkloadHistogram( Map( rangeStats.toList.map( rs => DirectorKeyRange((rs._1.minKey*factor).toInt,(rs._1.maxKey*factor).toInt) -> rs._2 ) :_* ) )
	}
	
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

	def totalRate:Double = rangeStats.map(_._2.sum).reduceLeft(_+_)

	def toShortString():String = {
		val getRate = rangeStats.map(_._2.getRate).reduceLeft(_+_)
		val putRate = rangeStats.map(_._2.putRate).reduceLeft(_+_)
		"WorkloadHistogram: (total rates) get="+"%.2f".format(getRate)+"r/s, put="+"%.2f".format(putRate)+"r/s"
	}
	
	def toCSVString():String = {
		"minKey,maxKey,getRate,putRate,getsetRate\n"+
		rangeStats.keySet.toList.sort(_.minKey<_.minKey)
			.map( r=>r.minKey+","+r.maxKey+","+rangeStats(r).getRate+","+rangeStats(r).putRate+","+rangeStats(r).getsetRate )
			.mkString("\n")
	}
}

object WorkloadHistogram {
	def loadFromFile(file:String):WorkloadHistogram =
		WorkloadHistogram( 
			Map(scala.io.Source.fromFile(file).getLines.toList.drop(1).
			 		map( line => {val v=line.split(","); (DirectorKeyRange(v(0).toInt,v(1).toInt),WorkloadFeatures(v(2).toDouble,v(3).toDouble,v(4).toDouble) )} ) :_* ) )
	
	def loadFromCSVString(string:String):WorkloadHistogram =
		WorkloadHistogram( 
			Map( string.split("\n").toList.drop(1).
			 		map( line => {val v=line.split(","); (DirectorKeyRange(v(0).toInt,v(1).toInt),WorkloadFeatures(v(2).toDouble,v(3).toDouble,v(4).toDouble) )} ) :_* ) )

	def loadMostRecentFromDB(samplingRate:Double):WorkloadHistogram = {
		val histogramSQL = "SELECT histogram FROM director.scadsstate_histogram s order by time desc limit 1"
        val statement = SCADSState.dbConnection.createStatement
		var histogram:WorkloadHistogram = null
		try {
			val result = statement.executeQuery(histogramSQL)
			val set = result.first // set cursor to first row
			if (set) {
				val histogramCSV = result.getString("histogram")
				histogram = WorkloadHistogram( 
					Map(histogramCSV.split("\n").toList.drop(1).
					 		map( line => {val v=line.split(","); (DirectorKeyRange(v(0).toInt,v(1).toInt),WorkloadFeatures(v(2).toDouble/samplingRate,v(3).toDouble/samplingRate,v(4).toDouble/samplingRate) )}) :_* ) )
			}
       	} catch { case ex: SQLException => Director.logger.warn("SQL exception when loading histogram",ex)}
		finally {statement.close}
		histogram
	}
	
	def generateRequests(interval:WorkloadIntervalDescription, rate:Double, nBins:Int):List[SCADSRequest] = {
		val nRequests = nBins*100
		val requests = (for (r <- 1 to nRequests) yield { interval.requestGenerator.generateRequest(null,0) }).toList
		requests
	}
	def createRanges(interval:WorkloadIntervalDescription, rate:Double, nBins:Int, maxKey:Int):List[DirectorKeyRange] = {
		val minKey = 0
		val requests = generateRequests(interval,rate,nBins)
		val allkeys = for (r <- requests) yield { r match{
										case g:SCADSGetRequest => {g.key.toInt}
										case p:SCADSPutRequest => {p.key.toInt}
										case _ => {-1} } }
		val keys = allkeys.filter(_!= -1).toList.sort(_<_).toList
		val boundaries = (List(minKey)++((for (i <- 1 to nBins-1) yield { keys(Director.nextRndInt(keys.size)) }).toList.removeDuplicates)++List(maxKey)).sort(_<_)
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
	def createEquiDepthRanges(interval:WorkloadIntervalDescription, rate:Double, nBins:Int, maxKey:Int):List[DirectorKeyRange] = {
		val minKey = 0
		val requests = generateRequests(interval,rate,maxKey)
		val load_per_bin = requests.size/nBins
		// count how many requests for each key
		val keycounts = scala.collection.mutable.Map[Int,Int]()
		for (r <- requests) {
			val key = r match{
							case g:SCADSGetRequest => g.key.toInt
							case p:SCADSPutRequest => p.key.toInt
							case _ => -1 }
			if (keycounts.contains(key)) keycounts(key) +=1
			else keycounts(key) = 1
		}
		val boundaries = new scala.collection.mutable.ListBuffer[Int]()
		boundaries += minKey; boundaries += maxKey
		var count = 0
		keycounts.toList.filter(_._1 >= 0).sort(_._1 < _._1).foreach((entry)=>{
			count += entry._2
			if (count >= load_per_bin) { boundaries += entry._1; count = 0 }
		})
		val boundList = boundaries.toList.sort(_<_)
		val ranges = boundList.take(nBins).zip(boundList.tail).map(b=>new DirectorKeyRange(b._1,b._2))
		println("Histogram has ranges: \n"+ranges.sort(_.minKey<_.minKey).mkString("",",",""))
		ranges.toList
	}

	def createFromRanges(ranges:Array[DirectorKeyRange],interval:WorkloadIntervalDescription, rate:Double, nBins:Int):WorkloadHistogram = {
		var t0,t1:Long = 0
		var T = scala.collection.mutable.Map[String,Long]("init"->0,"L1"->0,"L2"->0,"L3"->0,"L4"->0,"L5"->0,"XE"->0)
		
		t0 = new Date().getTime
		val requests = generateRequests(interval,rate,nBins)
		val nRequests = requests.size
		val getCounts = scala.collection.mutable.Map[DirectorKeyRange,Int]()
		val putCounts = scala.collection.mutable.Map[DirectorKeyRange,Int]()
		var ri = 0
		var range = ranges(ri)
		t1 = new Date().getTime; T("init")+=(t1-t0)

		t0 = new Date().getTime
		val typesAndKeys = (for (r <- requests) yield {
			r match {
				case g:SCADSGetRequest => { ("get",g.key.toInt) }
				case p:SCADSPutRequest => { ("put",p.key.toInt) }
				case _ => { ("na",-1) }
			}
		}).filter(_._2!= -1).toList.sort(_._2<_._2).toList
		t1 = new Date().getTime; T("L1")+=(t1-t0)

		t0 = new Date().getTime
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
		t1 = new Date().getTime; T("L2")+=(t1-t0)

		t0 = new Date().getTime
		val rangeStats = scala.collection.mutable.Map[DirectorKeyRange,WorkloadFeatures]()
		for (range <- ranges) {
			val getRate = getCounts.getOrElse(range,0).toDouble / nRequests * rate //(interval.duration/1000)
			val putRate = putCounts.getOrElse(range,0).toDouble / nRequests * rate //(interval.duration/1000)
			rangeStats += range -> WorkloadFeatures(getRate,putRate,Double.NaN)
		}
		t1 = new Date().getTime; T("L3")+=(t1-t0)
		//T.keySet.toList.sort(_<_).foreach( t => Director.logger.debug("histogram-"+t+": "+ (T(t)/1000.0) + " sec" ))

		new WorkloadHistogram(Map[DirectorKeyRange,WorkloadFeatures]()++rangeStats)
	}
	def create(interval:WorkloadIntervalDescription, rate:Double, nBins:Int,maxKey:Int): WorkloadHistogram = {
		val ranges = createRanges(interval,rate,nBins,maxKey)
		// have the ranges, now count the keys in each range
		createFromRanges(ranges.toArray,interval,rate,nBins)
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
	
	def createHistograms(workload:WorkloadDescription, interval:Long, workloadMultiplier:Double, nBins:Int, maxKey:Int):Map[Long,WorkloadHistogram] = {
		Director.resetRnd(7)
		
		val ranges = WorkloadHistogram.createEquiWidthRanges(workload.workload(0),workload.workload(0).numberOfActiveUsers*workloadMultiplier,nBins,maxKey)
		
		val histograms = scala.collection.mutable.Map[Long,WorkloadHistogram]()
		var time:Long = 0
		var endOfWorkloadInterval = 0
		
		for (w <- workload.workload) {
			endOfWorkloadInterval += w.duration
			
			while (time <= endOfWorkloadInterval) {
				val histogramRaw = WorkloadHistogram.createFromRanges(ranges.toArray,w,w.numberOfActiveUsers*workloadMultiplier,nBins)
				histograms += (time+interval) -> histogramRaw
				time += interval
				Director.logger.debug("creating histogram for time "+time+" ms ("+"%.2f".format(time/1000.0/60.0)+" min)")
			}
		}
		Map[Long,WorkloadHistogram]() ++ histograms
	}
	
	def createAndSerializeHistograms(workload:WorkloadDescription, interval:Long, file:String, workloadMultiplier:Double, nBins:Int, maxKey:Int) {
		val histograms = createHistograms(workload,interval,workloadMultiplier,nBins,maxKey)
		BinarySerialization.serialize(file,histograms)
	}
	
	def loadHistograms(file:String):Map[Long,WorkloadHistogram] = {
		val brokenHistograms = BinarySerialization.deserialize(file).asInstanceOf[Map[Long,WorkloadHistogram]]
		// hack to fix broken scala serialization
		Map( brokenHistograms.toList.map( p => ( p._1, new WorkloadHistogram( Map(p._2.rangeStats.toList:_*) ) ) ) :_* )
	}
	
	def loadHistogramsFromDBAndSerialize(dbhost:String, file:String) {
		val dbconnection = Director.connectToDatabase(dbhost)
		val statement = dbconnection.createStatement

		var startTime = -1L
		val histograms = scala.collection.mutable.Map[Long,WorkloadHistogram]()
		try {
			val result = statement.executeQuery("SELECT * FROM director.scadsstate_histogram s")
			while (result.next) {
				val time = result.getLong("time")
				val histogramString = result.getString("histogram")
				if (startTime== -1) startTime = time
				
				val h = WorkloadHistogram.loadFromCSVString(histogramString)
				histograms += (time-startTime) -> h
			}
       	} catch { case ex: SQLException => Director.logger.warn("SQL exception in metric reader",ex)}
		statement.close
		
		BinarySerialization.serialize(file, Map[Long,WorkloadHistogram]()++histograms)
	}
	
}

object BinarySerialization {
	def serialize(file:String, o:Any) {
        val out = new ObjectOutputStream( new FileOutputStream(file) )
        out.writeObject(o)
        out.close()		
	}

	def deserialize(file:String):Any = {
		val fin = new ObjectInputStream( new FileInputStream(file) )
        val o = fin.readObject()
        fin.close
        o
	}	
}

import java.beans._
object XMLSerialization {
	def serialize(file:String, o:Any) {
		val e = new XMLEncoder( new BufferedOutputStream( new FileOutputStream(file) ) )
	    e.writeObject(o)
	    e.close()
	}
	
	def deserialize(file:String):Any = {
		val decoder = new XMLDecoder( new FileInputStream(file) )
		val o = decoder.readObject()
		decoder.close()
		o
	}
}


case class SCADSStateHistory(
	period:Long,
	metricReader:MetricReader,
	placementIP:String,
	policy:Policy
) {	
	val history = new scala.collection.mutable.HashMap[Long,SCADSState] with scala.collection.mutable.SynchronizedMap[Long,SCADSState]
	var lastInterval:Long = -1
	var updaterThread:Thread = null
	
	var maxLag:Long = 1*60*1000
	
	def getMostRecentState:SCADSState = if (lastInterval== -1) null else history(lastInterval)
	
	var costFunction:FullCostFunction = null
	
	def setCostFunction(costFunction:FullCostFunction) { this.costFunction = costFunction }
	
	def startUpdating {
		val updater = StateUpdater()
		updaterThread = new Thread(updater)
		updaterThread.start
	}
	
	def stopUpdating { if (updaterThread!=null) updaterThread.stop }
	
	case class StateUpdater() extends Runnable {
		def run() {
			var nextUpdateTime = new Date().getTime/period*period + period			
			
			while(true) {
				val state = SCADSState.refreshAtTime(metricReader,placementIP,nextUpdateTime)
				if (state!=null) {
					Director.logger.debug("updated state for time "+new Date(nextUpdateTime)+" ("+nextUpdateTime+")")
					SCADSState.dumpConfig(state.time,state.config)
					history += nextUpdateTime -> state
					if (costFunction!=null) costFunction.addState(state)
					policy.periodicUpdate(state)
					lastInterval = nextUpdateTime
					nextUpdateTime += period
				} else {
					Thread.sleep(period/3)
					//Director.logger.debug("trying to update state of "+new Date(nextUpdateTime)+" ("+nextUpdateTime+") at "+new Date()+" but don't have data yet")
				}
				
				if ( (new Date().getTime - nextUpdateTime)>maxLag ) {
					//Director.logger.info("couldn't update state of "+new Date(nextUpdateTime)+" ("+nextUpdateTime+") for too long; moving on")
					nextUpdateTime += period
				}
			}
			
		}
	}
}
