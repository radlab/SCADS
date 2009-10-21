package scads.director

import java.util.Date

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

import radlab.metricservice._

import org.apache.thrift._
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

object PerformanceMetrics {
	def load(metricReader:MetricReader, server:String, reqType:String):PerformanceMetrics = {
		// FIX: handling of the dates
		val (date0, workload) = 		metricReader.getSingleMetric(server, "workload", reqType)
		val (date1, latencyMean) = 		metricReader.getSingleMetric(server, "latency_mean", reqType)
		val (date7, latency50p) = 		metricReader.getSingleMetric(server, "latency_50p", reqType)
		val (date2, latency90p) = 		metricReader.getSingleMetric(server, "latency_90p", reqType)
		val (date3, latency99p) = 		metricReader.getSingleMetric(server, "latency_99p", reqType)
		val (date4, nRequests) = 		metricReader.getSingleMetric(server, "n_requests", reqType)
		val (date5, nSlowerThan50ms) = 	metricReader.getSingleMetric(server, "n_slower_50ms", reqType)
		val (date6, nSlowerThan100ms) = metricReader.getSingleMetric(server, "n_slower_100ms", reqType)
		val (date8, nSlowerThan120ms) = metricReader.getSingleMetric(server, "n_slower_120ms", reqType)
		val (date9, nSlowerThan150ms) = metricReader.getSingleMetric(server, "n_slower_150ms", reqType)
		val (date10,nSlowerThan200ms) = metricReader.getSingleMetric(server, "n_slower_200ms", reqType)

		//PerformanceMetrics(date0.getTime,metricReader.interval.toInt,workload,latencyMean,latency90p,latency99p,
		//	(nRequests/metricReader.report_prob).toInt, (nSlowerThan50ms/metricReader.report_prob).toInt, (nSlowerThan100ms/metricReader.report_prob).toInt)
		PerformanceMetrics(date0.getTime,metricReader.interval.toInt,workload,latencyMean,latency50p,latency90p,latency99p,nRequests.toInt,
			nSlowerThan50ms.toInt, nSlowerThan100ms.toInt, nSlowerThan120ms.toInt, nSlowerThan150ms.toInt, nSlowerThan200ms.toInt)
	}

	def load(metricReader:MetricReader, server:String, reqType:String, time:Long):PerformanceMetrics = {
		// FIX: handling of the dates
		val (date0, workload) = 		metricReader.getSingleMetric(server, "workload", reqType, time)
		val (date1, latencyMean) = 		metricReader.getSingleMetric(server, "latency_mean", reqType, time)
		val (date7, latency50p) = 		metricReader.getSingleMetric(server, "latency_50p", reqType, time)
		val (date2, latency90p) = 		metricReader.getSingleMetric(server, "latency_90p", reqType, time)
		val (date3, latency99p) = 		metricReader.getSingleMetric(server, "latency_99p", reqType, time)
		val (date4, nRequests) = 		metricReader.getSingleMetric(server, "n_requests", reqType, time)
		val (date5, nSlowerThan50ms) = 	metricReader.getSingleMetric(server, "n_slower_50ms", reqType, time)
		val (date6, nSlowerThan100ms) = metricReader.getSingleMetric(server, "n_slower_100ms", reqType, time)
		val (date8, nSlowerThan120ms) = metricReader.getSingleMetric(server, "n_slower_120ms", reqType, time)
		val (date9, nSlowerThan150ms) = metricReader.getSingleMetric(server, "n_slower_150ms", reqType, time)
		val (date10,nSlowerThan200ms) = metricReader.getSingleMetric(server, "n_slower_200ms", reqType, time)

		//PerformanceMetrics(time,metricReader.interval.toInt,workload,latencyMean,latency90p,latency99p,
		//	(nRequests/metricReader.report_prob).toInt, (nSlowerThan50ms/metricReader.report_prob).toInt, (nSlowerThan100ms/metricReader.report_prob).toInt)
		PerformanceMetrics(time,metricReader.interval.toInt,workload,latencyMean,latency50p,latency90p,latency99p,nRequests.toInt,
			nSlowerThan50ms.toInt, nSlowerThan100ms.toInt, nSlowerThan120ms.toInt, nSlowerThan150ms.toInt, nSlowerThan200ms.toInt)
	}

	def estimateFromSamples(samples:List[Double], time:Long, aggregationInterval:Long, fractionOfRequests:Double):PerformanceMetrics = {
		val samplesA = samples.sort(_<_).toArray
		val workload = computeWorkload(samplesA)*1000/aggregationInterval/fractionOfRequests
		val latencyMean = computeMean(samplesA)
		val latency50p = computeQuantileAssumeSorted(samplesA,0.5)
		val latency90p = computeQuantileAssumeSorted(samplesA,0.9)
		val latency99p = computeQuantileAssumeSorted(samplesA,0.99)
		val nRequests = (samples.size/fractionOfRequests).toInt
		val nSlowerThan50ms = (samples.filter(_>50).size/fractionOfRequests).toInt
		val nSlowerThan100ms = (samples.filter(_>100).size/fractionOfRequests).toInt
		val nSlowerThan120ms = (samples.filter(_>120).size/fractionOfRequests).toInt
		val nSlowerThan150ms = (samples.filter(_>150).size/fractionOfRequests).toInt
		val nSlowerThan200ms = (samples.filter(_>200).size/fractionOfRequests).toInt
		PerformanceMetrics(time, aggregationInterval, workload, latencyMean, latency50p, latency90p, latency99p, nRequests,
			nSlowerThan50ms, nSlowerThan100ms, nSlowerThan120ms, nSlowerThan150ms, nSlowerThan200ms)
	}

	private def computeWorkload( data:Array[Double] ): Double = if (data==null||data.size==0) Double.NaN else data.length
	private def computeMean( data:Array[Double] ): Double = if (data==null||data.size==0) Double.NaN else data.reduceLeft(_+_)/data.length
    private def computeQuantile( data:List[Double], q:Double): Double = if (data==null||data.size==0) Double.NaN else data.sort(_<_).toArray( Math.floor(data.length*q).toInt )
    private def computeQuantileAssumeSorted( data:Array[Double], q:Double): Double = if (data==null||data.size==0) Double.NaN else data( Math.floor(data.length*q).toInt )


	case class RawPerfData(
		server:String,
		reqType:String,
		stat:String,
		time:Long,
		value:Double
	) {
		def metricName:String = reqType+"_"+stat
	}

	def loadDataForPerfModels(dbhost:String, aggregation:Int, file:String) {
		var connection = Director.connectToDatabase(dbhost)
        try {
            val statement = connection.createStatement
            statement.executeUpdate("USE metrics")
			statement.close
       	} catch { case ex: SQLException => ex.printStackTrace() }

		// get all servers
		val sql0 = "select distinct server from scads_metrics"
		val statement0 = connection.createStatement
		val servers = new scala.collection.mutable.ListBuffer[String]
		try {
			val result = statement0.executeQuery(sql0)
			while (result.next) servers += result.getString("server")
		} catch { case ex: SQLException => println("Couldn't get workload"); ex.printStackTrace() }
		statement0.close
		servers -= "ALL"

		val rawData = new scala.collection.mutable.ListBuffer[RawPerfData]()
		for (server <- servers) {
			val sql1 = "SELECT * FROM metrics.scads_metrics sm, metrics.scads s "+
						"where sm.id=s.metric_id and sm.server=\""+server+"\" "+
						"and sm.aggregation=\""+aggregation+"\" "+
						"and (request_type=\"get\" or request_type=\"put\") "+
						"and stat in (\"workload\",\"latency_10p\",\"latency_25p\",\"latency_50p\",\"latency_60p\",\"latency_70p\",\"latency_80p\",\"latency_90p\",\"latency_95p\",\"latency_99p\")"
			val statement1 = connection.createStatement
			try {
				val result = statement1.executeQuery(sql1)
				while (result.next) {
					rawData += RawPerfData(
								result.getString("server"),
								result.getString("request_type"),
								result.getString("stat"),
								result.getLong("time"),
								result.getDouble("value") )
				}
			} catch { case ex: SQLException => println("Can't get perf data"); ex.printStackTrace() }
			statement1.close
		}

		//rawData.toList.sort(_.time<_.time).foreach( println(_) )

		val out = new java.io.FileWriter( new java.io.File(file), false )

		val reqTypes = rawData.map(_.reqType).toList.removeDuplicates
		val times = rawData.map(_.time).toList.removeDuplicates.sort(_<_)
		val metricNames = rawData.map(_.metricName).toList.removeDuplicates
		out.write( "time,server,"+metricNames.sort(_<_).mkString(",") + "\n" )
		// aggregate data per-server
		for (server <- servers) {
			val perServer = rawData.filter(_.server==server)
			// aggregate data per-timeinterval
			for (time <- times)	{
				val metrics = scala.collection.mutable.Map[String,String]()
				metricNames.foreach( n => metrics += n -> "NaN")
				reqTypes.foreach( t => metrics += (t+"_workload") -> "0" )
				perServer.filter(_.time==time).foreach( r => metrics += r.metricName -> r.value.toString )
				out.write( time.toString+","+server+","+metrics.toList.sort(_._1<_._1).map(_._2).mkString(",") + "\n" )
			}
		}
		out.close
	}

}
case class PerformanceMetrics(
	val time: Long,
	val aggregationInterval: Long,  // in milliseconds
	val workload: Double,
	val latencyMean: Double,
	val latency50p: Double,
	val latency90p: Double,
	val latency99p: Double,
	val nRequests: Int,
	val nSlowerThan50ms: Int,
	val nSlowerThan100ms: Int,
	val nSlowerThan120ms: Int,
	val nSlowerThan150ms: Int,
	val nSlowerThan200ms: Int
) {
	override def toString():String = (new Date(time))+" w="+"%.2f".format(workload)+" lMean="+"%.2f".format(latencyMean)+" l90p="+"%.2f".format(latency90p)+" l99p="+"%.2f".format(latency99p)+
									 " all="+nRequests+" >50="+nSlowerThan50ms+" >100ms="+nSlowerThan100ms
	def toShortLatencyString():String = "%.0f".format(latencyMean)+"/"+"%.0f".format(latency90p)+"/"+"%.0f".format(latency99p)

	def createMetricUpdates(server:String, requestType:String):List[MetricUpdate] = {
		// FIX: aggregation constant
		val agg = "20000"
		var metrics = new scala.collection.mutable.ListBuffer[MetricUpdate]()
		metrics += new MetricUpdate(time,new MetricDescription("scads",s2jMap(Map("aggregation"->agg,"server"->server,"request_type"->requestType,"stat"->"workload"))),workload.toString)
		metrics += new MetricUpdate(time,new MetricDescription("scads",s2jMap(Map("aggregation"->agg,"server"->server,"request_type"->requestType,"stat"->"latency_mean"))),latencyMean.toString)
		metrics += new MetricUpdate(time,new MetricDescription("scads",s2jMap(Map("aggregation"->agg,"server"->server,"request_type"->requestType,"stat"->"latency_50p"))),latency50p.toString)
		metrics += new MetricUpdate(time,new MetricDescription("scads",s2jMap(Map("aggregation"->agg,"server"->server,"request_type"->requestType,"stat"->"latency_90p"))),latency90p.toString)
		metrics += new MetricUpdate(time,new MetricDescription("scads",s2jMap(Map("aggregation"->agg,"server"->server,"request_type"->requestType,"stat"->"latency_99p"))),latency99p.toString)
		metrics += new MetricUpdate(time,new MetricDescription("scads",s2jMap(Map("aggregation"->agg,"server"->server,"request_type"->requestType,"stat"->"n_requests"))),nRequests.toString)
		metrics += new MetricUpdate(time,new MetricDescription("scads",s2jMap(Map("aggregation"->agg,"server"->server,"request_type"->requestType,"stat"->"n_slower_50ms"))),nSlowerThan50ms.toString)
		metrics += new MetricUpdate(time,new MetricDescription("scads",s2jMap(Map("aggregation"->agg,"server"->server,"request_type"->requestType,"stat"->"n_slower_100ms"))),nSlowerThan100ms.toString)
		metrics.toList
	}

	private def s2jMap[K,V](map:Map[K,V]): java.util.HashMap[K,V] = {
		var jm = new java.util.HashMap[K,V]()
		map.foreach( t => jm.put(t._1,t._2) )
		jm
	}
}

case class MetricReader(
	val host: String,
	val db: String,
	val interval: Long,
	val report_prob: Double
) {
	val port = 6000
	val user = "root"
	val pass = ""

	var connection = Director.connectToDatabase
	initDatabase

	def initDatabase() {
        // create database if it doesn't exist and select it
        try {
            val statement = connection.createStatement
            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + db)
            statement.executeUpdate("USE " + db)
       	} catch { case ex: SQLException => ex.printStackTrace() }
    }

	def getWorkload(host:String):Double = {
		if (connection == null) connection = Director.connectToDatabase
		val workloadSQL = "select time,value from scads,scads_metrics where scads_metrics.server=\""+host+"\" and request_type=\"ALL\" and stat=\"workload\" and aggregation=\""+interval+"\" and scads.metric_id=scads_metrics.id order by time desc limit 10"
		var value = Double.NaN
        val statement = connection.createStatement
		try {
			val result = statement.executeQuery(workloadSQL)
			val set = result.first // set cursor to first row
			if (set) value = (result.getLong("value")/interval/report_prob).toDouble
       	} catch { case ex: SQLException => println("Couldn't get workload"); ex.printStackTrace() }
		finally {statement.close}
		value
	}

	def haveDataForTime(time:Long):Boolean = {
		if (connection == null) connection = Director.connectToDatabase
		val sql = "SELECT * FROM director.scadsstate_histogram s WHERE time="+time
        val statement = connection.createStatement

		var haveData = false
		try {
			val result = statement.executeQuery(sql)
			val set = result.first // set cursor to first row
			if (set) haveData = true
       	} catch { case ex: SQLException => Director.logger.warn("SQL exception in metric reader",ex)}
		finally {statement.close}
		haveData
	}

	def getSingleMetric(host:String, metric:String, reqType:String):(java.util.Date,Double) = {
		if (connection == null) connection = Director.connectToDatabase
		//val workloadSQL = "select time,value from scads,scads_metrics where scads_metrics.server=\""+host+"\" and request_type=\""+reqType+"\" and stat=\""+metric+"\" and scads.metric_id=scads_metrics.id order by time desc limit 1"
		val workloadSQL = "select time,value from scads,scads_metrics where scads_metrics.server=\""+host+"\" and request_type=\""+reqType+"\" and stat=\""+metric+"\" and aggregation=\""+interval+"\" and scads.metric_id=scads_metrics.id order by time desc limit 1"
		var time:java.util.Date = null
		var value = Double.NaN
        val statement = connection.createStatement
		try {
			val result = statement.executeQuery(workloadSQL)
			val set = result.first // set cursor to first row
			if (set) {
				time = new java.util.Date(result.getLong("time"))
				value = if (metric=="workload") (result.getString("value").toDouble/report_prob)
						else if (metric=="n_requests"||metric=="n_slower_50ms"||metric=="n_slower_100ms") (result.getString("value").toDouble/report_prob).toInt
						else result.getString("value").toDouble
			}
       	} catch { case ex: SQLException => Director.logger.warn("SQL exception in metric reader",ex)}
		finally {statement.close}
		(time,value)
	}

	def getSingleMetric(host:String, metric:String, reqType:String, time:Long):(Long,Double) = {
		if (connection == null) connection = Director.connectToDatabase
		val workloadSQL = "select time,value from scads,scads_metrics where scads_metrics.server=\""+host+
									"\" and request_type=\""+reqType+
									"\" and stat=\""+metric+
									"\" and aggregation=\""+interval+
									"\" and time=\""+time+
									"\" and scads.metric_id=scads_metrics.id order by time desc limit 1"
		var value = Double.NaN
        val statement = connection.createStatement
		try {
			val result = statement.executeQuery(workloadSQL)
			val set = result.first // set cursor to first row
			if (set) {
				value = if (metric=="workload") (result.getString("value").toDouble/report_prob)
						else if (metric=="n_requests"||metric=="n_slower_50ms"||metric=="n_slower_100ms") result.getString("value").toDouble/report_prob
						else result.getString("value").toDouble
			}
       	} catch { case ex: SQLException => Director.logger.warn("SQL exception in metric reader",ex)}
		finally {statement.close}
		(time,value)
	}

	def getSingleMetric(host:String, metric:String, reqType:String, time0:Long, time1:Long):Map[Long,Double] = {
		if (connection == null) connection = Director.connectToDatabase
		val workloadSQL = "select time,value from scads,scads_metrics where scads_metrics.server=\""+host+
									"\" and request_type=\""+reqType+
									"\" and stat=\""+metric+
									"\" and aggregation=\""+interval+
									"\" and time>=\""+time0+
									"\" and time<=\""+time1+
									"\" and scads.metric_id=scads_metrics.id order by time"
		var value = Double.NaN
        val statement = connection.createStatement
		val values = scala.collection.mutable.Map[Long,Double]()
		try {
			val result = statement.executeQuery(workloadSQL)
			while (result.next) values += result.getLong("time") -> result.getString("value").toDouble
       	} catch { case ex: SQLException => Director.logger.warn("SQL exception in metric reader",ex)}
		finally {statement.close}
		Map[Long,Double]() ++ values
	}

	def getAllServers():List[String] = {
		if (connection == null) connection = Director.connectToDatabase
		val workloadSQL = "select distinct server from scads_metrics"
		var servers = new scala.collection.mutable.ListBuffer[String]()
        val statement = connection.createStatement
		try {
			val result = statement.executeQuery(workloadSQL)
			while (result.next) servers += result.getString("server")
       	} catch { case ex: SQLException => Director.logger.warn("SQL exception in metric reader",ex)}
		finally {statement.close}
		servers.toList
	}

}


case class ThriftMetricDBConnection (
	val host: String,
	val port: Int
) {
	val metricService = connectToMetricService(host,port)

	def connectToMetricService(host:String, port:Int): MetricServiceAPI.Client = {
		System.err.println("using MetricService at "+host+":"+port)

		var metricService: MetricServiceAPI.Client = null

        while (metricService==null) {
            try {
                val metricServiceTransport = new TSocket(host,port);
                val metricServiceProtocol = new TBinaryProtocol(metricServiceTransport);

                metricService = new MetricServiceAPI.Client(metricServiceProtocol);
                metricServiceTransport.open();
                System.err.println("connected to MetricService")

            } catch {
            	case e:Exception => {
	                e.printStackTrace
	                println("can't connect to the MetricService, waiting 60 seconds");
	                try {
	                    Thread.sleep(60 * 1000);
	                } catch {
	                    case e1:Exception => e1.printStackTrace()
	                }
                }
            }
        }
        metricService
	}

}
