package scads.director

import java.util.Date

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

// ??
class KeyRange(
	val minKey: String,
	val maxKey: String
) {
	override def toString():String = "["+minKey+","+maxKey+")"
}

object PerformanceMetrics {
	def load(metricReader:MetricReader, server:String, reqType:String):PerformanceMetrics = {
		// FIX: handling of the dates
		val (date0, workload) = metricReader.getSingleMetric(server, "workload", reqType)
		val (date1, latencyMean) = metricReader.getSingleMetric(server, "latency_mean", reqType)
		val (date2, latency90p) = metricReader.getSingleMetric(server, "latency_90p", reqType)
		val (date3, latency99p) = metricReader.getSingleMetric(server, "latency_99p", reqType)
		new PerformanceMetrics(date0,metricReader.interval.toInt,workload,latencyMean,latency90p,latency99p)
	}
}
class PerformanceMetrics(
	val time: Date,
	val aggregationInterval: Int,  // in seconds
	val workload: Double,
	val latencyMean: Double,
	val latency90p: Double,
	val latency99p: Double
) {
	override def toString():String = time+" w="+"%.2f".format(workload)+" lMean="+"%.2f".format(latencyMean)+" l90p="+"%.2f".format(latency90p)+" l99p="+"%.2f".format(latency99p)
	def toShortLatencyString():String = "%.0f".format(latencyMean)+"/"+"%.0f".format(latency90p)+"/"+"%.0f".format(latency99p)
}

// add multiple namespaces per node?
class StorageNodeState(
	val ip: String,
	val metrics: PerformanceMetrics,
	val metricsByType: Map[String,PerformanceMetrics],
	val range: KeyRange
) {
	override def toString():String = "server@"+ip+" range="+range.toString+" \n   all=["+metrics.toString+"]\n"+metricsByType.map(e => "   "+e._1+"=["+e._2.toString+"]").mkString("","\n","")
	def toShortString():String = "server@"+"%-45s".format(ip)+" range="+"%-20s".format(range.toString)+
									" W="+"%-20s".format(metrics.workload.toInt+"/"+metricsByType("get").workload.toInt+"/"+metricsByType("put").workload.toInt)+ 
									" getL="+"%-15s".format(metricsByType("get").toShortLatencyString())+" putL="+"%-15s".format(metricsByType("put").toShortLatencyString())
}

object SCADSState {
	def refresh(metricReader:MetricReader, placementServerIP:String): SCADSState = {
		
		val reqTypes = List("get","put")
		val servers = metricReader.getAllServers-"ALL"
		
		val nodes = for (server <- servers) yield {
			val ip = server
			val range = new KeyRange("","")
			val sMetrics = PerformanceMetrics.load(metricReader,server,"ALL")
			val sMetricsByType = reqTypes.map( (t) => t -> PerformanceMetrics.load(metricReader,server,t)).foldLeft(Map[String, PerformanceMetrics]())((x,y) => x + y)	
			new StorageNodeState(ip,sMetrics,sMetricsByType,range)
		}
		
		val metrics = PerformanceMetrics.load(metricReader,"ALL","ALL")
		val metricsByType = reqTypes.map( (t) => t -> PerformanceMetrics.load(metricReader,"ALL",t)).foldLeft(Map[String,PerformanceMetrics]())((x,y)=>x+y)
		new SCADSState(new Date(), nodes, metrics, metricsByType)
	}
}

class SCADSState(
	val time: Date,
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


case class MetricReader(
	val host: String,
	val db: String,
	val interval: Double,
	val report_prob: Double
) {
	val port = 6000
	val user = "root"
	val pass = ""
	
	var connection: Connection = null
	
	def connectToDatabase() {
        // open connection to the database
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance()
        } catch {
			case ex: Exception => ex.printStackTrace() }

        try {
            val connectionString = "jdbc:mysql://" + host + "/?user=" + user + "&password=" + pass
            connection = DriverManager.getConnection(connectionString)
		} catch {
			case ex: SQLException => {
            	// handle any errors
	            println("can't connect to the database")
	            println("SQLException: " + ex.getMessage)
	            println("SQLState: " + ex.getSQLState)
	           	println("VendorError: " + ex.getErrorCode)
	        }
		}

        // create database if it doesn't exist and select it
        try {
            val statement = connection.createStatement
            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + db)
            statement.executeUpdate("USE " + db)
       	} catch { case ex: SQLException => ex.printStackTrace() }

        println("have connection to database")
    }

	def getWorkload(host:String):Double = {
		if (connection == null) connectToDatabase
		val workloadSQL = "select time,value from scads,scads_metrics where scads_metrics.server=\""+host+"\" and request_type=\"ALL\" and stat=\"workload\" and scads.metric_id=scads_metrics.id order by time desc limit 10"
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
	
	def getSingleMetric(host:String, metric:String, reqType:String):(java.util.Date,Double) = {
		if (connection == null) connectToDatabase
		val workloadSQL = "select time,value from scads,scads_metrics where scads_metrics.server=\""+host+"\" and request_type=\""+reqType+"\" and stat=\""+metric+"\" and scads.metric_id=scads_metrics.id order by time desc limit 1"
		var time:java.util.Date = null
		var value = Double.NaN
        val statement = connection.createStatement
		try {
			val result = statement.executeQuery(workloadSQL)
			val set = result.first // set cursor to first row
			if (set) {
				time = new java.util.Date(result.getLong("time"))
				value = if (metric=="workload") (result.getString("value").toDouble/interval/report_prob) else result.getString("value").toDouble
			}
       	} catch { case ex: SQLException => }
		finally {statement.close}
		(time,value)
	}
	
	def getAllServers():List[String] = {
		if (connection == null) connectToDatabase
		val workloadSQL = "select distinct server from scads_metrics"
		var servers = new scala.collection.mutable.ListBuffer[String]()
        val statement = connection.createStatement
		try {
			val result = statement.executeQuery(workloadSQL)
			while (result.next) servers += result.getString("server")
       	} catch { case ex: SQLException => }
		finally {statement.close}
		servers.toList
	}

}
