package scads.director

import java.util.Date

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

object PerformanceMetrics {
	def load(metricReader:MetricReader, server:String, reqType:String):PerformanceMetrics = {
		// FIX: handling of the dates
		val (date0, workload) = metricReader.getSingleMetric(server, "workload", reqType)
		val (date1, latencyMean) = metricReader.getSingleMetric(server, "latency_mean", reqType)
		val (date2, latency90p) = metricReader.getSingleMetric(server, "latency_90p", reqType)
		val (date3, latency99p) = metricReader.getSingleMetric(server, "latency_99p", reqType)
		new PerformanceMetrics(date0,metricReader.interval.toInt,workload,latencyMean,latency90p,latency99p)
	}
	
	def estimateFromSamples(samples:List[Double], time:Date, aggregationInterval:Int):PerformanceMetrics = {
		val workload = computeWorkload(samples)/aggregationInterval
		val latencyMean = computeMean(samples)
		val latency90p = computeQuantile(samples,0.9)
		val latency99p = computeQuantile(samples,0.99)
		PerformanceMetrics(time, aggregationInterval, workload, latencyMean, latency90p, latency99p)
	}
	
	private def computeWorkload( data:List[Double] ): Double = if (data==Nil||data.size==0) Double.NaN else data.length
	private def computeMean( data:List[Double] ): Double = if (data==Nil) Double.NaN else data.reduceLeft(_+_)/data.length
    private def computeQuantile( data:List[Double], q:Double): Double = if (data==Nil) Double.NaN else data.sort(_<_)( Math.floor(data.length*q).toInt )
}
case class PerformanceMetrics(
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

case class MetricReader(
	val host: String,
	val db: String,
	val interval: Double,
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
		if (connection == null) connection = Director.connectToDatabase
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
       	} catch { case ex: SQLException => Director.logger.warn("SQL exception in metric reader",ex)}
		finally {statement.close}
		(time,value)
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
