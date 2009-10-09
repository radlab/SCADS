package scads.director

import java.io._
import java.net._
import java.util.regex._
import java.util.Date
import scala.collection.jcl.Conversions._

import org.apache.thrift._
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import radlab.metricservice._

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

case class StringKeyRange(
	minKey:String,
	maxKey:String
)

class WorkloadStats() {
	var nGets = 0
	var nPuts = 0

	def addGet { nGets += 1 }
	def addPut { nPuts += 1 }
	def adjust(fraction:Double) { 
		nGets = (nGets/fraction).toInt 
		nPuts = (nPuts/fraction).toInt
	}
	
	override def toString():String = "("+nGets+","+nPuts+")"
}

object ParseXtraceReports {
	val host = try { System.getProperty("chukwaHost") } catch { case _ => "r16.millennium.berkeley.edu" }
	val port = try { System.getProperty("chukwaPort").toInt } catch { case _ => 9094 }
	
	val keyFormat = new java.text.DecimalFormat("000000000000000")
	
	val samplingProbability = try { System.getProperty("samplingProbability").toDouble } catch { case _ => 1.0 }
	
	val histogramDir = "/tmp/histograms/"
	val histogramBreaksUpdateInterval = 24*60*60*1000
	val nHistogramBreaksPerServer = 20
	val nBins = try { System.getProperty("nBins").toInt } catch { case _ => 0 }
	var minKey = try { System.getProperty("minKey") } catch { case _ => "000000" }
	var maxKey = try { System.getProperty("maxKey") } catch { case _ => "000000" }
	val histogramDBHost = try { System.getProperty("histogramDBHost") } catch { case _ => "localhost" }
	val histogramDBUser = "root"
	val histogramDBPassword = ""
	val histogramDBName = "director"
	val histogramDBTable = "scadsstate_histogram"
	val histogramDBConnection = if (nBins>0) 
									connectToHistogramDatabase(histogramDBHost, histogramDBUser, histogramDBPassword, histogramDBName, histogramDBTable) 
								else null
	var histogramRange = if (nBins>0)
							(1 to nBins).
								map(_.toDouble).
								map( (i:Double) => ( Math.floor((i-1)/nBins*(maxKey.toInt-minKey.toInt)+minKey.toInt).toInt, Math.floor(i/nBins*(maxKey.toInt-minKey.toInt)+minKey.toInt).toInt) ).
								map( (p) => StringKeyRange( keyFormat.format(p._1), keyFormat.format(p._2)) ).
								toList
						else null
						
	println( histogramRange )
	
	// aggregation interval in milliseconds
	val aggregationInterval = try { System.getProperty("aggregationInterval").toLong } catch { case _ => println("missing aggregationInterval"); exit(-1) }
	
	// connect to MetricService
	val metricServiceHost = System.getProperty("MSHost")
	val metricServicePort = System.getProperty("MSPort").toInt
	val metricService = connectToMetricService(metricServiceHost,metricServicePort)

	var histogramRanges = scala.collection.mutable.Map(0->List[StringKeyRange](), 1->List[StringKeyRange](), 2->List[StringKeyRange]())
	var currentHistogram = 0
	var nServers = 1	
	(new File(histogramDir)).mkdirs()
	
	val detailsPattern = Pattern.compile("^.*(RequestDetails: )(.*\\n).*", Pattern.MULTILINE)
	val timePattern = Pattern.compile("^.*(Timestamp: )(.*\\n).*", Pattern.MULTILINE)
	
	var requests = new scala.collection.mutable.ListBuffer[SCADSRequestStats]()

	var lastInterval: Long = new Date().getTime / aggregationInterval * aggregationInterval
	var lastHistogramBreaksUpdate = new Date().getTime / histogramBreaksUpdateInterval * histogramBreaksUpdateInterval - histogramBreaksUpdateInterval
	
	def main(args: Array[String]) {
			
		val s = new Socket(host, port)
		s.getOutputStream().write("RAW datatype=XTrace\n".getBytes())
		
		val dis = new DataInputStream(s.getInputStream())
		dis.readFully(new Array[Byte](3)) //read "OK\n"
	
		while(true) {
			val len = dis.readInt()
			val data = new Array[Byte](len)
			dis.readFully(data)
			val report = new String(data)
			
			val now = new Date().getTime()
			if (now > lastInterval + aggregationInterval) {
				lastInterval += aggregationInterval
				
/*				// update histogram breaks?
				println(now+"  "+ (lastHistogramBreaksUpdate + histogramBreaksUpdateInterval))
				if (now > lastHistogramBreaksUpdate + histogramBreaksUpdateInterval) {
					println("updating histogram breaks")
					updateHistogramBreaks(requests.toArray)
					lastHistogramBreaksUpdate += histogramBreaksUpdateInterval
				}				
				computeAndStoreRollingHistograms(lastInterval,requests.toList)
*/				
				val updates = processInterval(requests.toList,lastInterval)
				updates.foreach( println(_) )
				metricService.update( s2jList(updates) )

				if (nBins>0) computeAndStoreHistogram(lastInterval,requests.toList,histogramRange)

				requests = new scala.collection.mutable.ListBuffer[SCADSRequestStats]()
			}
			
			// advance lastInterval up to now (in case we had a few intervals with no data)
			while (lastInterval + aggregationInterval < now)
				lastInterval += aggregationInterval

			processReport(report)
		}
	}
	
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
	
	def computeAndStoreRollingHistograms(time:Long, reqs:List[SCADSRequestStats]) = {
		for (hr <- histogramRanges) {
			if (hr._2.size>0) {
				println("computing histogram "+hr._1)
				val hist = computeHistogram(reqs,hr._2.toArray)
				storeHistogramToFile(hist,time)
			} else 
				println("skipping histogram "+hr._1)
		}
	}
	
	def computeAndStoreHistogram(time:Long, reqs:List[SCADSRequestStats], histogramRange:List[StringKeyRange]) = {
		println("computing histogram")
		val hist = computeHistogram(reqs,histogramRange.toArray)
		storeHistogram(hist,time)
	}
	
	def storeHistogram(histogram:Map[StringKeyRange,WorkloadStats], time:Long) {
		histogram.map( (p)=> p._1.toString+"->"+p._2.toString+"  " ).foreach( print(_) ); println
		
		val histCSV = histogramToCSV(histogram)
		val statement = histogramDBConnection.createStatement
		val histogramSQL = createInsertStatement("scadsstate_histogram", Map("time"->time.toString,"histogram"->("'"+histCSV+"'")))
		statement.executeUpdate(histogramSQL)
		statement.close	
	}
	
	def histogramToCSV(histogram:Map[StringKeyRange,WorkloadStats]):String = {
	    "minKey,maxKey,getRate,putRate,getsetRate\n"+
		histogram.map(r=>(r._1,r._2)).toList.
			sort(_._1.minKey<_._1.minKey).
			map( r=> r._1.minKey+","+r._1.maxKey+","+r._2.nGets.toDouble/aggregationInterval*1000+","+r._2.nPuts.toDouble/aggregationInterval*1000+",0.0\n" ).
			mkString("")
	}
	
	def storeHistogramToFile(histogram:Map[StringKeyRange,WorkloadStats], time:Long) {
	    val out = new BufferedWriter(new FileWriter(histogramDir+"/histogram_"+time+".csv"));
	    out.write( histogramToCSV(histogram) );
		out.close
	}
	
	def computeHistogram(reqs:List[SCADSRequestStats], ranges:Array[StringKeyRange]):Map[StringKeyRange,WorkloadStats] = {
		var ri = 0
		var range = ranges(ri)

		// create an empty histogram
		val hist = scala.collection.mutable.Map[StringKeyRange,WorkloadStats]()
		ranges.foreach( hist(_)=new WorkloadStats )
		
		val firstKey = ranges(0).minKey
		val lastKey = ranges(ranges.size-1).maxKey

		for (r <- reqs.sort(_.key<_.key).toList) { 
			if (r.key>=firstKey&&r.key<=lastKey) {
				while (r.key>range.maxKey) { ri = ri+1; /*println("next range: "+ri+" key="+key+"  max="+range.maxKey);*/ range = ranges(ri); /*hist(range)=new WorkloadStats*/ }
				r.reqType match {
					case "get" => hist(range).addGet
					case "put" => hist(range).addPut
					case _ =>
				}
			}
		}
		hist.values.foreach(_.adjust(samplingProbability))
		Map[StringKeyRange,WorkloadStats]()++hist
	}
	
	def updateHistogramBreaks(reqs:Array[SCADSRequestStats]) {
		if (reqs.size>0 && minKey!="" && maxKey!="") {
			val rnd = new java.util.Random()
			val nBreaks = nHistogramBreaksPerServer * nServers
			val breaks = (List(minKey)++(for (i <- 1 to nBreaks-1) yield { reqs(rnd.nextInt(reqs.size)).key })++List(maxKey)).sort(_<_)
			val ranges = breaks.take(nBreaks).zip(breaks.tail).map(x=>StringKeyRange(x._1,x._2))
		
			currentHistogram = (currentHistogram+1)%histogramRanges.size
			histogramRanges(currentHistogram) = ranges
			val histToDelete = (currentHistogram+1)%histogramRanges.size
			histogramRanges(histToDelete) = List[StringKeyRange]()
			println("currentI: "+currentHistogram)
			println("ranges: "+ranges)
			println(histogramRanges.keySet)
			println("current: "+histogramRanges(currentHistogram))
		}
	}
	
	def processReport(report:String) {
		val m = detailsPattern.matcher(report)
		if (m.find) {
			val values = m.group(2).split(",")
			val requestType = values(0)
			val key = values(1)
			val serverIP = values(2)
			val latency = values(3).toDouble
			
			if (key<minKey||minKey=="") minKey=key
			if (key>maxKey||maxKey=="") maxKey=key
			
			val m2 = timePattern.matcher(report)
			val timestamp = if (m2.find) m2.group(2).toDouble else 0.0
			
			//println("request: type="+requestType+"   key="+key+"  serverIP="+serverIP+"  latency="+latency)			
			requests += new SCADSRequestStats(requestType,timestamp,serverIP,latency,key)
		}
	}

	def processInterval(requests:List[SCADSRequestStats], interval:Long): List[MetricUpdate] = {
		val allServers = requests.map(_.serverIP).removeDuplicates
		val allRequestTypes = requests.map(_.reqType).removeDuplicates

		var metrics = new scala.collection.mutable.ListBuffer[MetricUpdate]()
		metrics ++= computeRequestMetrics(requests,"ALL","ALL",interval)
		for (server <- allServers) {
			metrics ++= computeRequestMetrics(requests.filter(_.serverIP==server),server,"ALL",interval)
			for (reqType <- allRequestTypes)
				metrics ++= computeRequestMetrics(requests.filter((r:SCADSRequestStats) => r.serverIP==server && r.reqType==reqType),server,reqType,interval)
		}
		nServers = allServers.size
		for (reqType <- allRequestTypes) 
			metrics ++= computeRequestMetrics(requests.filter(_.reqType==reqType),"ALL",reqType,interval)
		metrics.toList
	}
	
	def computeRequestMetrics(requests:List[SCADSRequestStats], server:String, requestType:String, interval:Long): List[MetricUpdate] = {
		var metrics = new scala.collection.mutable.ListBuffer[MetricUpdate]()
		metrics += new MetricUpdate(interval,new MetricDescription("scads",s2jMap(Map("aggregation"->aggregationInterval.toString,"server"->server,"request_type"->requestType,"stat"->"workload"))),(requests.length.toDouble/aggregationInterval*1000/samplingProbability).toString)
		metrics += new MetricUpdate(interval,new MetricDescription("scads",s2jMap(Map("aggregation"->aggregationInterval.toString,"server"->server,"request_type"->requestType,"stat"->"latency_mean"))),computeMean(requests.map(_.latency)).toString)
		metrics += new MetricUpdate(interval,new MetricDescription("scads",s2jMap(Map("aggregation"->aggregationInterval.toString,"server"->server,"request_type"->requestType,"stat"->"latency_50p"))),computeQuantile(requests.map(_.latency),0.50).toString)
		metrics += new MetricUpdate(interval,new MetricDescription("scads",s2jMap(Map("aggregation"->aggregationInterval.toString,"server"->server,"request_type"->requestType,"stat"->"latency_90p"))),computeQuantile(requests.map(_.latency),0.90).toString)
		metrics += new MetricUpdate(interval,new MetricDescription("scads",s2jMap(Map("aggregation"->aggregationInterval.toString,"server"->server,"request_type"->requestType,"stat"->"latency_99p"))),computeQuantile(requests.map(_.latency),0.99).toString)
		metrics += new MetricUpdate(interval,new MetricDescription("scads",s2jMap(Map("aggregation"->aggregationInterval.toString,"server"->server,"request_type"->requestType,"stat"->"n_requests"))),(requests.length/samplingProbability).toInt.toString)
		metrics += new MetricUpdate(interval,new MetricDescription("scads",s2jMap(Map("aggregation"->aggregationInterval.toString,"server"->server,"request_type"->requestType,"stat"->"n_slower_50ms"))),(requests.filter(_.latency>50).length/samplingProbability).toInt.toString)
		metrics += new MetricUpdate(interval,new MetricDescription("scads",s2jMap(Map("aggregation"->aggregationInterval.toString,"server"->server,"request_type"->requestType,"stat"->"n_slower_100ms"))),(requests.filter(_.latency>100).length/samplingProbability).toInt.toString)		
		metrics.toList
	}
	
	def computeMean( data: List[Double] ): Double = if (data==Nil) Double.NaN else data.reduceLeft(_+_)/data.length
	def computeQuantile( data: List[Double], q: Double): Double = if (data==Nil) Double.NaN else data.sort(_<_)( Math.floor(data.length*q).toInt )
	
	def connectToHistogramDatabase(databaseHost:String, databaseUser:String, databasePassword:String, databaseName:String, databaseTable:String):Connection = {
	    try {
	        Class.forName("com.mysql.jdbc.Driver").newInstance()
	    } catch { case ex: Exception => ex.printStackTrace() }

		var connection:Connection = null
		while (connection==null) {
		    try {
		        val connectionString = "jdbc:mysql://" + databaseHost + "/?user=" + databaseUser + "&password=" + databasePassword
				println("connecting to database: "+connectionString)
		        connection = DriverManager.getConnection(connectionString)
			} catch {
				case ex: SQLException => {
		            println("can't connect to the database")
		            println("SQLException: " + ex.getMessage)
		            println("SQLState: " + ex.getSQLState)
		           	println("VendorError: " + ex.getErrorCode)
					
					println("will wait 10 seconds and try again")
					Thread.sleep(10*1000)
		        }
			}
		}

		try {
			val statement = connection.createStatement
			statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + databaseName)
			statement.executeUpdate("USE " + databaseName)
	        statement.executeUpdate("CREATE TABLE IF NOT EXISTS "+databaseTable+" (`id` INT NOT NULL AUTO_INCREMENT, `time` BIGINT, `histogram` TEXT, PRIMARY KEY(`id`) ) ")
			statement.close
		} catch { case ex: SQLException => ex.printStackTrace() }
		connection
	}

	def createInsertStatement(table:String, data:Map[String,String]):String = {
		val colnames = data.keySet.toList
		"INSERT INTO "+table+" ("+colnames.mkString("`","`,`","`")+") values ("+colnames.map(data(_)).mkString(",")+")"
	}

	def s2jList[T](list:List[T]): java.util.ArrayList[T] = {
		var jl = new java.util.ArrayList[T]()
		list.foreach(jl.add(_))
		jl
	}

	def s2jMap[K,V](map:Map[K,V]): java.util.HashMap[K,V] = {	
		var jm = new java.util.HashMap[K,V]()
		map.foreach( t => jm.put(t._1,t._2) )
		jm
	}
}

class SCADSRequestStats(
	val reqType: String,
	val timestamp: Double,	// in seconds
	val serverIP: String,
	val latency: Double,    // in milliseconds
	val key: String
)
