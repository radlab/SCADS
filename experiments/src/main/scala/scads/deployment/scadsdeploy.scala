package scads.deployment

import deploylib._ /* Imports all files in the deployment library */
import org.json.JSONObject
import org.json.JSONArray
import scala.collection.jcl.Conversions._

import java.io._
import org.apache.log4j._
import org.apache.log4j.Level._

import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.thrift.{RangeSet,RecordSet,KnobbedDataPlacementServer,DataPlacement, RangeConversion}
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}

object ScadsDeploy {
	val keyFormat = new java.text.DecimalFormat("000000000000000")
	val server_port = 9000
	val server_sync = 9091
	val dp_port = 8000
	val serversName = "servers"
	val placeName = "placement"
	val placementInfoFile = "/tmp/scads_info.csv"
	val restrictFileName = "restrict.csv"
	var maxKey = 10000 // up to ~1920000 in cache

	var requestSamplingProbability = 0.02
	
	var logger:Logger = null

	def initLogger {
		if (logger==null) {
			Logger.getRootLogger.removeAllAppenders
			Logger.getRootLogger.addAppender( new varia.NullAppender() )
			logger = Logger.getLogger("scads.deploy")
			logger.addAppender( new FileAppender(new PatternLayout("%d %5p %c - %m%n"),"/tmp/deploy.txt",false) )
			logger.addAppender( new ConsoleAppender(new PatternLayout("%d %5p %c - %m%n")) )
			logger.setLevel(DEBUG)
			logger.debug("starting logging of deployment")
		}
	}

	val adaptors = Array[String](
		"add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Top 15000 /usr/bin/top -b -n 1 -c 0",
           "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Df 60000 /bin/df -x nfs -x none 0",
           "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Sar 1000 /usr/bin/sar -q -r -n ALL 55 0",
           "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Iostat 1000 /usr/bin/iostat -x -k 55 2 0",
           "add edu.berkeley.chukwa_xtrace.XtrAdaptor XTrace TcpReportSource 0",
           "add edu.berkeley.chukwa_xtrace.XtrAdaptor XTrace UdpReportSource 0"
		)
	val xtrace_adaptors = new JSONArray(adaptors)
	val xtraceConfig = new JSONObject()
	xtraceConfig.put("adaptors",xtrace_adaptors)

	def getXtraceConfig = xtraceConfig

	def getCollectorConfig:JSONObject = {
		val collectorConfig = new JSONObject()
		val collectorRecipes = new JSONArray()
	    collectorRecipes.put("chukwa::collector")
	    collectorConfig.put("recipes", collectorRecipes)
	}

	def getXtraceIntoConfig(collector_dns:String):JSONObject = {
		val scads_xtrace = new JSONObject()
		scads_xtrace.put("xtrace","-x")
		val serverConfig = new JSONObject()
		serverConfig.put("scads",scads_xtrace);
		val collector = Array[String](collector_dns)
		val xtrace_collector= new JSONArray(collector)
		val addedConfig = getXtraceConfig
		addedConfig.put("collectors",xtrace_collector)
		serverConfig.put("chukwa",addedConfig);
	}

	def getDataPlacementHandle(h:String,xtrace_on:Boolean):KnobbedDataPlacementServer.Client = {
		val p = dp_port
		var haveDPHandle = false
		var dpclient:KnobbedDataPlacementServer.Client = null
		while (!haveDPHandle) {
			try {
				val transport = new TFramedTransport(new TSocket(h, p))
		   		val protocol = if (xtrace_on) {new XtBinaryProtocol(transport)} else {new TBinaryProtocol(transport)}
		   		dpclient = new KnobbedDataPlacementServer.Client(protocol)
				transport.open()
				haveDPHandle = true
			} catch {
				case e: Exception => { ScadsDeploy.logger.debug("don't have connection to placement server, waiting 1 second"); Thread.sleep(1000) }
			}
		}
		dpclient
	}

	def getNumericKey(key:String) = {
		key.toInt
	}
}

case class ScadsDP(h:String, xtrace_on: Boolean, namespace: String) extends RangeConversion {
	var dpclient:KnobbedDataPlacementServer.Client = null

	// doesn't check that keys don't fall out of range of server responsibility
	def copy_data(from:String,to:String,startkey:Int,endkey:Int):(Long,Long) = {
		val start = new StringKey( ScadsDeploy.keyFormat.format( startkey ) )
		val end = new StringKey( ScadsDeploy.keyFormat.format( endkey ) )
		val range = new RangeSet()
		range.setStart_key(start.serialize)
		range.setEnd_key(end.serialize)
		val rset = new RecordSet(3,range,null,null)
		refreshHandle // get placement handle

		val startms = System.currentTimeMillis()
		dpclient.copy(namespace,rset, from, ScadsDeploy.server_port,ScadsDeploy.server_sync, to, ScadsDeploy.server_port,ScadsDeploy.server_sync)
		(startms,System.currentTimeMillis())
	}

	def move_data(from:String,to:String,startkey:Int,endkey:Int):(Long,Long) = {
		val start = new StringKey( ScadsDeploy.keyFormat.format( startkey ) )
		val end = new StringKey( ScadsDeploy.keyFormat.format( endkey ) )
		val range = new RangeSet()
		range.setStart_key(start.serialize)
		range.setEnd_key(end.serialize)
		val rset = new RecordSet(3,range,null,null)
		refreshHandle // get placement handle

		val startms = System.currentTimeMillis()
		dpclient.move(namespace,rset, from, ScadsDeploy.server_port,ScadsDeploy.server_sync, to, ScadsDeploy.server_port,ScadsDeploy.server_sync)
		(startms,System.currentTimeMillis())
	}

	def refreshHandle = {
		try {
			dpclient.lookup_namespace(namespace)
		} catch {
			case e: Exception => {
				println("dp client handle is shmetted, getting new one")
				dpclient = ScadsDeploy.getDataPlacementHandle(h,xtrace_on)
			}
		}
	}
}


