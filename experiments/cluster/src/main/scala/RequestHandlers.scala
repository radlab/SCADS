package edu.berkeley.cs.scads.scale.cluster

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.test._
import edu.berkeley.cs.scads.comm.Conversions._
import edu.berkeley.cs.scads.workload._
import org.apache.avro.util.Utf8
import org.apache.log4j.Logger

/**
* Request handler with static node responsibilty mapping passed in
*
*/

class StaticRequestHandler(val namespace:String, mapping: Map[PolicyRange,RemoteNode], val numClients:Int) extends RequestHandler with LogToFile {
	val logger = Logger.getLogger("statichandler")
	val quorum = 1
	val failedRequestLatency = 1000L
	
	val ranges = Array[PolicyRange](mapping.toList.map(e=>e._1):_*)
	val minKey = ranges.foldLeft(ranges(0).minKey)((out,entry)=>{if(entry.minKey<out) entry.minKey else out})
	val maxKey = ranges.foldLeft(ranges(0).maxKey)((out,entry)=>{if(entry.maxKey>out) entry.maxKey else out})
	val workload = WorkloadGenerators.linearWorkloadRates(1.0, 0.0, 0, maxKey.toString, namespace, 2000, 12000, 6, 60*1000)


	protected def locate(ns:String,needle:Int):List[RemoteNode] = {
		mapping.filter(e=>e._1.contains(needle)).toList.map(n=>n._2)
	}
	override def run:Unit = {
		super.run
	}
	override protected def endExperiment = {
		requestLog.flush; requestLog.close
		super.endExperiment
	}
}

class DynamicRequestHandler(val namespace:String, zoo_server:String, val workload:WorkloadDescription, val numClients:Int) extends RequestHandler with LogToFile {
	val logger = Logger.getLogger("dynamichandler")
	val quorum = 1
	val failedRequestLatency = 1000L
	
	val server_port = 9991
	val mapping = new ClientMapping(zoo_server)
	
	protected def locate(ns:String,needle:Int):List[RemoteNode] = {
		mapping.locate(ns,needle).map(entry => RemoteNode(entry,server_port))
	}
	protected def waitAndClearDelayedRequests:Long = {
		var waitTime:Long = 0
		if (request_map.size > 0) { logger.info("Still waiting for messages "+request_map.size); waitTime = System.currentTimeMillis+1000 }
		while (request_map.size > 0) {
			if (System.currentTimeMillis >= waitTime) { // try to grab a late request, and extend wait time
				var late_req = requests.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS)
				if (late_req != null) {
					waitTime = System.currentTimeMillis+1000
					while (late_req != null) { // keep pulling while still can
						logRequest(late_req)
						late_req = requests.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS)
					}
				}
				else { // give up on requests and deem all outstanding requests as failed
					logger.info("Had to give up on "+request_map.size+" requests")
					val iter = request_map.keySet.iterator
					while (iter.hasNext) logRequest(RequestResponse("unknown", iter.next, System.nanoTime, System.currentTimeMillis, FAILED))
				}
			}
		}
		waitTime
	}
	
	override protected def endInterval(stopTime:Long) = {
		val waitTime = waitAndClearDelayedRequests
		if (waitTime > 0L) logger.info("Had to wait additional "+(waitTime-stopTime)/1000.0 + " seconds")
	}
}

class DynamicWarmer(namespace:String, zoo_server:String, minKey:Int, maxKey:Int, workload:WorkloadDescription, numClients:Int, override val quorum:Int) extends DynamicRequestHandler(namespace, zoo_server, workload, numClients) {
	var success_count = new java.util.concurrent.atomic.AtomicInteger
	var currentKey = new java.util.concurrent.atomic.AtomicInteger(minKey)
	
	override def logRequest(req:RequestResponse):Unit = {
		if (req==null) return
		val metadata = request_map.remove(req.id)
		if (req.status != FAILED) success_count.getAndIncrement
		else logger.warn("Got request with non-success")
	}
	override protected def endInterval(stopTime:Long) = {
		if (request_map.size > 0) logger.info("Waiting for "+request_map.size+" requests to return")
		while (request_map.size >0 && System.currentTimeMillis < stopTime+5000 ) { logRequest(requests.poll(1000, java.util.concurrent.TimeUnit.MILLISECONDS)); logger.info("Waiting for "+request_map.size+" requests to return") }
		logger.info("Requests sent: "+request_id.get+". Messages sent: "+request_count.get+". " +(new java.util.Date).toString)
	}
	override protected def endExperiment = {
		if (success_count.get >= (maxKey-minKey)*quorum) { logger.info("Warming successful"); System.exit(0)}
		else { logger.warn("Warming unsuccessful, missing "+((maxKey-minKey)*quorum-success_count.get)); System.exit(1) }
	}
}

trait LogToFile {
	def request_map:java.util.concurrent.ConcurrentHashMap[Long,RequestSent]
	def EXCEPT:Int
	def period_nanos:Long
	def logger:Logger
	def failedRequestLatency:Long
	def thread_name:String
	def server:String
	def samplingFraction:Double
	
	val requestLog = new java.io.BufferedWriter(new java.io.FileWriter("/tmp/requestlogs.csv"))
	var lastFlush:Long = 0L
	val flushInterval = 30*1000
	val logrnd = new java.util.Random( (thread_name).hashCode.toLong+System.currentTimeMillis )
	
	protected def logRequest(req:RequestResponse):Unit = {
		if (req==null) return
		val metadata = request_map.remove(req.id)
		if (logrnd.nextDouble<samplingFraction && metadata != null) { // log some requests
			val latency = if (req.status == EXCEPT) failedRequestLatency else (req.receiveNano-metadata.startNano)/1000000.0 // if failed request, latency=1000ms
			// request type, key, hostname, latency, sucess status, request rate
			val details = metadata.request_type+","+metadata.key+","+req.host+","+latency+","+req.status+","+(java.lang.Math.pow(10,9)/period_nanos).toInt+","+metadata.startTimeStamp+","+req.receiveTimeStamp
			requestLog.write(details)
			requestLog.newLine
		}
		if (System.currentTimeMillis >= (lastFlush+flushInterval)) requestLog.flush
	}
}

trait LogInXtrace {
	def request_map:java.util.concurrent.ConcurrentHashMap[Long,RequestSent]
	def EXCEPT:Int
	def period_nanos:Long
	def logger:Logger
	def failedRequestLatency:Long
	def thread_name:String
	def server:String
	def samplingFraction:Double
	
	var xtrace_logger_queue:java.util.concurrent.BlockingQueue[String] = null
	val logrnd = new java.util.Random( (server+thread_name).hashCode.toLong+System.currentTimeMillis )
	
	protected def logRequest(req:RequestResponse):Unit = {
		if (req==null) return
		var metadata = request_map.remove(req.id)
		if (metadata != null && logrnd.nextDouble<samplingFraction) { // log some requests
			val latency = if (req.status == EXCEPT) failedRequestLatency else (req.receiveNano-metadata.startNano)/1000000.0 // if failed request, latency=1000ms
			
			// log request type, key, hostname, latency, sucess status//, request rate, start, end
			val details = metadata.request_type+","+"%010d".format(metadata.key)+","+req.host+","+latency+","+req.status//+","+(java.lang.Math.pow(10,9)/period_nanos).toInt+","+metadata.startTimeStamp+","+req.receiveTimeStamp
			if (xtrace_logger_queue != null) xtrace_logger_queue.put(details)
		}
	}
}

class RequestLogger(val queue:java.util.concurrent.BlockingQueue[String]) extends Runnable {
	import edu.berkeley.xtrace._
	val xtrace_on = System.getProperty("xtrace_stats","false").toBoolean	
	var running = true
	
	def run = {
		//logger.info("XTRACE SENDER: sending initial report")
		XTraceContext.startTraceSeverity("xtracesender","Initiated",1)
		
		while (running) {
			send(queue.take) // send to xtrace
		}
	}
	def send(details:String) =  { if (xtrace_on) XTraceContext.logEvent("some_thread","ReadRandomPolicyXtrace","RequestDetails",details) }
	def stop = {
		running = false
	}
}
case class RequestSent(request_type:String, key:Int, startNano:Long, startTimeStamp:Long)
case class RequestResponse(host:String, id:java.lang.Long, receiveNano:Long,receiveTimeStamp:Long, status:Int)

abstract class RequestHandler extends ServiceHandler with Runnable {
	/* abstract stuff */
	def logger:Logger
	def namespace:String
	def workload:WorkloadDescription
	protected def locate(ns:String,needle:Int):List[RemoteNode]
	protected def logRequest(req:RequestResponse):Unit
	def quorum:Int
	def numClients:Int
	/* end abstract */
	
	val serviceName = "RequestHandler"
	MessageHandler.registerService(serviceName,this)
	val thread_name = Thread.currentThread.getName
	val server = java.net.InetAddress.getLocalHost().getHostName
	val requests = new java.util.concurrent.ArrayBlockingQueue[RequestResponse](5000)
	val stpe = new java.util.concurrent.ScheduledThreadPoolExecutor(2) // threads in pool for generating requests periodically
	val request_id = new java.util.concurrent.atomic.AtomicLong
	val request_count = new java.util.concurrent.atomic.AtomicLong	
	val request_map = new java.util.concurrent.ConcurrentHashMap[Long,RequestSent] // id -> RequestSent(request type, key, starttime in nanos, start timestamp)
	val SUCCESS:Int = 0;	val FAILED:Int = 1;		val EXCEPT:Int = 2
	val nodernd = new java.util.Random( (thread_name).hashCode.toLong+System.currentTimeMillis )
	
	var requestGenerator:SCADSRequestGenerator = null
	var period_nanos:Long = 0L
	var samplingFraction = 0.02

	class Requestor() extends Runnable {
		def run():Unit = {
			val req_info = generateRequest(requestGenerator,System.currentTimeMillis)
			if (req_info._3 != null) {
				val req = new Message
				req.body = req_info._3 // e.g. get() or put() request
				req.dest = new Utf8("Storage") // send to storage engine(s)
				req.src = new Utf8(serviceName) // have response go to service that logs latencies
				req.id = req_info._1
				req_info._2.foreach(host=> {MessageHandler.sendMessage(host, req); request_count.getAndIncrement} ) // send to quorum replicas
			}
		}
	}

	def run:Unit = {
		val total_intervals = workload.workload.size
		var currentIntervalI = 0
		var currentInterval = workload.workload(currentIntervalI)
		var duration = currentInterval.duration
		requestGenerator = currentInterval.requestGenerator
		val divisor = requestGenerator match { case g:WarmingSCADSRequestGenerator => 1; case _ => 2*quorum }
		period_nanos = ( java.lang.Math.pow(10,9) / (currentInterval.numberOfActiveUsers/divisor) ).toLong
		
		
		var startTime = System.currentTimeMillis
		var stopTime:Long = 0
		var currentRunner = stpe.scheduleAtFixedRate(new Requestor, 0, period_nanos, java.util.concurrent.TimeUnit.NANOSECONDS)
		logger.info("Running at "+(java.lang.Math.pow(10,9)/period_nanos).toInt+ " requests/sec "+(new java.util.Date).toString)
		
		var running = true
		while (running) {
			if (System.currentTimeMillis >= startTime+duration) { // done with this interval
				currentRunner.cancel(true)
				stopTime = System.currentTimeMillis
				endInterval(stopTime)
				if (currentIntervalI < total_intervals-1) { // have more intervals to run
					
					// start new interval
					currentIntervalI += 1
					currentInterval = workload.workload(currentIntervalI)
					duration = currentInterval.duration
					period_nanos = (java.lang.Math.pow(10,9)/(currentInterval.numberOfActiveUsers/divisor)).toLong
					//samplingFraction = 10000.0 / ((java.lang.Math.pow(10,9)/period_nanos)*10.0*numClients ) // TODO: remove
					
					startTime = System.currentTimeMillis
					logger.info("Running at "+(java.lang.Math.pow(10,9)/period_nanos).toInt+ " requests/sec with sampling "+samplingFraction+" "+(new java.util.Date).toString)
					currentRunner = stpe.scheduleAtFixedRate(new Requestor, 0, period_nanos, java.util.concurrent.TimeUnit.NANOSECONDS)
				}
				else { // done all intervals
					stpe.shutdown
					running = false
				}
			}
			else logRequest(requests.poll(1000, java.util.concurrent.TimeUnit.MILLISECONDS))
		}
		logger.info("Ending experiment.")
		endExperiment
	}
	protected def generateRequest(requestGenerator:SCADSRequestGenerator,time:Long):(Long,List[RemoteNode], Object) = {
		val request = requestGenerator.generateRequest(time)
		if (request != null) {
			val key = request.key.toInt
			val nodes = locate(request.namespace,key)
			val nodes_quorum = shuffle(nodes).slice(0,quorum)
			if (nodes_quorum.size < quorum) logger.warn("Have less than quorum nodes to send to")
			val id = request_id.getAndIncrement
			request_map.put(id, RequestSent(request.reqType, key, System.nanoTime, System.currentTimeMillis))// update global map of requests
			(id, nodes_quorum, request.getRequest)
		}
		else {/*logger.warn("Null request generated");*/(-1,List[RemoteNode](),null)}
	}
	/**
	* Get the response message, log the endtime in nanos, and
	* put on queue to be logged somewhere
	*/
	def receiveMessage(src: RemoteNode, msg:Message): Unit = {
		val status:Int = msg.body match {
			case exp: ProcessingException => { /*logger.warn("Exception making request: "+exp);*/ FAILED }
			case rec:Record => SUCCESS
			case null => SUCCESS // since put() returns null
			case msg => { logger.warn("Some weird state"); EXCEPT}
		}
		if (requests.remainingCapacity < 1) logger.warn("Received requests queue full")
		else if (requests.remainingCapacity < 5) logger.warn("Received requests queue nearly full")
		requests.put(RequestResponse(src.hostname, msg.id, System.nanoTime, System.currentTimeMillis, status))
	}
	protected def endInterval(stopTime:Long) = {}
	protected def endExperiment = {
		logger.info("Done experiment. Requests sent: "+(request_id.get-1)+". Messages sent: "+request_count.get+". " +(new java.util.Date).toString)
		System.exit(0)
	}
	protected def shuffle[T](seq: List[T]): List[T] = {
    val buf: Array[T] = seq.toArray

    def swap(i1: Int, i2: Int) {
      val tmp = buf(i1)
      buf(i1) = buf(i2)
      buf(i2) = tmp
    }

    for (n <- buf.length to 2 by -1) {
      val k = nodernd.nextInt(n)
      swap(n - 1, k)
    }

    buf.toList
  }
}