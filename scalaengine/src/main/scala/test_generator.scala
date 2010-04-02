/*
THIS FILE IS OLD!! don't use
*/

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.test._
import edu.berkeley.cs.scads.comm.Conversions._
import scala.actors._
import scala.actors.Actor._
import org.apache.avro.util.Utf8
import org.apache.zookeeper.CreateMode
import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator

case class PolicyRange(val minKey:Int, val maxKey:Int) {
	def contains(needle:Int) = (needle >= minKey && needle < maxKey)
}

object ZooKeeperServer extends optional.Application {
	def main(port: Int, data_path: String):Unit = {
		ZooKeep.start(data_path, port).root.getOrCreate("scads")
	}
}


object SetupNodes{
	val logger = Logger.getLogger("cluster.config")
	val commapattern = java.util.regex.Pattern.compile(",")
	
	def main(args: Array[String]): Unit = {
		val createNS = args(0).toBoolean
		val zoo_dns = args(1)
		val namespace = args(2)
		val cluster = new ScadsCluster(new ZooKeeperProxy(zoo_dns+":2181").root.get("scads"))
		try {
			if (createNS) createNamespace(cluster,namespace)
			else setPartitions(cluster, "/tmp/scads_config.dat",namespace)
			System.exit(0)
		} catch { case e => {logger.warn("Got exception "+ e.toString); System.exit(1)}}
	}
	
	def createNamespace(cluster:ScadsCluster, namespace:String) = {
		val k1 = new IntRec
		val v1 = new StringRec
		cluster.createNamespace(namespace, k1.getSchema(), v1.getSchema())
		
		// set up partitions for empty and fake clusters
		val minKey = new IntRec
		val maxKey = new IntRec
		val partition = new KeyPartition; partition.minKey = minKey.toBytes; partition.maxKey = maxKey.toBytes
		val policy = new PartitionedPolicy; policy.partitions = List(partition)
		cluster.addPartition(namespace,"empty",policy)
		
		minKey.f1 = -1
		maxKey.f1 = -1
		partition.minKey = minKey.toBytes; partition.maxKey = maxKey.toBytes
		policy.partitions = List(partition)
		cluster.addPartition(namespace,"fake",policy)
	}
	/**
	* file1: partition, min,max
	* file2: partition,server
	*/
	def setPartitions(cluster:ScadsCluster, filename:String,namespace:String) = {
		// tell each server which partition it has
		// and register each server with zookeeper
		val cr = new ConfigureRequest
		cr.namespace = namespace
		val f = scala.io.Source.fromFile(filename)
		val lines = f.getLines
		lines.foreach(line=>{
			val tokens = commapattern.split(line)
			if (tokens.size==2) {
				val part = tokens(0).trim
				logger.info("Setting "+tokens(1).trim+" with partition "+part)
				cr.partition = part
				Sync.makeRequest(RemoteNode(tokens(1).trim,9991), new Utf8("Storage"),cr)
				try {
					if (!part.equals("1")) cluster.addPartition(namespace,part)
					cluster.namespaces.get(namespace+"/partitions/"+part+"/servers").createChild(tokens(1).trim, "", CreateMode.PERSISTENT)
				} catch { case e => logger.warn("Got exception when adding server to placement "+ e.toString) }
			} 
		})
		
	}
}

@deprecated
object CreateDirectorData {
	val logger = Logger.getLogger("scads.datagen")
	BasicConfigurator.configure()
	val namespace = "perfTest256"
	val key = new IntRec
	var request_count = 0
	var exception_count = 0

	def main(args: Array[String]): Unit = {
		
		val host = args(0)
		val minKey = args(1).toInt
		val maxKey = args(2).toInt
		logger.info("Warming node "+host)
		(minKey until maxKey).foreach(currentKey=> {
			key.f1 = currentKey
			val pr = new PutRequest
			pr.namespace = namespace
			pr.key = key.toBytes
			pr.value = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".getBytes

			// create request actor
			val request = new ScadsWarmerActor(RemoteNode(host,9991),pr)
			request.start // actually send request
			Thread.sleep(1) // limit 1000 reqs per sec
		})
		logger.info("Done creating requests")
		while ((request_count+exception_count) < (maxKey-minKey)) { logger.info("Still waiting for responses"); Thread.sleep(5000) }
		if (exception_count == 0) { logger.info("Done warming with no exceptions"); System.exit(0)}
		else { logger.warn("Warming had "+exception_count+" exceptions and "+request_count+ " successful requests"); System.exit(1)}
	}
	class ScadsWarmerActor(dest:RemoteNode, scads_req:Object) extends Actor {
		var starttime:Long = -1L; var startNano:Long = -1L
		var endtime:Long = -1L; var latency:Double = -1.0
		def act = {
			starttime = System.currentTimeMillis
			startNano = System.nanoTime
			val req = new Message
			req.body = scads_req
			req.dest = new Utf8("Storage")
			val id = MessageHandler.registerActor(self)
			req.src = new java.lang.Long(id)
			//logger.info("Message sent as "+req.src)
			makeRequest(req,id)
		}
		def makeRequest(req:Message,id:Long):Object = {
			// send the request
			MessageHandler.sendMessage(dest, req)

			// wait for response
			reactWithin(10000) {
				case (RemoteNode(hostname, port), msg: Message) => msg.body match {
					case exp: ProcessingException => { logger.warn("Exception making request: "+exp); logException; MessageHandler.unregisterActor(id) }
					case obj => {
						endtime = System.currentTimeMillis
						latency = (System.nanoTime-startNano)/1000000.0
						logRequest
						MessageHandler.unregisterActor(id)
					}
				}
				case TIMEOUT => { logger.warn("Request timeout"); logException; MessageHandler.unregisterActor(id) }
				case msg => { logger.warn("Unexpected message: " + msg); MessageHandler.unregisterActor(id) }
			}
		}
	}
	
	def logException = {
		synchronized {
			exception_count+=1
			//logger.info("Exceptions: "+exception_count)
		}
	}
	def logRequest = {
		synchronized {
			request_count+=1
			//logger.info("Requests: "+request_count)
		}
	}
}
/*
object PolicyRange {
	def createPartitions(startKey: Int, endKey: Int, numPartitions: Int):List[PolicyRange] = {
		val partitions = new scala.collection.mutable.ArrayStack[PolicyRange]()
		val numKeys = endKey - startKey + 1

		val last = (startKey to endKey by (numKeys/numPartitions)).toList.reduceLeft((s,e) => {
			partitions.push(PolicyRange(s,e))
			e
		})
		partitions.push(PolicyRange(last,(numKeys + startKey)))

		return partitions.toList.reverse
	}
}
*/

class RequestLogger(val queue:java.util.concurrent.BlockingQueue[String]) extends Runnable {
	//import edu.berkeley.xtrace._
	val xtrace_on = System.getProperty("xtrace_stats","false").toBoolean	
	var running = true
	
	def run = {
		//logger.info("XTRACE SENDER: sending initial report")
		//XTraceContext.startTraceSeverity("xtracesender","Initiated",1)
		
		while (running) {
			send(queue.take) // send to xtrace
		}
	}
	def send(details:String) =  { if (xtrace_on) {}/*XTraceContext.logEvent("some_thread","ReadRandomPolicyXtrace","RequestDetails",details)*/ }
	def stop = {
		running = false
	}
}

@deprecated
object RequestRunner {
	val logger = Logger.getLogger("scads.requestgen")
	BasicConfigurator.configure()
	val commapattern = java.util.regex.Pattern.compile(",")
	var logfile:java.io.BufferedWriter = null
	
	// if doing any xtrace reporting, report locally using TCP
	System.setProperty("xtrace_stats","false")
	System.setProperty("xtrace.reporter","edu.berkeley.xtrace.reporting.TcpReporter")
	System.setProperty("xtrace.tcpdest","127.0.0.1:7831")
	
	val request_queue = new java.util.concurrent.ArrayBlockingQueue[Long](1000) // not used
	//var gens:RandomAccessSeq.Projection[RequestGeneratorTest] = null
	var num_requests:Long = 0L
	//var burst:Array[Int] = null
	//var wait_time = 1//1000000 // nanos before sending another request
	var wait_nanos:Long = 0L
	var lastRequestTime = System.nanoTime
	var startTime:Long = 0L
	
	class Runner(gen:RequestGenerator) extends Runnable {
		def run() = {
			//enqueueRequest
			num_requests+=1
			gen.doRequest(num_requests)
			lastRequestTime = System.nanoTime
			logfile.write(lastRequestTime+"\n")
		}
	}
	def getWorkload:Map[Int,(Int,Int)] = {
		Map[Int,(Int,Int)]((1 to 1).map(i=> (i-1,(i*120*1000,1000))):_*)
	}
	/*def enqueueRequest = {
		//logger.info("queueing request")
		burst.foreach(i => {request_queue.put(num_requests); num_requests+=1})
	}*/
	def main(args: Array[String]): Unit = {
		val numthreads = args(0).toInt
		val mapping = fileToMapping(args(1))
		
		logfile = new java.io.BufferedWriter(new java.io.FileWriter("/tmp/genreqs.csv"))
		val workload = getWorkload
		val queue = new java.util.concurrent.ArrayBlockingQueue[String](1000)
		val requestlogger = new RequestLogger(queue)

		val gen = new RequestGenerator(mapping,queue,request_queue)
		var running = true
		var currentInterval = 0; var currentDuration = workload(currentInterval)._1; wait_nanos = 1000000/(workload(currentInterval)._2)
		val stpe = new java.util.concurrent.ScheduledThreadPoolExecutor(1)

		// start!
		startTime = System.currentTimeMillis
		(new Thread(requestlogger)).start
		stpe.scheduleAtFixedRate(new Runner(gen), 0, 500000, java.util.concurrent.TimeUnit.NANOSECONDS)//MILLISECONDS)

		while (running && !mapping.isEmpty) {
			val currentTime = System.currentTimeMillis
			if (currentTime >= startTime+currentDuration) { // advance to next interval in workload, or end
				if (currentInterval < workload.size-1) {
					currentInterval +=1; currentDuration = workload(currentInterval)._1
					//burst_size = workload(currentInterval)._2; burst = (0 until burst_size).toArray
					wait_nanos = 1000000/(workload(currentInterval)._2)
					logfile.flush
					logger.info("Advanced interval. Wait time should be "+(wait_nanos/100000.0) +" ms")
				} else { // stop workload
					logger.info("Stopping generators")
					stpe.shutdown
					running = false
					Thread.sleep(2*1000)
					requestlogger.stop
					gen.stop//gens.foreach(g=> {g.stop;})
					logfile.flush; logfile.close
					logger.info(num_requests +" in "+ ((System.currentTimeMillis-startTime)/1000) +" seconds")
				}
			}
			/*else if (System.nanoTime >= (lastRequestTime+wait_nanos)) {
					gens(0).doRequest(num_requests);num_requests+=1//request_queue.put(num_requests); num_requests+=1
					lastRequestTime = System.nanoTime
					logfile.write(lastRequestTime+"\n")
			}*/
			//enqueueRequest
			//Thread.sleep(wait_time)
		}
	}
	def fileToMapping(filename:String):Map[PolicyRange,RemoteNode] = {
		
		val f = scala.io.Source.fromFile(filename)
		val lines = f.getLines
		Map[PolicyRange,RemoteNode]( lines.map(line=>{
			val tokens = commapattern.split(line)
			/*if (tokens.size == 3 )*/ (PolicyRange(tokens(1).toInt,tokens(2).toInt) -> RemoteNode(tokens(0),9991))
			//else println("Wrong number of tokens in file!")
		}).toList:_*)
	}
}

@deprecated
class RequestGenerator(mapping: Map[PolicyRange,RemoteNode], request_info:java.util.concurrent.BlockingQueue[String], request_queue:java.util.concurrent.BlockingQueue[Long]) {//extends Runnable {
	val logger = Logger.getLogger("scads.requestgen")
	val logfile = new java.io.BufferedWriter(new java.io.FileWriter("/tmp/sendreqs.csv"))
	logger.info("creating request class")
	logfile.write("created class")
	logfile.flush
	
	var running = true
	val rnd = new java.util.Random
	
	var startr = System.nanoTime
	var request_gen_time = System.nanoTime
	var before_send = System.nanoTime
	
	var request_count = 0
	var exception_count = 0
	var total_request_gen_time:Long = 0L // nanosec
	
	// this stuff should be replaced with real request generator
	val key = new IntRec
	val namespace = "perfTest256"
	val ranges = Array[PolicyRange](mapping.toList.map(e=>e._1):_*)
	var minKey = ranges.foldLeft(0)((out,entry)=>{if(entry.minKey<out) entry.minKey else out})
	var maxKey = ranges.foldLeft(0)((out,entry)=>{if(entry.maxKey>out) entry.maxKey else out})
	
	class ScadsRequestActor(dest:List[RemoteNode], key:Int,scads_req:Object,log:Boolean) extends Actor {
		val createNano:Long = System.nanoTime
		var starttime:Long = -1L; var startNano:Long = -1L
		var endtime:Long = -1L; var latency:Double = -1.0; var request_gen = -1L; var send_gen = -1L
		def act = {
			startNano = System.nanoTime
			val req = new Message
			req.body = scads_req // get or put request
			req.dest = new Utf8("Storage")
			val id = MessageHandler.registerActor(self)
			req.src = new java.lang.Long(id)
			makeRequest(req,id)
		}
		def makeRequest(req:Message,id:Long):Object = {
			// send the request
			starttime = System.currentTimeMillis
			val start_send = System.nanoTime
			logTime(start_send)
			MessageHandler.sendMessage(dest(0), req) // go to only first node
			send_gen = System.nanoTime-start_send
			request_gen = System.nanoTime-startNano
			// wait for response
			reactWithin(1000) {
				case (RemoteNode(hostname, port), msg: Message) => msg.body match {
					case exp: ProcessingException => { logger.warn("Exception making request: "+exp); logException; MessageHandler.unregisterActor(id)}
					case obj => {
						endtime = System.currentTimeMillis
						latency = (System.nanoTime-start_send)/1000000.0
						logRequest
						val total_time = System.nanoTime - createNano
						// log start_time, end_time, latency, hostname
						val request_type = scads_req match { // parse key and get type
							case r:PutRequest => "put"
							case r:GetRequest => "get"
							case _ => "unknown"
						}
						//if (log) request_info.put(request_type+","+key+","+hostname+","+latency+",0")
						if (log) request_info.put(starttime+","+endtime+","+request_gen/1000000.0+","+send_gen/1000000.0+","+latency+","+hostname+"\n")
						MessageHandler.unregisterActor(id)
					}
				}
				case TIMEOUT => { logException; MessageHandler.unregisterActor(id) }
				case msg => { logger.warn("Unexpected message: " + msg); MessageHandler.unregisterActor(id) }
			}
		}
	}
	
	def run = {
		while (running) {
			doRequest(request_queue.take)		
		}
	}
	def flushlog = logfile.flush
	def doRequest(id:Long) = {
		startr = System.nanoTime
		val request = generateRequest
		request_gen_time = System.nanoTime
		total_request_gen_time += (request_gen_time-startr)
		request.start // actually send request
	}
	def stop = { logger.info("Ending generation"); running = false; logfile.flush; logfile.close; logger.info("exceptions: "+exception_count+", requests: "+request_count) }

	def logException = {
		synchronized {
			exception_count+=1
		}
	}
	def logRequest = {
		synchronized {
			request_count+=1
		}
	}
	def logTime(time:Long) = {
		synchronized {
			logfile.write(time+"\n")
		}
	}
	private def generateRequest():ScadsRequestActor = {
		// pick a key
		key.f1 = rnd.nextInt(maxKey-minKey) + minKey
		// locate correct node for key
		val nodes = locate(key.f1)
		// pick and create type of request
		val gr = new GetRequest
		gr.namespace = namespace
		gr.key = key.toBytes
		// create request actor
		new ScadsRequestActor(nodes,key.f1,gr,(rnd.nextDouble<0.02))
		
	}
	private def locate(needle:Int):List[RemoteNode] = {
		mapping.filter(e=>e._1.contains(needle)).toList.map(n=>n._2)//(0)._2 // pick first one, and get node
	}
}

object RequesterServer {
	//BasicConfigurator.configure()
	val commapattern = java.util.regex.Pattern.compile(",")
	System.setProperty("xtrace_stats","true")
	val xtrace_on = System.getProperty("xtrace_stats","false").toBoolean
	
	def main(args: Array[String]) {
		// args: do warming, path to mapping file
		val doWarming = args(0).toBoolean
		val mapping = fileToMapping(args(1))
		
		val requesthandler = if (doWarming) new Warmer( args(2).toInt, args(3).toInt, args(4).toInt,1000000,mapping) else new SimpleRequestHandler(0, mapping)
		if (xtrace_on) {
			val xtrace_logger = new RequestLogger(new java.util.concurrent.ArrayBlockingQueue[String](1000))
			requesthandler.xtrace_logger_queue = xtrace_logger.queue
			(new Thread(xtrace_logger)).start
		}
		requesthandler.run

		//val intervals = List(5000000,3000000,2500000,2000000,1000000,500000,250000,125000,100000,62500/*,50000,40000,30000*/)
		//val intervals = List(200000,100000,80000,75000,65000,58000,55000,50000,45000,40000).map(i =>i*10)
		/*val rates = List(13,12,11,10,2,1).map(i=>i*1000)
		val intervals = rates.map(r=>(java.lang.Math.pow(10,9)/r).toInt)
		val handlers = intervals.map(i=> new SimpleRequestHandler(i,mapping))
		handlers.foreach(h => h.run)*/
	}
	/**
	* Convert a csv file representing node to responsilibty mapping into a Map
	* This should be replaced when fix data placement
	*/
	private def fileToMapping(filename:String):Map[PolicyRange,List[RemoteNode]] = {
		val f = scala.io.Source.fromFile(filename)
		val lines = f.getLines
		val mapping = new scala.collection.mutable.HashMap[PolicyRange,scala.collection.mutable.ListBuffer[RemoteNode]]()
		lines.foreach(line=>{
			val tokens = commapattern.split(line)
			val entry = mapping.getOrElse(PolicyRange(tokens(1).trim.toInt,tokens(2).trim.toInt),new scala.collection.mutable.ListBuffer[RemoteNode]())
			entry += RemoteNode(tokens(0).trim,9991)
			mapping(PolicyRange(tokens(1).trim.toInt,tokens(2).trim.toInt)) = entry
		})
		Map[PolicyRange,List[RemoteNode]]( mapping.toList.map(entry=>(entry._1,entry._2.toList)):_* )
	}
}

class Warmer(minKey:Int, maxKey:Int, replicas:Int,period_nanos:Long,mapping: Map[PolicyRange,List[RemoteNode]]) extends SimpleRequestHandler(period_nanos,mapping) {
	var success_count = new java.util.concurrent.atomic.AtomicInteger
	var currentKey = new java.util.concurrent.atomic.AtomicInteger(minKey)
	var currentRunner = stpe.scheduleAtFixedRate(new Requestor, 0, period_nanos, java.util.concurrent.TimeUnit.NANOSECONDS)
	
	override def run():Unit = {
		logger.info("About to warm "+minKey+ "-"+maxKey+" with replicas: "+replicas)
		while (!currentRunner.isDone) { logRequest(requests.poll(1000, java.util.concurrent.TimeUnit.MILLISECONDS)) }
		val stopTime = System.currentTimeMillis
		if (request_map.size > 0) logger.info("Waiting for "+request_map.size+" requests to return")
		while (request_map.size >0 && System.currentTimeMillis < stopTime+5000 ) { logRequest(requests.poll(1000, java.util.concurrent.TimeUnit.MILLISECONDS)); logger.info("Waiting for "+request_map.size+" requests to return") }
		logger.info("Done experiment. Requests sent: "+request_id.get+". Messages sent: "+request_count.get+". " +(new java.util.Date).toString)
		if (success_count.get >= (maxKey-minKey)*replicas) { logger.info("Warming successful"); System.exit(0) }
		else { logger.warn("Warming unsuccessful, missing "+((maxKey-minKey)*replicas-success_count.get)); System.exit(1)}
	}
	override protected def logRequest(req:RequestResponse):Unit = {
		if (req==null) return
		val metadata = request_map.remove(req.id)
		if (req.status != FAILED) success_count.getAndIncrement
		else logger.warn("Got request with non-success")
		if (currentKey.get >= maxKey && !currentRunner.isDone) { currentRunner.cancel(true); logger.info("Done sending requests, cancelled scheduler") }
	}
	override protected def generateRequest:(Long,List[RemoteNode], Object) = {
		// pick and increment
		key.f1 = currentKey.getAndIncrement
		val nodes = locate(key.f1) // locate correct node for key
		// pick and create type of request
		val gr = new PutRequest
		gr.namespace = namespace
		gr.value = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".getBytes
		gr.key = key.toBytes
		val id = request_id.getAndIncrement
		val request_type = "put"
		request_map.put(id, RequestSent(request_type, key.f1, System.nanoTime, System.currentTimeMillis))// update global map of requests
		(id, nodes, gr)
	}
}

/**
* Handler service for receiving request responses, logging request latencies
* Also sends requests periodically
* new Utf8("SimpleRequest")
*/
class SimpleRequestHandler(var period_nanos:Long, mapping: Map[PolicyRange,List[RemoteNode]]) extends ServiceHandler with Runnable {
	val serviceName = "SimpleRequest_"+period_nanos
	protected val logger = Logger.getLogger("SimpleRequestHandler")
	MessageHandler.registerService(serviceName,this)

	// this stuff should be replaced with real request generator
	val key = new IntRec
	val namespace = "perfTest256"
	val ranges = Array[PolicyRange](mapping.toList.map(e=>e._1):_*)
	var minKey = ranges.foldLeft(ranges(0).minKey)((out,entry)=>{if(entry.minKey<out) entry.minKey else out})
	var maxKey = ranges.foldLeft(ranges(0).maxKey)((out,entry)=>{if(entry.maxKey>out) entry.maxKey else out})
	// end replacement stuff

	val requests = new java.util.concurrent.ArrayBlockingQueue[RequestResponse](5000)
	val stpe = new java.util.concurrent.ScheduledThreadPoolExecutor(2) // two threads in pool for generating requests periodically
	var duration:Int = 0
	var previousIntervalsCount = 0L
	var running = true
	val request_id = new java.util.concurrent.atomic.AtomicLong
	val request_count = new java.util.concurrent.atomic.AtomicLong
	val request_map = new java.util.concurrent.ConcurrentHashMap[Long,RequestSent] // id -> RequestSent(request type, key, starttime in nanos, start timestamp)
	val replica_map = new java.util.concurrent.ConcurrentHashMap[Long,RequestSent] // id -> RequestSent(request type, key, starttime in nanos, start timestamp)
	val SUCCESS:Int = 0;	val FAILED:Int = 1;		val EXCEPT:Int = 2

	// logging stuff
	val requestLog = new java.io.BufferedWriter(new java.io.FileWriter("/tmp/requestlogs.csv"))
	val sendLog = new java.io.BufferedWriter(new java.io.FileWriter("/tmp/sendreqs.csv"))
	val doSendLog = false
	var lastFlush:Long = 0L
	val flushInterval = 30*1000
	val rnd = new java.util.Random
	val logrnd = new java.util.Random // since another thread uses other one
	var xtrace_logger_queue:java.util.concurrent.BlockingQueue[String] = null

	// helper classes
	case class RequestSent(request_type:String, key:Int, startNano:Long, startTimeStamp:Long)
	case class RequestResponse(host:String, id:java.lang.Long, receiveNano:Long,receiveTimeStamp:Long, status:Int)
	class Requestor() extends Runnable {
		def run():Unit = {
			val req_info = generateRequest
			val req = new Message
			req.body = req_info._3 // e.g. get() or put() request
			req.dest = new Utf8("Storage") // send to storage engine(s)
			req.src = new Utf8(serviceName) // have response go to service that logs latencies
			req.id = req_info._1
			if (doSendLog) sendLog.write(System.nanoTime+"\n")
			req_info._2.foreach(host=> {MessageHandler.sendMessage(host, req); request_count.getAndIncrement} ) // send to all replicas
		}
	}
	private def waitAndClearDelayedRequests:Long = {
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
	def run():Unit = {
		// map of duration -> interval time
		val basetime = (960*(java.lang.Math.pow(10,6))).toInt
		val rates = List(2,4,6,8,10,12,14).map(i=>i*1000)//.map(i=>i/2)
		val workload = Map[Int,(Int,Int)]( (0 until rates.size).map(i=> (i,(300*1000/*basetime/rates(i).toInt*/,(java.lang.Math.pow(10,9)/rates(i)).toInt))):_* )//Map[Int,(Int,Int)]( (0->(120000,2000000)),(1->(120000,1000000)) )
		//val rates = List(16).map(i=>i*1000)
		//val workload = Map[Int,(Int,Int)]( (0 until rates.size).map(i=> (i,(60*1000,(java.lang.Math.pow(10,9)/rates(i)).toInt))):_* )
		
		val total_intervals = workload.size
		var startTime = System.currentTimeMillis
		var stopTime:Long = 0
		var currentInterval = 0
		duration = workload(currentInterval)._1
		period_nanos = workload(currentInterval)._2
		lastFlush = System.currentTimeMillis
		var currentRunner = stpe.scheduleAtFixedRate(new Requestor, 0, period_nanos, java.util.concurrent.TimeUnit.NANOSECONDS)
		logger.info("Running at "+(java.lang.Math.pow(10,9)/period_nanos).toInt+ " requests/sec "+(new java.util.Date).toString)
		
		while (running) {
			if (System.currentTimeMillis >= startTime+duration) { // done with this interval
				currentRunner.cancel(true)
				if (currentInterval < total_intervals-1) { // have more intervals to run
					// finish up last interval
					stopTime = System.currentTimeMillis
					val waitTime = waitAndClearDelayedRequests
					if (waitTime > 0L) logger.info("Had to wait additional "+(waitTime-stopTime)/1000.0 + " seconds")
					// start new interval
					previousIntervalsCount = request_id.get
					currentInterval += 1
					duration = workload(currentInterval)._1
					period_nanos = workload(currentInterval)._2
					startTime = System.currentTimeMillis
					logger.info("Running at "+(java.lang.Math.pow(10,9)/period_nanos).toInt+ " requests/sec "+(new java.util.Date).toString)
					currentRunner = stpe.scheduleAtFixedRate(new Requestor, 0, period_nanos, java.util.concurrent.TimeUnit.NANOSECONDS)
				}
				else { // done all intervals
					stpe.shutdown
					running = false
				}
			}
			else logRequest(requests.poll(1000, java.util.concurrent.TimeUnit.MILLISECONDS))
		}
		logger.info("Done experiment. Requests sent: "+(request_id.get-1)+". Messages sent: "+request_count.get+". " +(new java.util.Date).toString)
		requestLog.flush; requestLog.close; sendLog.flush; sendLog.close // flush and close all logs
		System.exit(0)
	}

	/**
	* Get the response message, log the endtime in nanos, and
	* put on queue to be logged somewhere
	*/
	def receiveMessage(src: RemoteNode, msg:Message): Unit = {
		//val status = SUCCESS // TODO: figure out request exit status
		val status:Int = msg.body match {
			case exp: ProcessingException => { /*logger.warn("Exception making request: "+exp);*/ FAILED }
			//case good:Boolean => {if (good) SUCCESS else FAILED} // fix this when PutRequest returns something other than null
			case rec:Record => SUCCESS
			case msg => {/*logger.warn("Some weird state"); */EXCEPT}
		}
		if (requests.remainingCapacity < 1) logger.warn("Received requests queue full")
		else if (requests.remainingCapacity < 5) logger.warn("Received requests queue nearly full")
		requests.put(RequestResponse(src.hostname, msg.id, System.nanoTime, System.currentTimeMillis, status))
	}

	def stop = { running = false }

	/**
	* look up start time of this request id and log the latency
	* and the hostname of where the request was sent
	*/
	protected def logRequest(req:RequestResponse):Unit = {
		if (req==null) return
		var metadata = request_map.remove(req.id)
		var primary = 0 // was first returned request?
		if (metadata == null) { primary = 1; metadata = replica_map.remove(req.id) }// check for replica's response
		if (metadata != null && logrnd.nextDouble<0.02) { // log some requests
			val latency = if (req.status == EXCEPT) 1000 else (req.receiveNano-metadata.startNano)/1000000.0 // if failed request, latency=1000ms
			
			// log request type, key, hostname, latency, sucess status, request rate, start, end, was primary request
			val details = metadata.request_type+","+metadata.key+","+req.host+","+latency+","+req.status+","+(java.lang.Math.pow(10,9)/period_nanos).toInt+","+metadata.startTimeStamp+","+req.receiveTimeStamp+","+primary
			if (xtrace_logger_queue != null) xtrace_logger_queue.put(details)
			if (req.id.longValue > duration*(java.lang.Math.pow(10,6)/period_nanos.toInt)-500000+previousIntervalsCount) { requestLog.write(details); requestLog.newLine }
		}
		if (System.currentTimeMillis >= (lastFlush+flushInterval)) requestLog.flush
	}
	protected def generateRequest:(Long,List[RemoteNode], Object) = {
		// pick a key
		key.f1 = rnd.nextInt(maxKey-minKey) + minKey
		val nodes = locate(key.f1) // locate correct node for key
		// pick and create type of request
		val gr = new GetRequest
		gr.namespace = namespace
		gr.key = key.toBytes
		val id = request_id.getAndIncrement
		//logger.info("Generating request "+id)
		val request_type = "get"
		request_map.put(id, RequestSent(request_type, key.f1, System.nanoTime, System.currentTimeMillis))// update global map of requests
		if (nodes.size > 1) replica_map.put(id, RequestSent(request_type, key.f1, System.nanoTime, System.currentTimeMillis))
		(id, nodes, gr)
	}
	/**
	* Find storage nodes responsible for key.
	* This should be replaced/augmented when data placement provides mapping
	*/
	protected def locate(needle:Int):List[RemoteNode] = {
		val ret = mapping.filter(e=>e._1.contains(needle)).toList
		if (ret.size == 1) ret(0)._2
		else List[RemoteNode]()
	}
}
