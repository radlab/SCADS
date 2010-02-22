import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.test._
import edu.berkeley.cs.scads.comm.Conversions._
import scala.actors._
import scala.actors.Actor._
import org.apache.avro.util.Utf8
import org.apache.log4j.Logger

object ZooKeeperServer extends optional.Application {
	def main(port: Int, data_path: String):Unit = {
		val zookeeper = ZooKeep.start(data_path, port).root.getOrCreate("scads")
		zookeeper.updateData(true)
		zookeeper.updateChildren(true)
	}
}

//ScadsDeployment.createJavaService(machine, new java.io.File(jarpath), "SetupNodes", 1024, host + " perfTest256")
object SetupNodes{
	def main(args: Array[String]): Unit = {
		val host = args(0)

		val cr = new ConfigureRequest
		cr.namespace = args(1)
		cr.partition = "1"
		Sync.makeRequest(RemoteNode(host,9991), new Utf8("Storage"),cr)
	}
}

object CreateDirectorData {
	val logger = Logger.getLogger("scads.datagen")
	val namespace = "perfTest256"
	val key = new IntRec
	var request_count = 0
	var exception_count = 0
	
	def main(args: Array[String]): Unit = {
		
		val host = args(0)
		val minKey = args(1).toInt
		val maxKey = args(2).toInt
		(minKey until maxKey).foreach(currentKey=> {
			key.f1 = currentKey
			val pr = new PutRequest
			pr.namespace = namespace
			pr.key = key.toBytes
			pr.value = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".getBytes

			// create request actor
			val request = new ScadsWarmerActor(RemoteNode(host,9991),pr)
			request.start // actually send request
		})
		logger.info("Done creating requests")
		while ((request_count+exception_count) < (maxKey-minKey)) { logger.info("Still waiting for responses"); Thread.sleep(500) }
		if (exception_count == 0) logger.info("Done warming with no exceptions")
		else logger.warn("Warming had "+exception_count+" exceptions and "+request_count+ " successful requests")
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
			req.src = new java.lang.Long(MessageHandler.registerActor(self))
			//logger.info("Message sent as "+req.src)
			makeRequest(req)
		}
		def makeRequest(req:Message):Object = {
			// send the request
			MessageHandler.sendMessage(dest, req)

			// wait for response
			reactWithin(10000) {
				case (RemoteNode(hostname, port), msg: Message) => msg.body match {
					case exp: ProcessingException => { logger.warn("Exception making request: "+exp); exception_count +=1 }
					case obj => {
						endtime = System.currentTimeMillis
						latency = (System.nanoTime-startNano)/1000000.0
						request_count += 1
						// log start_time, end_time, latency, hostname
						//request_info.put(starttime+","+endtime+","+latency+","+hostname+"\n")
					}
				}
				case TIMEOUT => { logger.warn("Request timeout"); exception_count +=1 }
				case msg => logger.warn("Unexpected message: " + msg)
			}
		}
	}
	
}

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

case class PolicyRange(val minKey:Int, val maxKey:Int) {
	def contains(needle:Int) = (needle >= minKey && needle < maxKey)
}

class RequestLogger(queue:java.util.concurrent.BlockingQueue[String]) extends Runnable {
	var running = true
	val sleep_time = 30*1000
	var lastFlush = System.currentTimeMillis
	val logfile = new java.io.BufferedWriter(new java.io.FileWriter("/tmp/requestlogs.csv"))
	
	def run = {
		while (running) {
			if (System.currentTimeMillis > lastFlush+sleep_time) {
				// flush stats so far to file
				logfile.flush
				lastFlush = System.currentTimeMillis
			}
			logfile.write(queue.take)
		}
	}
	def stop = {
		running = false
		logfile.flush
		logfile.close
	}
}

object RequestRunner {
	val logger = Logger.getLogger("scads.requestgen")
	val commapattern = java.util.regex.Pattern.compile(",")
	var wait_time = 5 // ms before sending another request
	
	def main(args: Array[String]): Unit = {
		var num_requests:Long = 0L
		val numthreads = args(0).toInt
		val numMillis = args(1).toInt

		val mapping = fileToMapping(args(2))
		val queue = new java.util.concurrent.ArrayBlockingQueue[String](100)
		val request_queue = new java.util.concurrent.ArrayBlockingQueue[Long](100)
		val requestlogger = new RequestLogger(queue)

		val gens = (0 until numthreads).map(t=>new RequestGenerator(mapping,queue,request_queue))
		val threads = gens.map(g => new Thread(g))
		val startTime = System.currentTimeMillis
		var running = true

		// start!
		(new Thread(requestlogger)).start
		threads.foreach(t=> t.start)

		while (running && !mapping.isEmpty) {
			if (System.currentTimeMillis >= (startTime+numMillis)) {
				logger.info("Stopping generators")
				requestlogger.stop
				gens.foreach(g=> {g.stop; logger.info(g.total_request_gen_time+","+g.request_count+","+g.exception_count)})
				running = false
			}
			else {
				request_queue.put(num_requests)
				num_requests+=1
				Thread.sleep(wait_time)
			}
			//Thread.sleep(100)
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

class RequestGenerator(mapping: Map[PolicyRange,RemoteNode], request_info:java.util.concurrent.BlockingQueue[String], request_queue:java.util.concurrent.BlockingQueue[Long]) extends Runnable {
	val logger = Logger.getLogger("scads.requestgen")
	
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
	
	class ScadsRequestActor(dest:List[RemoteNode], scads_req:Object,log:Boolean) extends Actor {
		var starttime:Long = -1L; var startNano:Long = -1L
		var endtime:Long = -1L; var latency:Double = -1.0
		def act = {
			starttime = System.currentTimeMillis
			startNano = System.nanoTime
			val req = new Message
			req.body = scads_req // get or put request
			req.dest = new Utf8("Storage")
			req.src = new java.lang.Long(MessageHandler.registerActor(self))
			//logger.info("Sending message as "+req.src)
			makeRequest(req)
		}
		def makeRequest(req:Message):Object = {
			// send the request
			MessageHandler.sendMessage(dest(0), req) // go to only first node

			// wait for response
			reactWithin(10000) {
				case (RemoteNode(hostname, port), msg: Message) => msg.body match {
					case exp: ProcessingException => { logger.warn("Exception making request: "+exp); exception_count +=1}
					case obj => {
						endtime = System.currentTimeMillis
						latency = (System.nanoTime-startNano)/1000000.0
						request_count += 1
						// log start_time, end_time, latency, hostname
						if (log) request_info.put(starttime+","+endtime+","+latency+","+hostname+"\n")
					}
				}
				case TIMEOUT => exception_count +=1
				case msg => logger.warn("Unexpected message: " + msg)
			}
		}
	}
	
	def run = {
		while (running) {
			doRequest(request_queue.take)		
		}
	}
	def doRequest(id:Long) = {
		startr = System.nanoTime
		val request = generateRequest
		request_gen_time = System.nanoTime
		total_request_gen_time += (request_gen_time-startr)
		//logger.info("Starting request after "+request_gen_time+"-"+startr)
		request.start // actually send request
	}
	def stop = { logger.info("Ending generation"); running = false }
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
		new ScadsRequestActor(nodes,gr,(rnd.nextDouble<0.02))
		
	}
	private def locate(needle:Int):List[RemoteNode] = {
		mapping.filter(e=>e._1.contains(needle)).toList.map(n=>n._2)//(0)._2 // pick first one, and get node
	}
}

