package edu.berkeley.cs.scads.director

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import net.lag.logging.Logger

object ClientRunner extends optional.Application {
  private val logger = Logger()
	org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)
  def main(zooAddress: String, namespace:String, numServers: Int, numClients:Int = 1, workload:String = "/tmp/workload.ser") : IntClient = {
		if (workload == "/tmp/workload.ser") {
			val workload = WorkloadGenerators.linearWorkloadRates(1.0,0,0,200,5000,40000,8,30*1000)
			workload.serialize("/tmp/workload.ser")
		}
		
		val node = ZooKeeperNode(zooAddress)
		val client = AsyncIntClient(numServers,numClients,namespace,workload,200,2,0.01)
		client.run(node)
		return client
  }
}


case class IntClient(var numServers: Int, var numClients: Int, namespace:String,workloadFilePath:String,maxkey:Int,threads:Int) {
	var clientns:GenericNamespace = null
	val failures = new java.util.concurrent.atomic.AtomicLong
	val totalreqs = new java.util.concurrent.atomic.AtomicLong
	var currentHistogram = Histogram(1,500)
	val history = new scala.collection.mutable.ListBuffer[(Histogram,Int)]()
	var requestGenerator:SCADSRequestGenerator = null
	val rnd = new java.util.Random()
	val histogramPeriod = 20*1000 // ms
	var lastHistogramCleared = System.currentTimeMillis
	val logger = Logger("IntClient")
	
	class Requestor() extends Runnable {
		def run():Unit = {
			
			val success = makeRequest(requestGenerator)
			if (!success) failures.getAndIncrement
		}
	}
	
	def run(clusterRoot: ZooKeeperProxy#ZooKeeperNode) = {
    val coordination = clusterRoot.getOrCreate("coordination")
    val cluster = new ScadsCluster(clusterRoot)

    val clientId = coordination.registerAndAwait("clientStart", numClients)
    if(clientId == 0) {
      logger.info("Awaiting scads cluster startup")
      //cluster.blockUntilReady(numServers) // TODO: bring back when Experiment.scala compiles
			//val ns = cluster.createNamespace[IntRec, StringRec](namespace, List((None, storageServers.slice(0,1)), (Some(IntRec(100)), storageServers.slice(0,1)),(Some(IntRec(200)), storageServers.slice(1,2)),(Some(IntRec(300)), storageServers.slice(2,3)),(Some(IntRec(400)), storageServers.slice(3,4)),(Some(IntRec(500)), storageServers.slice(4,5)),(Some(IntRec(600)), storageServers.slice(5,6))  )) // 6 servers, one has two partitions, others have one
			// TODO: set up quorum parameters
		}
		clientns = cluster.getNamespace(namespace)
		
		// bulk load data
		coordination.registerAndAwait("startBulkLoad", numClients)
    logger.info("Begining bulk loading of data")
    clientns ++= getBulkLoadSlice(clientId)
    logger.info("Bulk loading complete")

    coordination.registerAndAwait("loadingComplete", numClients)

		// deserialize workload and initiate state
		val workload =  WorkloadDescription.deserialize(workloadFilePath)
		val stpe = new java.util.concurrent.ScheduledThreadPoolExecutor(threads)
		val total_intervals = workload.workload.size
		var currentIntervalI = 0
		var currentInterval = workload.workload(currentIntervalI)
		var duration = currentInterval.duration
		requestGenerator = currentInterval.requestGenerator
		var period_nanos = ( java.lang.Math.pow(10,9) / (currentInterval.numberOfActiveUsers/numClients) ).toLong
		var previous_total_reqs = 0L
		
		// start the workload experiment
		coordination.registerAndAwait("startWorkload", numClients)
		var startTime = System.currentTimeMillis
		var currentRunner = stpe.scheduleAtFixedRate(new Requestor, 0, period_nanos, java.util.concurrent.TimeUnit.NANOSECONDS)
		//var currentRunner = stpe.scheduleAtFixedRate(new Requestor, 0, 1000000, java.util.concurrent.TimeUnit.NANOSECONDS)
		var running = true
		logger.info("Starting workload")
		while (running) {
			val now = System.currentTimeMillis
			
			// archive last period's performance info
			if (now > lastHistogramCleared + histogramPeriod)
				synchronized {
					history += ((currentHistogram,failures.get.toInt))
					//logger.info("histogram: %s\nfailures: %d",currentHistogram.toString,failures.get)
					currentHistogram = Histogram(1,500)
					failures.set(0)
					lastHistogramCleared = now
				}	
			if (now >= startTime+duration) { // done with this interval
				currentRunner.cancel(true)
				
				if (currentIntervalI < total_intervals-1) { // have more intervals to run
					// start new interval
					currentIntervalI += 1
					currentInterval = workload.workload(currentIntervalI)
					duration = currentInterval.duration
					requestGenerator = currentInterval.requestGenerator
					period_nanos = ( java.lang.Math.pow(10,9) / (currentInterval.numberOfActiveUsers/numClients) ).toLong
					
					startTime = System.currentTimeMillis
					logger.info("total requests %d",totalreqs.get-previous_total_reqs)
					previous_total_reqs = totalreqs.get
					logger.info("Running at "+(java.lang.Math.pow(10,9)/period_nanos).toInt+ " requests/sec "+(new java.util.Date).toString)
					currentRunner = stpe.scheduleAtFixedRate(new Requestor, 0, period_nanos, java.util.concurrent.TimeUnit.NANOSECONDS)
				}
				
				else { // done all intervals
					stpe.shutdown
					running = false
				}
				
			}

		}
		// get the last histogram into history
		synchronized { history += ((currentHistogram,failures.get.toInt)) }
	
		// log data to file
		val requestLog = new java.io.BufferedWriter(new java.io.FileWriter("/tmp/requestlogs.csv"))
		history.foreach(entry => {
			requestLog.write(entry._1.sum+","+entry._2+","+entry._1.percentile(0.5)+","+entry._1.percentile(0.9)+","+entry._1.percentile(0.99)) // total reqs, failures,median, 90th, 99th
			requestLog.newLine
		})
		requestLog.flush
		requestLog.close
		
		System.exit(0)
	}
	
	private def getBulkLoadSlice(id:Int):Iterable[(org.apache.avro.generic.GenericData.Record, org.apache.avro.generic.GenericData.Record)] = {
		val slicesize = scala.math.ceil(maxkey/numClients).toInt
		val start = id * slicesize
		val end = if (id >= numClients-1) maxkey else start + slicesize
		Set( (start until end).toList.map(i => (IntRec(i).toGenericRecord,StringRec(getValue(256)).toGenericRecord) ) :_*)
	}
	private def getValue(length:Int):String = "x" * length 
	
	/**
	* make a request using the given request generator
	* returns boolean indicating request's success 
	*/
	protected def makeRequest(requestGenerator:SCADSRequestGenerator):Boolean = {
		val starttime = System.currentTimeMillis
		var ret = false
		try {
			val request = requestGenerator.generateRequest
			request match {
				case r:ScadsGet => clientns.get(r.key)
				case r:ScadsPut => clientns.put(r.key, r.value)
				case _ => logger.warning("wtf request type")
			}
			totalreqs.getAndIncrement
			synchronized { currentHistogram.add(System.currentTimeMillis - starttime) }
			ret = true
		} catch {
			case e => logger.warning(e, "Query Failed")
		}
		ret
	}
}

case class AsyncIntClient(s: Int, c: Int, ns:String,wl:String,k:Int,t:Int, sampling:Double) extends IntClient(s,c,ns,wl,k,t) {
	
	// (future,start timestamp)
	val request_map = new java.util.concurrent.ArrayBlockingQueue[(ScadsFuture[Option[org.apache.avro.generic.GenericData.Record]],Long)](20000)
	//val pool = new java.util.concurrent.ThreadPoolExecutor(3, 3, 1000, java.util.concurrent.TimeUnit.MILLISECONDS, request_map)
	val pool = java.util.concurrent.Executors.newFixedThreadPool(2)
	val rnd2 = new java.util.Random
	//val requestlog = new RequestLogger()
	//new Thread(requestlog).start
	
	class RequestLog() extends Runnable() {
		def run():Unit = {
			try {
				val request = request_map.take()
				val curval = request._1.get(1)
				if (rnd2.nextDouble < sampling) {
					if (curval != None) currentHistogram.add(System.currentTimeMillis - request._2)
					else request_map.add(request) // put it back at end of list
				}
			} catch { case e => }
		}
	}
	
	class RequestLogger() extends Runnable() {
		var runnin = true
		def run():Unit = {
			while (runnin) {	
				try {
					val request = request_map.take()
					if (request._1.get(1) != None) currentHistogram.add(System.currentTimeMillis - request._2)
					else request_map.add(request) // put it back at end of list
				} catch { case e => }
			} // end while
		}
		def stop = runnin = false
	}
		
	override protected def makeRequest(requestGenerator:SCADSRequestGenerator):Boolean = {
		val starttime = System.currentTimeMillis
		var ret = false
		try {
			val request = requestGenerator.generateRequest
			request match {
				case r:ScadsGet => { request_map.put(clientns.asyncGet(r.key),System.currentTimeMillis); pool.execute(new RequestLog()) }
				case r:ScadsPut => {
					val put = new ComputationFuture[Unit] {
			      def compute(timeoutHint: Long, unit: java.util.concurrent.TimeUnit) = clientns.put(r.key, r.value)
			      def cancelComputation = error("NOT IMPLEMENTED")
			    }
					put.get(1,java.util.concurrent.TimeUnit.MILLISECONDS)
				}
				case _ => logger.warning("wtf request type")
			}
			totalreqs.getAndIncrement
			true
		} catch {
			case e => {
        logger.warning("Query Failed")
				false
			}
		}

	}
}

import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.avro.marker._

import scala.collection.mutable.ArrayBuffer

object Histogram {
  def apply(bucketSize: Int, bucketCount: Int): Histogram =
    new Histogram(bucketSize, ArrayBuffer.fill(bucketCount)(0L))
}

case class Histogram(var bucketSize: Int, var buckets: ArrayBuffer[Long]) extends AvroRecord {
  def +(left: Histogram): Histogram = {
    require(bucketSize == left.bucketSize)
    require(buckets.size == left.buckets.size)

    Histogram(bucketSize, buckets.zip(left.buckets).map{ case (a,b) => a + b })
  }

  def +=(value: Long) {
    add(value)
  }

	def add(value: Long):Histogram = {
		val bucket = (value / bucketSize).toInt
		if(bucket >= buckets.length)
			buckets(buckets.length - 1) += 1
		else
			buckets(bucket) +=1

		this
	}
	
	def sum:Long = buckets.foldLeft(0L){(output,entry) => output + entry}
	def percentile(percent:Double):Long = {
		val goal = scala.math.ceil(percent * sum).toLong
		val totals = buckets.scanLeft(0L){(output,entry) => output + entry}
		totals.takeWhile(_ < goal).size * bucketSize
	}
}