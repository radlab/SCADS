package performance

import edu.berkeley.xtrace._
import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.nodes._
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.placement._
import edu.berkeley.cs.scads.client._
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol,XtBinaryProtocol}

import java.io._
import java.net._

/*object WorkloadAgentTest {
	
	def linearWorkload(readProb:Double, namespace:String, totalUsers:Int, userStartDelay:Int, thinkTime:Int) = {
		
		// create sample workload description
		val mix = Map("get"->readProb,"put"->(1-readProb))
		val parameters = Map("get"->Map("minKey"->"0","maxKey"->"10000","namespace"->namespace),
							 "put"->Map("minKey"->"0","maxKey"->"10000","namespace"->namespace))
		val reqGenerator = new SimpleSCADSRequestGenerator(mix,parameters)		
		var intervals = new scala.collection.mutable.ListBuffer[WorkloadIntervalDescription]
		
		for (nusers <- 1 to totalUsers) {
			val interval = new WorkloadIntervalDescription(nusers, userStartDelay, reqGenerator)
			intervals += interval
		}
		val w = new WorkloadDescription(thinkTime, intervals.toList)
		w
	}
	
	def disjointWorkload(readProb:Double, namespace:String, totalUsers:Int, userStartDelay:Int, thinkTime:Int) = {
		// workload in which gets and puts are from disjoint subsets of the keyspace
		val mix = Map("get"->readProb,"put"->(1-readProb))
		val parameters = Map("get"->Map("minKey"->"0","maxKey"->"10000","namespace"->namespace),
							 "put"->Map("minKey"->"20000","maxKey"->"30000","namespace"->namespace))
		val reqGenerator = new SimpleSCADSRequestGenerator(mix,parameters)
		var intervals = new scala.collection.mutable.ListBuffer[WorkloadIntervalDescription]

		for (nusers <- 1 to totalUsers) {
			val interval = new WorkloadIntervalDescription(nusers, userStartDelay, reqGenerator)
			intervals += interval
		}
		val w = new WorkloadDescription(thinkTime, intervals.toList)
		w
	}
	def flatWorkload(readProb:Double, namespace:String, totalUsers:Int, num_minutes:Int, thinkTime: Int) = {
		// how many minutes to run test flat workload: all users start making requests at the same time
		val mix = Map("get"->readProb,"put"->(1-readProb))
		val parameters = Map("get"->Map("minKey"->"0","maxKey"->"10000","namespace"->namespace),
							 "put"->Map("minKey"->"0","maxKey"->"10000","namespace"->namespace))
		val reqGenerator = new SimpleSCADSRequestGenerator(mix,parameters)
		var intervals = new scala.collection.mutable.ListBuffer[WorkloadIntervalDescription]
		val interval = new WorkloadIntervalDescription(totalUsers, num_minutes*60000, reqGenerator)
		intervals += interval

		val w = new WorkloadDescription(thinkTime, intervals.toList)
		w
	}
	
}
*/


class WorkloadAgent(client:ClientLibrary, workload:WorkloadDescription, userID:Int) extends Runnable {
	import java.util.Random
	val rand = new Random()
	val max_prob = 100
	val wait_sec = 0
	val report_probability = 2.0
	
	val localIP = InetAddress.getLocalHost().getHostName 

	var threadlogf: FileWriter = null
	def threadlog(line: String) {
		threadlogf.write(new java.util.Date() + ": " + line+"\n")
		threadlogf.flush
	}

	// if choose to use xtrace reporting, report locally using TCP
	System.setProperty("xtrace.reporter","edu.berkeley.xtrace.reporting.TcpReporter")
	System.setProperty("xtrace.tcpdest","127.0.0.1:7831")

	def run() = {
		val thread_name = Thread.currentThread().getName()

		(new File("/mnt/workload/logs")).mkdirs()
		threadlogf = new FileWriter( new java.io.File("/mnt/workload/logs/"+thread_name+".log"), true )
		threadlog("starting workload generation. thread:"+thread_name+"\n")
		
		val log = new java.io.File("/mnt/xtrace/logs/"+thread_name)
		val logwriter = if (log.getParentFile().exists()) {new java.io.FileWriter(log, true) } else {null}
		var severity = 1
		var startt = System.nanoTime()
		var startt_ms = System.currentTimeMillis()
		var endt = System.nanoTime()
		var endt_ms = System.currentTimeMillis()
		var latency = endt-startt

		var currentIntervalDescriptionI = 0
		var currentIntervalDescription = workload.workload(currentIntervalDescriptionI)
		val workloadStart = System.currentTimeMillis()
		var nextIntervalTime = workloadStart + currentIntervalDescription.duration

		var result = List[String]() // "request,threads,types,start,end,latency\n"
		var requestI = 0;
		var running = true;
				
		while (running) {
			
			if (currentIntervalDescription.numberOfActiveUsers >= userID) {
				// I'm active, send a request
				requestI += 1
				
				// create the request
				val request = currentIntervalDescription.requestGenerator.generateRequest(client, System.currentTimeMillis-workloadStart)

				severity = if (rand.nextInt(max_prob) < report_probability) {1} else {6}
				XTraceContext.startTraceSeverity(thread_name,"Initiated: LocalRequest",severity,"RequestID: "+requestI)
				startt = System.nanoTime()
				startt_ms = System.currentTimeMillis()
/*				this.makeRequest*/
				
				try {
					request.execute
				} catch {
					case e: Exception => threadlog("got an exception. "+stack2string(e))
				}
				
				endt = System.nanoTime()
				endt_ms = System.currentTimeMillis()
				XTraceContext.clearThreadContext()

				// log 20% of reports
				latency = endt-startt; result += (localIP+","+thread_name+","+requestI+","+request.reqType+","+startt_ms+","+endt_ms+","+(latency/1000000.0)+"\n")

				// periodically flush log to disk and clear result list
				if (requestI%5000==0) { 
					if (logwriter != null) {
						logwriter.write(result.mkString)
						logwriter.flush()
					}
					result = result.remove(p=>true)
				}
				
				Thread.sleep(workload.thinkTimeMean)
				
			} else {
				// I'm inactive, sleep for a while
				//println("inactive, sleeping 1 second")
				Thread.sleep(1000)
			}
			
			// check if time for next workoad interval
			val currentTime = System.currentTimeMillis()
			if (currentTime > nextIntervalTime) {
				currentIntervalDescriptionI += 1
				if (currentIntervalDescriptionI < workload.workload.length) {
					currentIntervalDescription = workload.workload(currentIntervalDescriptionI)
					nextIntervalTime += currentIntervalDescription.duration
					threadlog("switching to next workload interval @ "+new java.util.Date() )
				} else
					running = false
			}
			
		}

		threadlog("done")

		if (logwriter != null) { logwriter.write(result.mkString); logwriter.flush(); logwriter.close() }
		threadlogf.close()
		
	}
	
	def stack2string(e:Exception):String = {
	    val sw = new StringWriter()
	    val pw = new PrintWriter(sw)
	    e.printStackTrace(pw)
	    sw.toString()
  	}
	
}
