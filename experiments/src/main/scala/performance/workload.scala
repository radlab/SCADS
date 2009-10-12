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

class WorkloadAgent(client:ClientLibrary, workload:WorkloadDescription, userID:Int) extends Runnable {
	val wait_sec = 0
	
	val getReportProbability = 0.02
	val putReportProbability = 0.40
	
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
		threadlog("starting workload generation. thread:"+thread_name+"  useID="+userID)
		
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

		var result = new scala.collection.mutable.ListBuffer[String]() // "request,threads,types,start,end,latency\n"
		var requestI = 0;
		var running = true;
				
		while (running) {
			
			if (currentIntervalDescription.numberOfActiveUsers >= userID) {
				threadlog("ACTIVE")
				// I'm active, send a request
				requestI += 1
				
				// create the request
				val request = currentIntervalDescription.requestGenerator.generateRequest(client, System.currentTimeMillis-workloadStart)

				val reportProb = if (request.reqType=="get") getReportProbability
								else if (request.reqType=="put") putReportProbability
								else 0.0
				severity = if (WorkloadDescription.rnd.nextDouble < reportProb) {1} else {6}

				XTraceContext.startTraceSeverity(thread_name,"Initiated: LocalRequest",severity,"RequestID: "+requestI)
				
				try {
					startt = System.nanoTime()
					startt_ms = System.currentTimeMillis()
					request.execute
					endt = System.nanoTime()
					endt_ms = System.currentTimeMillis()
					latency = endt-startt
					result += (localIP+","+thread_name+","+requestI+","+request.reqType+","+startt_ms+","+endt_ms+","+(latency/1000000.0)+"\n")
					threadlog("executed: "+request.toString+"    latency="+(latency/1000000.0))
				} catch {
					case e: Exception => threadlog("got an exception. \n"+stack2string(e))
				}
				
				XTraceContext.clearThreadContext()

				// periodically flush log to disk and clear result list
				if (requestI%5000==0) { 
					if (logwriter != null) {
						logwriter.write(result.mkString)
						logwriter.flush()
					}
					//result = result.remove(p=>true)
					result = new scala.collection.mutable.ListBuffer[String]()
				}
				
				threadlog("thinking: "+workload.thinkTimeMean)
				Thread.sleep(workload.thinkTimeMean)
				
			} else {
				// I'm inactive, sleep for a while
				//println("inactive, sleeping 1 second")
				threadlog("PASSIVE, SLEEPING 1000ms")
				Thread.sleep(1000)
			}
			
			// check if time for next workoad interval
			val currentTime = System.currentTimeMillis()
			while ( currentTime > nextIntervalTime && running ) {
				currentIntervalDescriptionI += 1
				if (currentIntervalDescriptionI < workload.workload.length) {
					currentIntervalDescription = workload.workload(currentIntervalDescriptionI)
					nextIntervalTime += currentIntervalDescription.duration
					threadlog("switching to workload interval #"+currentIntervalDescriptionI+"/"+workload.workload.length+" @ "+new java.util.Date() )
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
