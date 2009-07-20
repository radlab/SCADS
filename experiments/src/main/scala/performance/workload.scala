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

object WorkloadAgentTest {
	
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
	
}


@serializable
class WorkloadIntervalDescription(
	val numberOfActiveUsers: Int,
	val duration: Int,
	val requestGenerator: SCADSRequestGenerator)


@serializable
class WorkloadDescription(
	val thinkTimeMean: Long, 
	val workload: List[WorkloadIntervalDescription]
	) 
{
	def serialize(file: String) = {
        val out = new ObjectOutputStream( new FileOutputStream(file) )
        out.writeObject(this);
        out.close();
	}
}

object WorkloadDescription {
	def deserialize(file: String): WorkloadDescription = {
        val fin = new ObjectInputStream( new FileInputStream(file) )
        val wd = fin.readObject().asInstanceOf[WorkloadDescription]
        fin.close
        wd
	}
}

class WorkloadAgent(client:ClientLibrary, workload:WorkloadDescription, userID:Int) extends Runnable {
/*case class WorkloadAgent(client: ClientLibrary,namespace:String, max_key:Int, workload:WorkloadDescription, userID:Int, testid: Long) extends XtRequest {*/
	import java.util.Random
	val rand = new Random()
	val max_prob = 100
	val wait_sec = 0
	val report_probability = 2.0
	
	def run() = {
		val thread_name = Thread.currentThread().getName()
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
				request.execute
				endt = System.nanoTime()
				endt_ms = System.currentTimeMillis()
				XTraceContext.clearThreadContext()

				// log 20% of reports
				if (requestI%5==0) { latency = endt-startt; result += (thread_name+"-"+requestI+","+request.reqType+","+"N/A"+","+startt_ms+","+endt_ms+","+(latency/1000000.0)+"\n") }

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
				println("inactive, sleeping 1 second")
				Thread.sleep(1000)
			}
			
			// check if time for next workoad interval
			val currentTime = System.currentTimeMillis()
			if (currentTime > nextIntervalTime) {
				currentIntervalDescriptionI += 1
				if (currentIntervalDescriptionI < workload.workload.length) {
					currentIntervalDescription = workload.workload(currentIntervalDescriptionI)
					nextIntervalTime += currentIntervalDescription.duration
					println("switching to next workload interval @ "+new java.util.Date() )
				} else
					running = false
			}
			
		}

		if (logwriter != null) { logwriter.write(result.mkString); logwriter.flush(); logwriter.close() }
	}
	
}
