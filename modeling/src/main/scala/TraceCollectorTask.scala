package edu.berkeley.cs
package scads
package piql
package modeling

import deploylib.mesos._
import comm._
import storage._
import piql._
import perf._
import avro.marker._
import avro.runtime._

import net.lag.logging.Logger
import java.io.File
import java.net._

import javax.mail._
import javax.mail.internet._
//import java.util._

case class TraceCollectorTask(
	var clusterAddress: String, 
	var baseCardinality: Int, 
	var warmupLengthInMinutes: Int = 5, 
	var numStorageNodes: Int = 1, 
	var numQueriesPerCardinality: Int = 1000, 
	var sleepDurationInMs: Int = 100
) extends AvroTask with AvroRecord {
	var beginningOfCurrentWindow = 0.toLong

	def run(): Unit = {
		println("made it to run function")
		val clusterRoot = ZooKeeperNode(clusterAddress)
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    logger.info("Adding servers to cluster for each namespace")
    cluster.blockUntilReady(numStorageNodes)

    /* get namespaces */
		println("getting namespace...")
    val ns = cluster.getNamespace[PrefixedNamespace]("prefixedNamespace")
		/*
    val r1 = cluster.getNamespace[R1]("r1")
    val r2 = cluster.getNamespace[R2]("r2")
		*/

    /* create executor that records trace to fileSink */
		println("creating executor...")
    val fileSink = new FileTraceSink(new File("/mnt/piqltrace.avro"))
    implicit val executor = new ParallelExecutor with TracingExecutor {
      val sink = fileSink
    }

    /* Register a listener that will record all messages sent/recv to fileSink */
		println("registering listener...")
    val messageTracer = new MessagePassingTracer(fileSink)
    MessageHandler.registerListener(messageTracer)

    /* Bulk load some test data into the namespaces */
		println("loading data...")
		ns ++= (1 to 10).view.flatMap(i => (1 to getNumDataItems).map(j => PrefixedNamespace(i,j)))	 // might want to fix hard-coded 10 at some point
		/*
		r1 ++= (1 to getNumDataItems).view.map(i => R1(i))
    r2 ++= (1 to 10).view.flatMap(i => (1 to getNumDataItems).map(j => R2(i,j)))    
		*/

    /**
     * Write queries against relations and create optimized function using .toPiql
     * toPiql uses implicit executor defined above to run queries
     */
		println("creating queries...")
		val cardinalityList = getCardinalityList
		
		val queries = cardinalityList.map(currentCardinality => 
			ns.where("f1".a === 1)
					.limit(currentCardinality)
					.toPiql("getRangeQuery-rangeLength=" + currentCardinality.toString)
		)
		/*
		val queries = cardinalityList.map(currentCardinality => 
			r2.where("f1".a === 1)
		      .limit(currentCardinality)
		      .join(r1)
		      .where("r1.f1".a === "r2.f2".a)
					.toPiql("joinQuery-cardinality=" + currentCardinality.toString)
		)
		*/
    

		// initialize window
		beginningOfCurrentWindow = System.nanoTime
				    
		// warmup to avoid JITing effects
		println("beginning warmup...")
		fileSink.recordEvent(WarmupEvent(warmupLengthInMinutes, true))
		var queryCounter = 1
		while (withinWarmup) {
			cardinalityList.indices.foreach(r => {
				fileSink.recordEvent(ChangeCardinalityEvent(cardinalityList(r)))
				
	    	fileSink.recordEvent(QueryEvent("getRangeQuery" + queryCounter, true))
				//fileSink.recordEvent(QueryEvent("joinQuery" + queryCounter, true))
	      
				queries(r)()
	      
				fileSink.recordEvent(QueryEvent("getRangeQuery" + queryCounter, false))
				//fileSink.recordEvent(QueryEvent("joinQuery" + queryCounter, false))
	
				Thread.sleep(sleepDurationInMs)
				queryCounter += 1
	    })
		}
		fileSink.recordEvent(WarmupEvent(warmupLengthInMinutes, false))
				

    /* Run some queries */
		println("beginning run...")
		cardinalityList.indices.foreach(r => {
			println("current cardinality = " + cardinalityList(r).toString)
			fileSink.recordEvent(ChangeCardinalityEvent(cardinalityList(r)))
			
			(1 to numQueriesPerCardinality).foreach(i => {
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, true))
	      //fileSink.recordEvent(QueryEvent("joinQuery" + i, true))
	
	      queries(r)()
	
	      fileSink.recordEvent(QueryEvent("getRangeQuery" + i, false))
	      //fileSink.recordEvent(QueryEvent("joinQuery" + i, false))

				Thread.sleep(sleepDurationInMs)
	    })
		})

    //Flush trace messages to the file
		println("flushing messages to file...")
    fileSink.flush()

		// Upload file to S3
		println("uploading data...")
		TraceS3Cache.uploadFile("/mnt/piqltrace.avro")
		
		println("Finished with trace collection.")
  }

	def convertMinutesToNanoseconds(minutes: Int): Long = {
		minutes.toLong * 60.toLong * 1000000000.toLong
	}

	def withinWarmup: Boolean = {
		val currentTime = System.nanoTime
		currentTime < beginningOfCurrentWindow + convertMinutesToNanoseconds(warmupLengthInMinutes)
	}
	
	def getNumDataItems: Int = {
		getMaxCardinality*10
	}
	
	def getMaxCardinality: Int = {
		getCardinalityList.sortWith(_ > _).head
	}
	
	def getCardinalityList: List[Int] = {
		((baseCardinality*0.5).toInt :: (baseCardinality*0.75).toInt :: baseCardinality :: baseCardinality*2 :: baseCardinality*10 :: baseCardinality*100:: Nil).reverse
	}
	
	// doesn't work
	def sendMail(message:String) = {
		val props = new java.util.Properties();
		props.put("mail.smtp.host", "calmail.berkeley.edu")
		props.put("mail.smtp.auth", "true")
		
		val auth = new SMTPAuthenticator
		
		val session = Session.getDefaultInstance(props, auth)
		session.setDebug(false)
		
		val msg = new MimeMessage(session)
		
		val addressFrom = new InternetAddress("kristal.curtis@gmail.com")
		msg.setFrom(addressFrom)
		
		val addressTo = new InternetAddress("kristal.curtis@gmail.com")
		msg.setRecipients(javax.mail.Message.RecipientType.TO, "kristal.curtis@gmail.com")
		
		msg.setSubject("experiment finished at " + System.currentTimeMillis)
		msg.setContent("please check data files", "text/plain")
		Transport.send(msg)
		
	}
	
}

// not currently used
class SMTPAuthenticator extends javax.mail.Authenticator {
	override def getPasswordAuthentication = {
		new javax.mail.PasswordAuthentication("kcurtis", "***")
	}
}
