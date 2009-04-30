import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TTransportException
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TServerTransport
import org.apache.thrift.transport.TServerSocket
import org.apache.thrift.server.TServer
import org.apache.thrift.server.TThreadPoolServer

case class StorageNode(host: String, thriftPort: Int, syncPort: Int) {
	@transient
	var client: SCADS.Storage.Client = null
	def this(host: String, port: Int) = this(host, port, port)
	def syncHost = host + ":" + syncPort

	def getClient(): SCADS.Storage.Client = {
		if(client == null) {
			val transport = new TFramedTransport(new TSocket(host, thriftPort))
			client = new SCADS.Storage.Client(new TBinaryProtocol(transport))
			transport.open()
		}
		return client
	}
	
	override def clone() = {
		new StorageNode(host, thriftPort, syncPort)
	}
}

object TestableStorageNode {
	var port = 9000

	def syncPort(thriftPort: Int): Int = {
		if((System.getProperty("storage.engine") != null) && (System.getProperty("storage.engine") equals "bdb"))
			thriftPort+1000
		else
			thriftPort
	}
}

class TestableStorageNode(thriftPort: Int, syncPort: Int) extends StorageNode("127.0.0.1", thriftPort, syncPort) with Runnable {
	class ProcKiller(p: Process) extends Runnable {
		def run() = p.destroy()
	}

	var proc: Process = null
	val thread = new Thread(this, "StorageNode"+thriftPort)
	var lines = new Array[String](0)

	thread.start

	var startTime = System.currentTimeMillis() 
	while(!lines.contains("Opening socket on 0.0.0.0:" + thriftPort) && !lines.contains("Starting nonblocking server...")) {
		if(System.currentTimeMillis() - startTime > 20000)
			throw new Exception("failed to connect to " + this + " after " + (System.currentTimeMillis() - startTime))
		Thread.`yield`
	}

	println("connected to localhost, " + thriftPort + ", " + syncPort)

	def this() {
		this(TestableStorageNode.port, TestableStorageNode.syncPort(TestableStorageNode.port))
		TestableStorageNode.port += 1
	}

	def run() {
		var logFile: java.io.FileOutputStream = null

		if((System.getProperty("storage.engine") != null) && (System.getProperty("storage.engine") equals "bdb")) {
			val dbDir = new java.io.File("db")
			if(!dbDir.exists() || !dbDir.isDirectory())
				dbDir.mkdir()

			val testDir = new java.io.File("db/test" + thriftPort)

			if(testDir.exists()) {
				testDir.delete()
			}

			testDir.mkdir()

			logFile = new java.io.FileOutputStream("db/test" + thriftPort + "/bdb.log", false)

			proc = Runtime.getRuntime().exec("../storage/engines/bdb/storage.bdb -p " + thriftPort + " -l " + syncPort + " -d db/test" + thriftPort + " -t nonblocking 2>&1")
		}
		else {
			logFile = new java.io.FileOutputStream("ruby.log", true)
			proc = Runtime.getRuntime().exec("ruby -I ../lib -I ../storage/engines/simple/ -I ../storage/gen-rb/ ../storage/engines/simple/bin/start_scads.rb -d -p "+ thriftPort + " 2>&1")
		}
		Runtime.getRuntime().addShutdownHook(new Thread(new ProcKiller(proc)))

		val reader = new java.io.BufferedReader(new java.io.InputStreamReader(proc.getInputStream()), 1)

		try{
			var line = reader.readLine()
			while(line != null) {
				lines = lines ++ Array(line)
				logFile.write((line + "\n").getBytes())
				logFile.flush()
				line = reader.readLine()
			}
		}
		catch {
			case ex: java.io.IOException => //println("StorageEngine " + port + " Exited")
		}
	}

	override def finalize() {
		proc.destroy()
	}
}

trait ThriftConversions {
	implicit def keyRangeToScadsRangeSet(x: KeyRange):SCADS.RecordSet = {
		val recSet = new SCADS.RecordSet
		val range = new SCADS.RangeSet
		recSet.setType(SCADS.RecordSetType.RST_RANGE)
		recSet.setRange(range)
		range.setStart_key(x.start)
		range.setEnd_key(x.end)

		return recSet
	}
}

trait Cluster {
	def nodes: Set[StorageNode]
	def add(n: StorageNode): Boolean
	def remove(n: StorageNode): Boolean
}

class SynchronousHeartbeatCluster(node_list: Set[StorageNode], freq: Int) extends Cluster {
	import java.util.Timer;
	import java.util.TimerTask;
	
	var nodes = node_list
	val timer = new Timer()
	val interval = freq // seconds
	
	def start() = {
		timer.schedule(new PingTask(),0,interval*1000)
	}
	
	def stop() = {
		timer.cancel
	}
	
	private class PingTask extends TimerTask {
		override def run() = {
			nodes.foreach({ case(node) => {
				try {
					//FIXME
				} catch {
					case e:TTransportException => println("node unresponsive: "+node.host+":"+node.thriftPort)
				}
			}})
		}
	}
	
	override def add(n: StorageNode): Boolean = {
		nodes += n
		nodes.contains(n)
	}
	override def remove(n: StorageNode): Boolean = {
		nodes -= n
		!nodes.contains(n)
	}
}


/**
* Use Thrift to connect over socket to a host:port.
* Intended for clients to connect to servers.
*/
trait ThriftConnection {
	def host: String
	def port: Int
	
	val transport = new TSocket(host, port)
	val protocol = new TBinaryProtocol(transport)
}

/**
* Use Thrift to start a server listening on a socket at localhost:port.
*/
trait ThriftServer extends java.lang.Thread {
	def port: Int
	def processor: TProcessor

	private val serverTransport = new TServerSocket(port)
	private val protFactory = new TBinaryProtocol.Factory(true, true)
	private val server = new TThreadPoolServer(processor, serverTransport,protFactory)
	
	override def run = {
		try {
			println("starting server on "+port)
			server.serve
		} catch { case x: Exception => x.printStackTrace }		
	}
}