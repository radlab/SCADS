import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.transport.TServerTransport
import org.apache.thrift.transport.TServerSocket
import org.apache.thrift.server.TServer
import org.apache.thrift.server.TThreadPoolServer

case class StorageNode(host: String, thriftPort: Int, syncPort: Int) extends SCADS.Storage.Client(new TBinaryProtocol(new TFramedTransport(new TSocket(host, thriftPort)))) {

	def this(host: String, port: Int) = this(host, port, port)
	def syncHost = host + ":" + syncPort

	def connect() {
		iprot_.getTransport.open()
	}
}

object TestableStorageNode {
	var port = 9000
}

class TestableStorageNode(port: Int) extends StorageNode("localhost", port) with Runnable {
	class ProcKiller(p: Process) extends Runnable {
		def run() = p.destroy()
	}

	var proc: Process = null
	val thread = new Thread(this, "StorageNode"+port)
	var lines = new Array[String](0)

	thread.start

	while(!lines.contains("Opening socket on 0.0.0.0:" + port))
	Thread.`yield`

	connect()

	def this() {
		this(TestableStorageNode.port)
		TestableStorageNode.port += 1
	}

	def run() {
		proc = Runtime.getRuntime().exec("ruby -I ../lib -I ../storage/engines/simple/ -I ../storage/gen-rb/ ../storage/engines/simple/bin/start_scads.rb -p "+ port + " 2>&1")
		Runtime.getRuntime().addShutdownHook(new Thread(new ProcKiller(proc)))

		val reader = new java.io.BufferedReader(new java.io.InputStreamReader(proc.getInputStream()), 1)

		try{
			var line = reader.readLine()
			while(line != null) {
				lines = lines ++ Array(line)
//				println("" + port + ":" + line)
				line = reader.readLine()
			}
		}
		catch {
			case ex: java.io.IOException => //println("StorageEngine " + port + " Exited")
		}
	}

	override def clone(): StorageNode = {
		val n = new StorageNode("localhost", port)
		n.connect()
		return n
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