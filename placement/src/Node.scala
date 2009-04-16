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

class TestableStorageNode(host: String, port: Int) extends StorageNode(host, port) with Runnable {
	val thread = new Thread(this)
	thread.start()

	def run() {
		Runtime.getRuntime().exec("start_scads.rb -p "+ port)
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