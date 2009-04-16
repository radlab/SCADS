import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.protocol.TBinaryProtocol

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