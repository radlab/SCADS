import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol

case class StorageNode(host: String, thriftPort: Int, syncPort: Int) extends SCADS.Storage.Client(new TBinaryProtocol(new TSocket(host, thriftPort))) {
	iprot_.getTransport.open()

	def this(host: String, port: Int) = this(host, port, port)

	def syncHost = host + ":" + syncPort
}

trait ThriftConversions {
	implicit def keyRangeToScadsRangeSet(x: KeyRange):SCADS.RecordSet = {
		val recSet = new SCADS.RecordSet
		val range = new SCADS.RangeSet
		recSet.setType(SCADS.RecordSet.RANGE)
		recSet.setRange(range)
		range.setStart_key(x.start)
		range.setEnd_key(x.end)

		return recSet
	}
}