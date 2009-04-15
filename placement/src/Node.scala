import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol

case class StorageNode(host: String, port: Int) extends SCADS.Storage.Client(new TBinaryProtocol(new TSocket(host, port))) {
	iprot_.getTransport.open()
}