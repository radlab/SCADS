package SCADS.perf;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;

trait Connector {
	def host: String
	def port: Int
	
	def useConnection(): Map[String, String]
	def makeRequest(client: SCADS.Storage.Client ): Map[String, String]
}

trait SingleConnection extends Connector {
	private val transport = new TFramedTransport(new TSocket(host, port))
	private val protocol = new TBinaryProtocol(transport)
	private val client = new SCADS.Storage.Client(protocol)
	transport.open()
	
	override def useConnection(): Map[String, String] = {
		makeRequest(client) + ("server" -> host)
	}
}

trait SingleUseConnection extends Connector {
	override def useConnection(): Map[String, String] = {
		val transport = new TFramedTransport(new TSocket(host, port))
		val protocol = new TBinaryProtocol(transport)
		val client = new SCADS.Storage.Client(protocol)
		transport.open()
		var ret = makeRequest(client)
		transport.close()
		
		ret + ("server" -> host)
	}
}



