package SCADS.perf;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

abstract class RequestMaker(valSize: Int) {
	val keyFormat = new java.text.DecimalFormat("000000000000000")
	val value = ("*" * valSize).getBytes
	
	def getKey(key: Int) = keyFormat.format(key)
	def getRecord(key: Int) = new Record(getKey(key), value)
	def makeRequest(client: SCADS.Storage.Client): Map[String, String]
}

abstract class SequentialWriter(valSize: Int) extends RequestMaker(valSize) {
	var key: Int = 0
	
	def makeRequest(client: SCADS.Storage.Client): Map[String, String] = {
		key += 1
		val record = getRecord(key)
		client.put("perfTest", record)
		Map("request_type" -> "write", "request_pattern" -> "sequential", "key" -> new String(record.key))
	}
}

abstract class RandomReader(mKey: Int) extends RequestMaker(0) {
	val rand = new java.util.Random
	val maxKey = mKey
	
	def makeRequest(client: SCADS.Storage.Client): Map[String, String] = {
		val key = getKey(rand.nextInt(maxKey))
		client.get("perfTest", key)
		Map("request_type" -> "read", "request_pattern" -> "random", "key" -> key, "max_key" -> maxKey.toString())
	}
}