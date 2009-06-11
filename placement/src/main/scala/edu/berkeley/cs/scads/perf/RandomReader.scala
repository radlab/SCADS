package edu.berkeley.cs.scads

case class RandomReader(testData: Map[String, String], server: String, maxKey: Int, requests: Int) extends Runnable with CSVWriter {
	case class SCADSClient(host: String, port: Int) extends ROWAClientLibrary with RemoteKeySpaceProvider
	val client = new SCADSClient(server, 8000)
	val resultPrefix = "/mnt"

	def run() {
		var result = Map("data_placement_server" -> server, "max_key" -> maxKey.toString, "requests" -> requests.toString, "client_machine" -> java.net.InetAddress.getLocalHost().getHostName(), "client_threat" -> Thread.currentThread().getName()) ++ testData
		val keyFormat = new java.text.DecimalFormat("000000000000000")
		val rand = new java.util.Random
		def getKey(key: Int) = keyFormat.format(key)

		result +=  ("start_time" -> System.currentTimeMillis().toString)
		for(i <- 1 to requests) {
			val key = getKey(rand.nextInt(maxKey))
			client.get("perfTest", key)
		}
		result +=  ("end_time" -> System.currentTimeMillis().toString)

		println("test complete")
		output(result)
	}
}
