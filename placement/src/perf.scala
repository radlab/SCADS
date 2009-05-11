case class RandomReader(test_id: String, server: String, maxKey: Int, requests: Int) extends Runnable {
	case class SCADSClient(host: String, port: Int) extends ROWAClientLibrary with RemoteKeySpaceProvider
	val client = new SCADSClient(server, 8000)
	
	def run() {
		val keyFormat = new java.text.DecimalFormat("000000000000000")
		val rand = new java.util.Random
		def getKey(key: Int) = keyFormat.format(key)

		val start = System.currentTimeMillis()
		for(i <- 1 to requests) {
			val key = getKey(rand.nextInt(50*1024))
			client.get("perfTest", key)
		}
		val end = System.currentTimeMillis()
		println("test complete: " + (end - start))

		val resultfile = new java.io.File("/mnt/results." + java.net.InetAddress.getLocalHost().getHostName() +".csv")
		val newfile = !resultfile.exists()
		val writer = new java.io.FileWriter(resultfile, true)

		if(newfile){
			writer.write("test, client, server, num_requests, start_time, end_time\n")
		}
		
		writer.write(test_id + "," + java.net.InetAddress.getLocalHost().getHostName() + "," + server + "," + requests + "," + start + "," + end + "\n")
		writer.flush
	}
}