case class StorageNode(host: String, thriftPort: Int, syncPort: Int) {
	def this(host: String, port: Int) = this(host, port, port)
	def syncHost = host + ":" + syncPort

	def 
}
