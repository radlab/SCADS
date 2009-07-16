case class KeySpaceServer(port: Int) extends Runnable {
	val spaces = new scala.collection.mutable.HashMap[String, KeySpace]
	val thread = new Thread(this, "KeySpaceServer" + port)
	thread.start

	def add(ns: String, ks: KeySpace) = spaces += (ns -> ks)

	def run() {
		val serverSocket = new java.net.ServerSocket(port);
		while(true) {
			val clientSocket = serverSocket.accept()
			println("connection received from " + clientSocket.getInetAddress())
			val oStream = new java.io.ObjectOutputStream(clientSocket.getOutputStream())
			oStream.flush
			val iStream = new java.io.ObjectInputStream(clientSocket.getInputStream())

			val ns = iStream.readObject().asInstanceOf[String]
			oStream.writeObject(spaces(ns))
			iStream.close
			oStream.close
			clientSocket.close
		}
	}
}