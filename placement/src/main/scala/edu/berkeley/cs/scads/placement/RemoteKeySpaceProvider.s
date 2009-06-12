trait RemoteKeySpaceProvider extends KeySpaceProvider {
	val host: String
	val port: Int
	var cachedKeySpaces = new scala.collection.mutable.HashMap[String, KeySpace]

	def getKeySpace(ns: String): KeySpace = {
		val ret = cachedKeySpaces.get(ns)
		if(ret.isEmpty)
		getFromRemote(ns)
		else
		ret.getOrElse(null)
	}

	def refreshKeySpace() = cachedKeySpaces.clear

	private def getFromRemote(ns:String): KeySpace = {
		val sock = new java.net.Socket(host, port)
		val oStream = new java.io.ObjectOutputStream(sock.getOutputStream())
		oStream.flush()
		val iStream = new java.io.ObjectInputStream(sock.getInputStream())

		oStream.writeObject(ns)
		oStream.flush()
		val ks = iStream.readObject.asInstanceOf[KeySpace]
		cachedKeySpaces += (ns -> ks)
		return ks
	}
}