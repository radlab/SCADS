package edu.berkeley.cs.scads

trait KeySpaceProvider {
	def getKeySpace(ns: String):KeySpace
	def refreshKeySpace()
}

trait DataPlacement {
	def assign(node: StorageNode, range: KeyRange)
	def copy(keyRange: KeyRange, src: StorageNode, dest: StorageNode)
	def move(keyRange: KeyRange, src: StorageNode, dest: StorageNode)
	def remove(keyRange: KeyRange, node: StorageNode)
}

class SimpleDataPlacementCluster(ns:String, freq:Int) extends SimpleDataPlacement(ns) with SynchronousHeartbeatCluster {
	val interval = freq
	var nodes = Set[StorageNode]()

	def join(n: StorageNode): Boolean = {
		nodes += n
		nodes.contains(n)
	}
	
	def leave(n: StorageNode): Boolean = {
		nodes -= n
		!nodes.contains(n)
	}
	
	override def assign(node: StorageNode, range: KeyRange) = {
		super.assign(node,range)
		join(node)
	}
	
	override def remove(node: StorageNode) = {
		super.remove(node)
		leave(node)
	}
}

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

@serializable
class SimpleDataPlacement(ns: String) extends SimpleKeySpace with ThriftConversions with DataPlacement {
	val nameSpace = ns
	val conflictPolicy = new SCADS.ConflictPolicy()
	conflictPolicy.setType(SCADS.ConflictPolicyType.CPT_GREATER)

	override def assign(node: StorageNode, range: KeyRange) {
		super.assign(node, range)
		node.getClient().set_responsibility_policy(nameSpace, range)
	}

	def copy(keyRange: KeyRange, src: StorageNode, dest: StorageNode) {
		val newDestRange = lookup(dest) + keyRange

		//Verify the src has our keyRange
		assert((lookup(src) & keyRange) == keyRange)

		//Tell the servers to copy the data
		src.getClient().copy_set(nameSpace, keyRange, dest.syncHost)

		//Change the assigment
		assign(dest, newDestRange)

		//Sync keys that might have changed
		src.getClient().sync_set(nameSpace, keyRange, dest.syncHost, conflictPolicy)
	}

	def move(keyRange: KeyRange, src: StorageNode, dest: StorageNode) {
		val newSrcRange = lookup(src) - keyRange
		val newDestRange = lookup(dest) + keyRange

		src.getClient().copy_set(nameSpace, keyRange, dest.syncHost)
		assign(dest, newDestRange)
		assign(src, newSrcRange)

		src.getClient().sync_set(nameSpace, keyRange, dest.syncHost, conflictPolicy)
		src.getClient().remove_set(nameSpace, keyRange)
	}

	def remove(keyRange: KeyRange, node: StorageNode) {
		val newRange = lookup(node) - keyRange

		assign(node, newRange)
		lookup(keyRange).foreach((n) => {
			node.getClient().sync_set(nameSpace, n._2, n._1.syncHost, conflictPolicy)
		})
		node.getClient().remove_set(nameSpace, keyRange)
	}
}