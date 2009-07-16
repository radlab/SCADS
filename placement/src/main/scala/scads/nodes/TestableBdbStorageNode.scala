package edu.berkeley.cs.scads.nodes

class TestableBdbStorageNode(thriftPort: Int, syncPort: Int) extends TestableStorageNode(thriftPort, syncPort) {
	def startedMsg = "Starting nonblocking server..."
	def execCmd = "../storage/engines/bdb/storage.bdb -p " + thriftPort + " -l " + syncPort + " -d " + testDir + " -t nonblocking 2>&1"
	
	def this() {
		this(TestableStorageNode.port, TestableStorageNode.port + 1000)
		TestableStorageNode.port += 1
	}
}