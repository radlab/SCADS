package edu.berkeley.cs.scads.nodes

class TestableSimpleStorageNode(thriftPort: Int, syncPort: Int) extends TestableStorageNode(thriftPort, syncPort) {
	def startedMsg = "Opening socket on 0.0.0.0:"
	def execCmd = "ruby -rubygems -I ../lib -I ../storage/engines/simple/ -I ../storage/gen-rb/ ../storage/engines/simple/bin/start_scads.rb -d -p "+ thriftPort + " 2>&1"
	
	def this() {
		this(TestableStorageNode.port, TestableStorageNode.port)
		TestableStorageNode.port += 1
	}
}