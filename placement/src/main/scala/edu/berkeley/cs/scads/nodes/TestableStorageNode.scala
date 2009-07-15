package edu.berkeley.cs.scads.nodes

import org.apache.log4j.Level

object TestableStorageNode {
	var port = 9000
}

abstract class TestableStorageNode(thriftPort: Int, syncPort: Int) extends StorageNode("127.0.0.1", thriftPort, syncPort) with Runnable {
	class ProcKiller(t: Thread, p: Process) extends Runnable {
		def run() = {
			logger.setLevel(Level.FATAL)
			p.destroy()
		}
	}

  def rmDir(dir: java.io.File): Boolean = {
    if (dir.isDirectory()) {
      val children = dir.list();
      children.foreach(
        child => {
          if (!rmDir(new java.io.File(dir,child)))
            return false
        }
      )
    }
    dir.delete();
  }

	@transient
	var proc: Process = null
	@transient
	val thread = new Thread(this, "StorageNode"+thriftPort)
	
	val dbDir = new java.io.File("target/db")
	if(!dbDir.exists() || !dbDir.isDirectory())
		dbDir.mkdir()
	val testDir = new java.io.File("target/db/test" + thriftPort)
	if(testDir.exists()) {
    logger.debug("Removing existing test dir: "+testDir)
		if (!rmDir(testDir))
      logger.debug("Failed to remove existing test dir: "+testDir)
  }
	testDir.mkdir()
  logger.debug("created " + testDir + " for testable node")

	var started = false
	def execCmd: String
	def startedMsg: String
	
	thread.start

	val startTime = System.currentTimeMillis()
  logger.debug("Waiting for storage engine " + this + " to come up.")
	while(!started) {
		if(System.currentTimeMillis() - startTime > 20000)
			throw new Exception("failed to connect to " + this + " after " + ((System.currentTimeMillis() - startTime) / 1000) + "seconds")
		Thread.`yield`
	}
  logger.debug("Successfully started " + this)
	
	def run() {
    logger.debug("about to exec " + execCmd)
		proc = Runtime.getRuntime().exec(execCmd)
		Runtime.getRuntime().addShutdownHook(new Thread(new ProcKiller(thread, proc)))
		val reader = new java.io.BufferedReader(new java.io.InputStreamReader(proc.getInputStream()), 1)
		
		try{
			var line = reader.readLine()
			while(line != null) {
				if(!started && line.contains(startedMsg))
					started = true
        logger.debug(line)
				line = reader.readLine()
			}
		}
		catch {
			case ex: java.io.IOException => logger.warn("Storage engine " + this + " died.")
		}
	}

	def stop() {
		proc.destroy()
	}

	override def finalize() {
		proc.destroy()
	}
}
