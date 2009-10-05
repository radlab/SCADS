package edu.berkeley.cs.scads.nodes

import org.apache.log4j.Level
import java.io.BufferedReader

object TestableStorageNode {
	var port = 9000

	def rmDir(dir: java.io.File): Boolean = {
		if (dir.isDirectory()) {
			val children = dir.list();
			children.foreach((child) => {
				if (!rmDir(new java.io.File(dir,child)))
					return false
					})
		}
		dir.delete();
	}

}

abstract class TestableStorageNode(thriftPort: Int, syncPort: Int) extends StorageNode("127.0.0.1", thriftPort, syncPort) {
	class StreamGobbler(in: BufferedReader, func: (String) => Unit) extends Runnable {
		def run() {
			try{
				var line = in.readLine()
				while(line != null) {
					func(line)
					line = in.readLine()
				}
			}
			catch {
				case e: Exception => logger.warn("Failure to read " + this + " died: " + e)
			}
		}
	}

	class ProcKiller(p: Process) extends Runnable {
		def run() = {
			logger.setLevel(Level.FATAL)
			p.destroy()
		}
	}

	@transient
	var proc: Process = null

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

	logger.debug("about to exec " + execCmd)
	proc = Runtime.getRuntime().exec(execCmd)
	Runtime.getRuntime().addShutdownHook(new Thread(new ProcKiller(proc)))
	val stdoutThread = new Thread(new StreamGobbler(
			new java.io.BufferedReader(new java.io.InputStreamReader(proc.getInputStream()), 1),
			(line) => {
				if(!started && line.contains(startedMsg))
					started = true
					logger.debug(line)
					}),
		"StorageEngine" + thriftPort + "stdout"
	)

	val stderrThread = new Thread(new StreamGobbler(
			new java.io.BufferedReader(new java.io.InputStreamReader(proc.getErrorStream()), 1),
			(line) => logger.warn(line)),
		"StorageEngine" + thriftPort + "stderr"
	)

	stdoutThread.start
	stderrThread.start

	val startTime = System.currentTimeMillis()
	logger.debug("Waiting for storage engine " + this + " to come up.")
	while(!started) {
		if(System.currentTimeMillis() - startTime > 20000)
		throw new Exception("failed to connect to " + this + " after " + ((System.currentTimeMillis() - startTime) / 1000) + "seconds")
		Thread.`yield`
	}

	logger.debug("Successfully started " + this)


	def rmDir(dir: java.io.File): Boolean = {
	if (dir.isDirectory()) {
		val children = dir.list();
		children.foreach((child) => {
			if (!rmDir(new java.io.File(dir,child)))
			return false
			})
		}
		dir.delete();
	}

	def stop() {
		proc.destroy()
	}

	override def finalize() {
		proc.destroy()
	}
}
