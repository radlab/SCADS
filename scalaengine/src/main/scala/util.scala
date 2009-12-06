package edu.berkeley.cs.scads.storage

import java.io.File

import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig

import org.apache.thrift.server.THsHaServer
import org.apache.thrift.transport.TNonblockingServerSocket
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}

import edu.berkeley.cs.scads.thrift._

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

class TestableScalaStorageEngine(id: Int) extends StorageNode("localhost", id) with Runnable{
	val dbDir = new File("target/db" + id)
	TestableStorageNode.rmDir(dbDir)
	dbDir.mkdir

	val config = new EnvironmentConfig();
	config.setAllowCreate(true);
	config.setTransactional(true)
	val env = new Environment(dbDir, config)

	val processor = new StorageEngine.Processor(new StorageProcessor(env, true))
	val transport = new TNonblockingServerSocket(id)
	val protFactory = new TBinaryProtocol.Factory(true, true)
	val serverOpt = new THsHaServer.Options
	serverOpt.maxWorkerThreads=20
	serverOpt.minWorkerThreads=2
	val server = new THsHaServer(processor, transport, protFactory, serverOpt)
	val thread = new Thread(this, "ScalaEngine" + id)
	thread.start()

	def run() = server.serve()

	def this() = {
		this(TestableStorageNode.port)
		TestableStorageNode.port +=1
	}

	}
