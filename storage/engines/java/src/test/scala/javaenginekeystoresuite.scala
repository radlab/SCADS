package edu.berkeley.cs.scads.test

import org.scalatest.Suite
import java.io.File
import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.nodes.StorageNode
import edu.berkeley.cs.scads.storage.StorageProcessor
import com.sleepycat.je.Environment

class JavaEngineKeyStoreSuite extends KeyStoreSuite {
	object JavaNode extends StorageNode("localhost", 0, 0) {
		val dir = new File("target/db")
		rmDir(dir)
		dir.mkdir()
		val env = new Environment(dir, null)
		val engine = new StorageProcessor(env)

		override def useConnection[ReturnType](f: StorageEngine.Iface => ReturnType): ReturnType = {
			f(engine)
		}

		override def createConnection() = {
			engine
		}
	}

	def getNode: StorageNode = JavaNode

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
