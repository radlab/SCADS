package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import org.apache.log4j.Logger

object TestScalaEngine {
	val logger = Logger.getLogger("scads.test")
	val path = new java.io.File("target/testCluster")
	rmDir(path)
	path.mkdir()

	val zooKeeper = ZooKeep.start("target/testCluster", 2181).root.getOrCreate("scads")
	val handler = ScalaEngine.main(9000, "localhost:2181", Some(path), None, false)
	val node = RemoteNode("localhost", 9000)
	val cluster = new ScadsCluster(zooKeeper)

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
