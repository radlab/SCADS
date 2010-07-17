package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import org.apache.log4j.Logger
import org.apache.avro.Schema
import edu.berkeley.cs.scads.comm.Conversions._
import org.apache.avro.util.Utf8

/**
 * Object that creates a local zookeeper / scads cluster for testing.
 * TODO: Instead of being a singleton it would be nice if this could return multiple concurrent / independent scads clusters.
 * TODO: Instead of hardcoding the directory to be target/testCluster it should use a JVM provided temporary directory for backing storage.
 */
object TestScalaEngine {
	val logger = Logger.getLogger("scads.test")
	val path = new java.io.File("target/testCluster")
	rmDir(path)
	path.mkdir()

	val zooKeeper = ZooKeeperHelper.getTestZooKeeper
	val handler = ScalaEngine.main(9000, Some(zooKeeper.address), Some(path), None, false)
	val node = RemoteNode("localhost", 9000)
	val cluster = new ScadsCluster(zooKeeper.root.getOrCreate("scads"))

	private def rmDir(dir: java.io.File): Boolean = {
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
