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

	val zooKeeper = ZooKeep.start("target/testCluster", 2181).root.getOrCreate("scads")
	val handler = ScalaEngine.main(9000, "localhost:2181", Some(path), None, false,false)
	val node = RemoteNode("localhost", 9000)
	val cluster = new ScadsCluster(zooKeeper)

  @deprecated("Use ScadsCluster to create/maintain namespaces")
  def createNamespace(ns: String, keySchema: Schema, valueSchema: Schema): Unit = {
    cluster.createNamespace(ns, keySchema, valueSchema)
    val cr = new ConfigureRequest
    cr.namespace = ns
    cr.partition = "1"
    println("Making CR request: " + cr)
    Sync.makeRequest(node, new ActorName("Storage"), cr)
  }

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
