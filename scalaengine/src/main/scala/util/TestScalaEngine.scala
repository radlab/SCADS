package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import org.apache.log4j.Logger
import org.apache.avro.Schema
import edu.berkeley.cs.scads.comm.Conversions._
import org.apache.avro.util.Utf8
import java.io.File

/**
 * Object that creates a local zookeeper / scads cluster for testing.
 */
object TestScalaEngine {
  lazy val zooKeeper = ZooKeeperHelper.getTestZooKeeper
  lazy val defaultStorageHandler = getTestHandler()
	protected val clusterId = new java.util.concurrent.atomic.AtomicInteger
  protected val logger = Logger.getLogger("scads.test")

  def getTestCluster(): ScadsCluster = {
    val handler = getTestHandler()
    new ScadsCluster(handler.root)
  }

  def getTestHandler(): StorageHandler = {
    val tempDir = File.createTempFile("scads", "testdb")
    tempDir.delete()
    tempDir.mkdir()

    ScalaEngine.main(Some("testScads" + clusterId.getAndIncrement), Some(zooKeeper.address), Some(tempDir), None, false)
  }

  def getTestHandler(count: Int): Seq[StorageHandler] = {
    val root = Some("testScads" + clusterId.getAndIncrement)
    (1 to count).map(i => {
      val tempDir = File.createTempFile("scads", "testdb")

      tempDir.delete()
      tempDir.mkdir()

      ScalaEngine.main(root, Some(zooKeeper.address), Some(tempDir), None, false)
    })
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
