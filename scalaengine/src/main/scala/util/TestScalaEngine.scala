package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._
import net.lag.logging.Logger
import org.apache.avro.Schema
import edu.berkeley.cs.scads.comm.Conversions._
import org.apache.avro.util.Utf8
import java.io.File

import java.util.concurrent.ConcurrentHashMap

/**
 * Object that creates a local zookeeper / scads cluster for testing.
 */
object TestScalaEngine {
  lazy val zooKeeper = ZooKeeperHelper.getTestZooKeeper
  lazy val defaultStorageHandler = getTestHandler()
  protected val logger = Logger()

  /** Maps (cluster name, test node ID) -> Temp file for BDB env */
  private val tempFileMap = new ConcurrentHashMap[(String, String), File]
  
  /** The default name for a test scads cluster */
  private val TestCluster = "testScads"


  private def makeScadsTempDir() = {
    val tempDir = File.createTempFile("scads", "testdb")
    /* This strange sequence of delete and mkdir is required so that BDB can
     * acquire a lock file in the created temp directory */
    tempDir.delete()
    tempDir.mkdir()
    tempDir
  }

  /**
   * Returns a ScadsCluster instance for the cluster `testScads` guaranteed to 
   * contain at least one node.
   * Creates a new StorageHandler
   */
  def getTestCluster(): ScadsCluster = {
    val handler = getTestHandler()
    new ScadsCluster(handler.root)
  }


  /**
   * Create and return a fresh scala engine, guaranteed to use a BDB 
   * environment backed by a temp directory never used before. 
   * The root of the engine in ZooKeeper will be `testScads`. 
   * The engine returned will be managed by the local running
   * test ZooKeeper instance
   *
   * Thus, multiple invocations of this method will return new handlers which
   * run in their own BDB environments, but will use the same ZooKeeper root,
   * meaning they will be part of the same cluster named `testScads`
   */
  def getTestHandler(): StorageHandler = {
    val tempDir = makeScadsTempDir()
    ScalaEngine.main(Some(TestCluster), Some(zooKeeper.address), Some(tempDir), None, false)
  }

  /**
   * Create and return a new scala engine for the cluster `clusterId` which
   * will share a BDB environment among all engines created by invoking this
   * method with the same `clusterId` and `nodeId` arguments. Thus, invoking
   * this method more than once with the same arguments only makes sense 
   * if in between invocations, the returned StorageHandler instance of the 
   * first invocation is properly shut down first. The use case for this
   * method is to signal a shutdown/restart of a node in a particular cluster.
   */
  def getTestHandler(clusterId: String, nodeId: String): StorageHandler = {
    require(clusterId ne null)
    require(nodeId ne null)

    val tempDir = makeScadsTempDir()
    val tempDir0 = Option(tempFileMap.putIfAbsent((clusterId, nodeId), tempDir))

    ScalaEngine.main(Some(clusterId), Some(zooKeeper.address), tempDir0.orElse(Some(tempDir)), None, false)
  }

  /**
   * Returns `count` new StorageHandler instances. The semantics for the new
   * instance are those of invoking `getTestHandler` `count` times.
   */
  def getTestHandler(count: Int): List[StorageHandler] = (1 to count).map(_ => getTestHandler()).toList

  // currently unused- delete?
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
