package edu.berkeley.cs.scads.storage

import com.sleepycat.je.{ Environment, EnvironmentConfig }

import edu.berkeley.cs.scads.comm._
import net.lag.logging.Logger
import java.io.File

import java.util.concurrent.ConcurrentHashMap
import java.lang.{ Integer => JInteger }

/**
 * Object that creates a local zookeeper / scads cluster for testing.
 */
object TestScalaEngine {
  lazy val zooKeeper = ZooKeeperHelper.getTestZooKeeper
  lazy val defaultStorageHandler = getTestHandler()
  protected val logger = Logger()

  implicit def toOption[A](a: A) = Option(a)

  /** The default name for a test scads cluster */
  private val testCluster = zooKeeper.root.getOrCreate("testScads")

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

  def getTestClusterWithoutAllocation(): ScadsCluster = 
    new ScadsCluster(testCluster)

  private def makeStorageHandler(dir: File, zooRoot: ZooKeeperProxy#ZooKeeperNode): StorageHandler = {
    val config = new EnvironmentConfig
    config.setAllowCreate(true)
    config.setTransactional(true)
    config.setSharedCache(true) /* share cache w/ all other test handlers in proces */

    /** Try to never run the checkpointer (mimics no writes to log) */
    config.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, JInteger.MAX_VALUE.toString)

    logger.info("Opening test BDB Environment: " + dir + ", " + config)
    val env = new Environment(dir, config)
    new StorageHandler(env, zooRoot)
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
    makeStorageHandler(tempDir, testCluster)
  }

  class TestHandlerGroup(dir: File, root: ZooKeeperProxy#ZooKeeperNode) {
    def getHandler(): StorageHandler =
      makeStorageHandler(dir, root)
  }

  /** Create a new TestHandlerGroup. each invocation of getHandler on the
   * same group object will return a storage handler backed by the exact same
   * BDB env, with the exact same zookeeper root node */
  def getTestHandlerGroup(): TestHandlerGroup = {
    val tempDir = makeScadsTempDir()
    val groupId = java.util.UUID.randomUUID.toString
    val zooAddress = zooKeeper.root.getOrCreate(groupId)
    new TestHandlerGroup(tempDir, zooAddress)
  }

  /**
   * Returns `count` new StorageHandler instances. The semantics for the new
   * instance are those of invoking `getTestHandler` `count` times.
   */
  def getTestHandler(count: Int): List[StorageHandler] = (1 to count).map(_ => getTestHandler()).toList

  def withHandler[T](f: StorageHandler => T): T = {
    val handler = getTestHandler()
    val ret = f(handler)
    handler.stop
    ret
  }

  def withCluster[T](f: ScadsCluster => T): T = {
    val handler = getTestHandler()
    val cluster = new ScadsCluster(handler.root)
    val ret = f(cluster)
    handler.stop
    ret
  }

}
