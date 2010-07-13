package edu.berkeley.cs.scads.comm

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.SyncVar

import org.apache.zookeeper.server.ServerConfig
import org.apache.zookeeper.server.ZooKeeperServerMain

import org.apache.log4j.Logger

/**
 * Helper object for spinning up a local zookeeper instance.  Used primarily for testing.
 */
object ZooKeeperHelper {
  private val basePort = 2000
  private var currentPort = new AtomicInteger
	private val logger = Logger.getLogger("scads.test")

  /**
   * Create a local zookeeper instance in JVM and return a ZooKeeperProxy for it.  Intended for testing purposes only.
   */
  def getTestZooKeeper(): ZooKeeperProxy = {
    val workingDir = File.createTempFile("scads", "zookeeper")
    workingDir.delete()
    workingDir.mkdir()

    var port = 0
    val success = new SyncVar[Boolean]

    do {
      success.unset
      port = basePort + currentPort.getAndIncrement()
      logger.debug("Attempting to start test ZooKeeper on port: " + port)

      val zooThread = new Thread() {
        override def run() = try {
          val config = new ServerConfig()
          config.parse(Array[String](port.toString, workingDir.toString))
          val server = new ZooKeeperServerMain
          server.runFromConfig(config)
        } catch {
          case portInUse: java.net.BindException => success.set(false)
          case otherError => {
            logger.warn("Unexpected error when creating test zookeeper: " + otherError + ". Attempting again")
            success.set(false)
          }
        }
      }
      zooThread.start()

      var connected = false
      while(!connected && !success.isSet) {
        try {
          val s = new java.net.Socket("localhost", port)
          connected = true
          success.set(true)
        }
        catch {
          case ce: java.net.ConnectException => {
            logger.info("Connection to test zookeeper on port " + port + " failed, waiting")
            }
        }
        Thread.sleep(100)
      }
    } while(!success.get)

    new ZooKeeperProxy("localhost:" + port)
  }
}

