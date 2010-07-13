package edu.berkeley.cs.scads.storage

import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig
import com.sleepycat.je.jmx.JEMonitor

import edu.berkeley.cs.scads.comm._
import org.apache.log4j.Logger

/**
 * Application for starting up a standalone scads storage engine
 */
object ScalaEngine extends optional.Application {
  private val logger = Logger.getLogger("ScalaEngine")
  def main(port: Int, zooKeeper: Option[String], dbDir: Option[java.io.File], cachePercentage: Option[Int], verbose: Boolean) : StorageHandler = {
    val config = new EnvironmentConfig()
    config.setAllowCreate(true)
    config.setTransactional(true)
    config.setCachePercent(cachePercentage.getOrElse(80))

    val dir = dbDir.getOrElse(new java.io.File("db"))
    if(!dir.exists()) {
      dir.mkdir
    }

    val zooRoot = zooKeeper match {
      case Some(address) => new ZooKeeperProxy(address).root.getOrCreate("scads")
      case None => {
        logger.info("No zookeeper specifed.  Running in standalone mode with local zookeeper")
        ZooKeeperHelper.getTestZooKeeper.root("scads")
      }
    }

    val env = new Environment(dir, config)
    logger.info("Connecting to zookeeper")
    val handler = new StorageHandler(env, zooRoot,
                                     java.net.InetAddress.getLocalHost.getHostName,
                                     port)
    logger.info("Starting listener")
    MessageHandler.registerService("Storage",handler)
    MessageHandler.startListener(port)

    return handler
  }
}
