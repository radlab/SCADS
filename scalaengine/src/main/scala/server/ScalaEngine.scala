package edu.berkeley.cs.scads.storage

import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig
import com.sleepycat.je.jmx.JEMonitor

import edu.berkeley.cs.scads.comm._
import net.lag.logging.Logger

/**
 * Application for starting up a standalone scads storage engine
 */
object ScalaEngine extends optional.Application {
  private val logger = Logger()
  def main(clusterAddress: Option[String], dbDir: Option[java.io.File], cachePercentage: Option[Int], verbose: Boolean) : StorageHandler = {
    if(verbose)
      org.apache.log4j.BasicConfigurator.configure()

    val config = new EnvironmentConfig()
    config.setAllowCreate(true)
    config.setTransactional(true)
    config.setCachePercent(cachePercentage.getOrElse(80))

    val dir = dbDir.getOrElse(new java.io.File("db"))
    if(!dir.exists()) {
      dir.mkdir
    }

    val zooRoot = clusterAddress.map(p => ZooKeeperNode(p)).getOrElse(ZooKeeperHelper.getTestZooKeeper.root.getOrCreate("scads"))

    logger.info("Opening BDB Environment: " + dir + ", " + config)
    val env = new Environment(dir, config)
    return new StorageHandler(env, zooRoot)
  }
}
