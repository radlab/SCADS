package edu.berkeley.cs
package scads
package storage

import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig
import com.sleepycat.je.jmx.JEMonitor

import net.lag.logging.Logger
import java.io.File

import comm._
import storage._
import avro.marker._
import deploylib.mesos._

/**
 * Application for starting up a standalone scads storage engine
 */
object ScalaEngine extends optional.Application {
  private val logger = Logger()

  def startEngine(clusterAddress: Option[String] = None, dbDir: Option[java.io.File]= None, cachePercentage: Option[Int] = None, verbose: Boolean = false, name: Option[String] = None): ScadsCluster = {
    if (verbose)
      org.apache.log4j.BasicConfigurator.configure()

    val config = new EnvironmentConfig()
    config.setAllowCreate(true)
    config.setTransactional(true)
    config.setCachePercent(cachePercentage.getOrElse(80))

    val dir = dbDir.getOrElse(new java.io.File("db"))
    if (!dir.exists()) {
      dir.mkdir
    }

    val zooRoot = clusterAddress.map(p => ZooKeeperNode(p)).getOrElse(ZooKeeperHelper.getTestZooKeeper.getOrCreate("scads"))

    logger.info("Opening BDB Environment: " + dir + ", " + config)
    val env = new Environment(dir, config)
    val handler = new StorageHandler(env, zooRoot, name)
    new ScadsCluster(zooRoot)
  }

  def main(clusterAddress: Option[String], dbDir: Option[java.io.File], cachePercentage: Option[Int], verbose: Boolean, name: Option[String] = None): Unit = {
    startEngine(clusterAddress,dbDir,cachePercentage,verbose,name)
    //HACK
    while (true)
      Thread.sleep(100000)
  }
}

/**
 * Task for running scads storage engine on mesos
 */
case class ScalaEngineTask(var clusterAddress: String, var dbDir: Option[String] = None, var cachePercentage: Option[Int] = None, var name: Option[String] = None) extends AvroTask with AvroRecord {
  def run(): Unit = {
    val logger = Logger()
    val config = new EnvironmentConfig()
    config.setAllowCreate(true)
    config.setTransactional(true)
    config.setCachePercent(cachePercentage.getOrElse(60))

    val dir = dbDir.map(new File(_)).getOrElse(new File("db"))
    if (!dir.exists()) {
      dir.mkdir
    }

    val zooRoot = ZooKeeperNode(clusterAddress)

    logger.info("Opening BDB Environment: " + dir + ", " + config)
    val env = new Environment(dir, config)
    val handler = new StorageHandler(env, zooRoot, name)

    //HACK
    while (true)
      Thread.sleep(100000)
  }
}

