package edu.berkeley.cs
package scads
package storage

import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig
import com.sleepycat.je.jmx.JEMonitor

import net.lag.logging.Logger
import java.io.RandomAccessFile
import java.io._
import java.nio.channels.FileChannel.MapMode._

import comm._
import storage._
import avro.marker._
import deploylib.mesos._

/**
 * Application for starting up a standalone scads storage engine
 */
object ScalaEngine extends optional.Application {
  private val logger = Logger()

  def main(clusterAddress: Option[String], dbDir: Option[java.io.File], cachePercentage: Option[Int], verbose: Boolean, name: Option[String] = None): Unit = {
    if(verbose)
      org.apache.log4j.BasicConfigurator.configure()

    val config = new EnvironmentConfig()
    config.setAllowCreate(true)
    config.setTransactional(true)
//    config.setTxnNoSync(true)
//    config.setTxnWriteNoSync(true)
    config.setCachePercent(cachePercentage.getOrElse(80))

    val dir = dbDir.getOrElse(new java.io.File("db"))
    if(!dir.exists()) {
      dir.mkdir
    }

    val zooRoot = clusterAddress.map(p => ZooKeeperNode(p)).getOrElse(ZooKeeperHelper.getTestZooKeeper.getOrCreate("scads"))

    logger.info("Opening BDB Environment: " + dir + ", " + config)
    val env = new Environment(dir, config)
    val handler = new StorageHandler(env, zooRoot,name)

    //HACK
    while(true)
      Thread.sleep(100000)
  }
}

/**
 * Task for running scads storage engine on mesos
 */
case class ScalaEngineTask(var clusterAddress: String, var dbDir: Option[String] = None, var cachePercentage: Option[Int] = None, var name: Option[String] = None, var preallocSize: Long = 0) extends AvroTask with AvroRecord {

  /**
   * It's a good idea to exercise the storage on EC2 before writing
   * large amounts of data to avoid latency spikes.
   */
  def prealloc(dir: File, bytes: Long) {
    val chunksize = 1 << 28 /* 256 MiB chunks to avoid map size limit */
    val nChunks = (bytes / chunksize).intValue
    val chunks = (0 until nChunks).map(c => new File(dir, "zero_chunk_" + c))
    for (chunk <- chunks) {
      logger.info("Writing zeros to " + chunk.getName)
      var f = new RandomAccessFile(chunk, "rw");
      f.setLength(chunksize);
      val buffer = f.getChannel.map(READ_WRITE, 0, chunksize)
      for (i <- 0 until chunksize by 1024) {
        buffer.put(i, 0);
      }
      f.close()
    }
    for (chunk <- chunks) {
      logger.info("Deleting " + chunk.getName)
      chunk.delete();
    }
  }

  def run(): Unit = {
    val logger = Logger()
    val config = new EnvironmentConfig()
    config.setAllowCreate(true)
    config.setTransactional(true)
    config.setCachePercent(cachePercentage.getOrElse(60))

    val dir = dbDir.map(new File(_)).getOrElse(new File("db"))
    logger.info("dbdir: " + dir)
    if(!dir.exists()) {
      dir.mkdir
    }

    if (preallocSize > 0) {
      logger.info("Preallocating bytes in block of size " + preallocSize)
      prealloc(dir, preallocSize)
    }

    val zooRoot = ZooKeeperNode(clusterAddress)

    logger.info("Opening BDB Environment: " + dir + ", " + config)
    val env = new Environment(dir, config)
    val handler = new StorageHandler(env, zooRoot,name)

    //HACK
    while(true)
      Thread.sleep(100000)
  }
}

