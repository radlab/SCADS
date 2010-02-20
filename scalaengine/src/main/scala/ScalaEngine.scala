package edu.berkeley.cs.scads.storage

import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig
import com.sleepycat.je.jmx.JEMonitor

import edu.berkeley.cs.scads.comm._

object ScalaEngine extends optional.Application {
	def main(port: Int, zooKeeper: String, dbDir: Option[java.io.File], cachePercentage: Option[Int], verbose: Boolean): StorageHandler = {
		val config = new EnvironmentConfig()
		config.setAllowCreate(true)
		config.setTransactional(true)
		config.setCachePercent(cachePercentage.getOrElse(80))

		val dir = dbDir.getOrElse(new java.io.File("db"))
		if(!dir.exists()) {
			dir.mkdir
		}

		val env = new Environment(dir, config)
		val zooRoot = new ZooKeeperProxy(zooKeeper).root("scads")
		val handler = new StorageHandler(env, zooRoot)
    MessageHandler.registerService("Storage",handler)
		MessageHandler.startListener(port)
		return handler
	}
}
