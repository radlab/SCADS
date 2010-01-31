package edu.berkeley.cs.scads.storage

import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig
import com.sleepycat.je.jmx.JEMonitor

object ScalaEngine extends optional.Application {
	def main(port: Int, zooKeeper: Option[String], dbDir: Option[java.io.File], cachePercentage: Option[Int], verbose: Boolean): StorageHandler = {
		val config = new EnvironmentConfig()
		config.setAllowCreate(true)
		config.setTransactional(true)
		config.setCachePercent(cachePercentage.getOrElse(80))

		val env = new Environment(dbDir.getOrElse(new java.io.File("db")), config)
		val handler = new StorageHandler(env)
		handler.startListener(port)

		return handler
	}
}
