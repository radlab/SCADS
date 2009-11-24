package edu.berkeley.cs.scads.storage

import org.apache.zookeeper.{ZooKeeper, Watcher, WatchedEvent, CreateMode}
import org.apache.zookeeper.ZooDefs.Ids
import com.sleepycat.je.{Database, DatabaseConfig, DatabaseEntry, Environment, LockMode, OperationStatus}

class ZooKeptStorageProcessor(env: Environment, hostid: String, servers: String) extends StorageProcessor(env) with Watcher {
	val zoo = new ZooKeeper(servers, 3000, this)
	logger.info("Registering with zookeeper")

	if(zoo.exists("/scads", false) == null)
		zoo.create("/scads", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
	if(zoo.exists("/scads/servers", false) == null)
		zoo.create("/scads/servers", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
	if(zoo.exists("/scads/namespaces", false) == null)
		zoo.create("/scads/namespaces", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

	zoo.create("/scads/servers/" + hostid, "ScalaEngine".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

	def process(event: WatchedEvent): Unit = {
		logger.info(event)
	}
}
