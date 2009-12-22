package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.thrift._

import org.apache.zookeeper.{ZooKeeper, Watcher, WatchedEvent, CreateMode}
import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.ZooDefs.Ids
import com.sleepycat.je.{Database, DatabaseConfig, DatabaseEntry, Environment, LockMode, OperationStatus}

class ZooKeptStorageProcessor(env: Environment, hostid: String, servers: String, deferedWrite: Boolean) extends StorageProcessor(env, deferedWrite) with Watcher {
	var zoo = new ZooKeeper(servers, 30000, this)
	logger.info("Registering with zookeeper")

	/* Set up basic directory */
	try {
		if(zoo.exists("/scads", false) == null)
			zoo.create("/scads", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
		if(zoo.exists("/scads/servers", false) == null)
			zoo.create("/scads/servers", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
		if(zoo.exists("/scads/namespaces", false) == null)
			zoo.create("/scads/namespaces", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
	}
	catch {
		case e: org.apache.zookeeper.KeeperException.NodeExistsException => logger.warn("Race condition while create directories, giving up")
	}

	registerNamespaces()

	override def set_responsibility_policy(ns : String, policy: java.util.List[RecordSet]): Boolean = {
		val rangedPolicy = new RangedPolicy(RangedPolicy.convert(policy).toArray)
		val bytes = rangedPolicy.getBytes

		respPolicyDb.put(null, new DatabaseEntry(ns.getBytes), new DatabaseEntry(bytes))
		dbCache.put(ns, new CachedDb(getDatabase(ns).handle, rangedPolicy))

		if(zoo.exists("/scads/namespaces/" + ns, false) == null)
				zoo.create("/scads/namespaces/" + ns, "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
		val entry = "/scads/namespaces/" + ns + "/" + hostid
		val stat = zoo.exists(entry, false)
		if(stat == null)
			zoo.create(entry, bytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
		else
			zoo.setData(entry, bytes, stat.getVersion())

 		true
	}

	private def registerNamespaces(): Unit = {
		/* Register server entry */
		createOrReplaceEphemeralFile("/scads/servers/" + hostid, "ScalaEngine".getBytes)

		/* Register node for each namespace */
		val dbeNamespace = new DatabaseEntry()
		val dbePolicy = new DatabaseEntry()
		val cur = respPolicyDb.openCursor(null, null)

		var status = cur.getFirst(dbeNamespace, dbePolicy, null)
		while(status == OperationStatus.SUCCESS) {
			val ns = new String(dbeNamespace.getData)
			logger.info("Registering namespace: " + ns + ", policy" + RangedPolicy(dbePolicy.getData))

			if(zoo.exists("/scads/namespaces/" + ns, false) == null)
				zoo.create("/scads/namespaces/" + ns, "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

			createOrReplaceEphemeralFile("/scads/namespaces/" + ns + "/" + hostid, dbePolicy.getData)
			status = cur.getNext(dbeNamespace, dbePolicy, null)
		}
		cur.close()
	}

	private def createOrReplaceEphemeralFile(file: String, data: Array[Byte]): Unit = {
		try {
			logger.debug("Creating entry: " + file)
			zoo.create(file, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
		}
		catch {
			case e: org.apache.zookeeper.KeeperException.NodeExistsException => {
				logger.warn("Stale entry for '" + file + "' detected, deleting and retrying")
				try {
					zoo.delete(file, -1)
				}
				catch {
					case e: org.apache.zookeeper.KeeperException.NoNodeException => logger.warn("Stale entry disapeared. Oh well")
				}
				zoo.create(file, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
			}
		}
	}

	def process(event: WatchedEvent): Unit = {
		event.getType match {
			case Event.EventType.None => {
				event.getState match {
					case Event.KeeperState.Expired => {
						logger.fatal("Lost connection to zookeeper, attempting to reconnect")
						zoo = new ZooKeeper(servers, 30000, this)
						registerNamespaces()
					}
					case unhandledState => logger.info("Zookeeper state change: " + event)
				}
			}
			case unhandleEventType => logger.info("Zookeeper event: " + event)
		}
	}
}
