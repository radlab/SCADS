package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.thrift._

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
		val dbeNamespace = new DatabaseEntry()
		val dbePolicy = new DatabaseEntry()
		val cur = respPolicyDb.openCursor(null, null)

		var status = cur.getFirst(dbeNamespace, dbePolicy, null)
		while(status == OperationStatus.SUCCESS) {
			val ns = new String(dbeNamespace.getData)
			logger.info("Registering namespace: " + ns + ", policy" + RangedPolicy(dbePolicy.getData))

			if(zoo.exists("/scads/namespaces/" + ns, false) == null)
				zoo.create("/scads/namespaces/" + ns, "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
			zoo.create("/scads/namespaces/" + ns + "/" + hostid, dbePolicy.getData, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
			status = cur.getNext(dbeNamespace, dbePolicy, null)
		}
		cur.close()
	}

	def process(event: WatchedEvent): Unit = {
		logger.info(event)
	}
}
