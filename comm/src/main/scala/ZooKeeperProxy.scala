package edu.berkeley.cs.scads.comm

import org.apache.zookeeper.{ZooKeeper, Watcher, WatchedEvent, CreateMode, ZooDefs}
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.data.Stat
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import org.apache.log4j.Logger

object RClusterZoo extends ZooKeeperProxy("r2:2181")

/**
 * Scalafied interface to Zookeeper
 * TODO: Add the ability to execute callbacks on watches (possibly with weak references to callbacks)
 * TODO: Add actor handle serializing and deserializing with linking to the created ephemeral node.
 * TODO: Recreate ephemeral nodes on reconnect.
 */
class ZooKeeperProxy(val address: String) extends Watcher {
  val logger = Logger.getLogger("scads.zookeeper")
  var conn = new ZooKeeper(address, 10000, this)
  val root = new ZooKeeperNode("/")

  class ZooKeeperNode(val path: String) {
    var childrenCache: Option[HashMap[String, ZooKeeperNode]] = None
    var dataCache: Option[Array[Byte]] = None
    var statCache: Option[Stat] = None

    def name: String = path.split("/").last

    def apply(rpath: String): ZooKeeperNode = rpath.split("/").foldLeft(this)((n,p) => n.updateChildren(false).apply(p))

    def get(rpath: String): Option[ZooKeeperNode] = rpath.split("/").foldLeft(Option(this))((n,p) => n.flatMap(_.updateChildren(false).get(p)))

    def prefix: String = if(path equals "/") "/" else path + "/"

    def children: List[ZooKeeperNode] = updateChildren(false).map(_._2).toList
    def cachedChildren: Iterable[ZooKeeperNode] = childrenCache.getOrElse(updateChildren(false)).map(_._2)

    def data: Array[Byte] = updateData(false)
    def cachedData: Array[Byte] = dataCache.getOrElse(updateData())

		def data_=(d: Array[Byte]): Unit = {
			conn.setData(path, d, if(statCache.isDefined) statCache.get.getVersion else -1)
		}

    def getOrCreate(rpath: String): ZooKeeperNode = {
      rpath.split("/").foldLeft(this)((n,p) => n.updateChildren(false).get(p) match {
        case Some(c) => c
        case None => n.createChild(p, "".getBytes, CreateMode.PERSISTENT)
      })
    }

    def createChild(name: String, data: Array[Byte], mode: CreateMode): ZooKeeperNode = {
      conn.create(prefix + name, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode)
      updateChildren(false).apply(name)
    }

		def deleteChild(name:String):ZooKeeperNode = {
			conn.delete(prefix + name,-1)
			updateChildren(false)
			this
		}

    protected def updateChildren(watch: Boolean = false):HashMap[String, ZooKeeperNode] = {
      if(!childrenCache.isDefined)
        childrenCache = Some(new HashMap[String, ZooKeeperNode]())

      val c = conn.getChildren(path, watch)
      childrenCache.get.filter( t => {
        val p = t._1
        val n = t._2
        if(!c.contains(p)) {
          n.watchedEvent(EventType.NodeDeleted)
          false
        }
        else {
          true
        }
      })

      c --= childrenCache.get.keysIterator.toList
      c.foreach(k => {
        childrenCache.get += ((k, new ZooKeeperNode(prefix + k)))
      })
      childrenCache.get
    }

    protected def updateData(watch: Boolean = false): Array[Byte] = {
      val stat = new Stat
      val data = conn.getData(path, watch, stat)

      dataCache = Some(data)
      statCache = Some(stat)
      data
    }

    protected[ZooKeeperProxy] def watchedEvent(etype: EventType):Unit = etype match {
      case EventType.NodeChildrenChanged => updateChildren(true)
      case EventType.NodeCreated =>
      case EventType.NodeDataChanged => updateData(true)
      case EventType.NodeDeleted =>
    }

    override def toString(): String =
      "<znode path:" + path + ", data: '" + new String(data) + "', children: " + cachedChildren.map(_.name) + ">"
  }

  def process(event: WatchedEvent): Unit = {
    if(event.getType == EventType.None)
      event.getState match {
        case KeeperState.SyncConnected =>
        case KeeperState.Expired => {
          logger.info("Connection to Zookeeper Expired.  Attempting to reconnect")
          conn = new ZooKeeper(address, 3000, this)
        }
      }
    else {
      root(event.getPath).watchedEvent(event.getType)
    }
  }
}
