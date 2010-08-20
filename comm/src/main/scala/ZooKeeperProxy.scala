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
 *
 * Warning: Returned instances of ZooKeeperNode are not thread-safe
 */
class ZooKeeperProxy(val address: String) extends Watcher {
  protected val logger = Logger.getLogger("scads.zookeeper")
  protected var conn = new ZooKeeper(address, 10000, this)

  val propagationTime = 100 //TODO Michael should read the real value from the config
  val root = new ZooKeeperNode("/")

  def close() = conn.close()

  class ZooKeeperNode(val path: String) {
    protected val childrenCache = new HashMap[String, ZooKeeperNode]
    protected var dataCache: Option[Array[Byte]] = None
    protected var statCache: Option[Stat] = None

    def name: String = path.split("/").last

    def apply(rpath: String): ZooKeeperNode = get(rpath).getOrElse(throw new RuntimeException("Zookeeper node doesn't exist: " + prefix + rpath))

    def get(rpath: String): Option[ZooKeeperNode] = {
      val stat = conn.exists(prefix + rpath, false)
      if(stat == null)
        None
      else {
        Some(rpath.split("/").foldLeft(this)((n,p) => n.childrenCache.getOrElseUpdate(p, new ZooKeeperNode(n.prefix + p))))
      }
    }

    def waitUntilPropagated() : Unit = {
      wait(propagationTime)
    }

    def prefix: String = if(path equals "/") "/" else path + "/"

    def children: List[ZooKeeperNode] = updateChildren(false).map(_._2).toList
    def cachedChildren: Iterable[ZooKeeperNode] = childrenCache.map(_._2)

    def data: Array[Byte] = updateData(false)
    def cachedData: Array[Byte] = dataCache.getOrElse(updateData())

		def data_=(d: Array[Byte]): Unit = {
			conn.setData(path, d, statCache.map(_.getVersion).getOrElse(-1))
		}

    def getOrCreate(rpath: String): ZooKeeperNode = {
      rpath.split("/").foldLeft(this)((n,p) => n.updateChildren(false).get(p) match {
        case Some(c) => c
        case None => n.createChild(p, "".getBytes, CreateMode.PERSISTENT)
      })
    }

    def createChild(name: String, data: Array[Byte] = Array[Byte](), mode: CreateMode = CreateMode.PERSISTENT): ZooKeeperNode = {
      val newNode = new ZooKeeperNode(conn.create(prefix + name, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode))
      return newNode
    }

		def deleteChild(name:String):Unit = {
			conn.delete(prefix + name,-1)
		}

    def delete(): Unit = {
      conn.delete(path, -1)
    }

    protected def updateChildren(watch: Boolean = false): HashMap[String, ZooKeeperNode] = {
      val children = Set(conn.getChildren(path, watch) : _*)
      // keep the children already in the cache
      childrenCache.retain { case (child, _) => children.contains(child) }
      // add the ones that aren't there
      children filter { child => !childrenCache.contains(child) } foreach { child =>
        childrenCache += ((child, new ZooKeeperNode(prefix + child))) 
      }
      childrenCache
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
      "<znode path:" + path + ", data: '" + new String(data) + "', children: " + children.map(_.name) + ">"
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
