package edu.berkeley.cs.scads.comm

import org.apache.zookeeper.{ZooKeeper, Watcher, WatchedEvent, CreateMode, ZooDefs}
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.data.Stat
import scala.collection.mutable.HashMap
import scala.collection.jcl.Conversions._



class ZooKeeperProxy(server: String) extends Watcher {
  val conn = new ZooKeeper(server, 3000, this)
  val root = new ZooKeeperNode("/")
	
  class ZooKeeperNode(val path: String) {
    var childrenCache: Option[HashMap[String, ZooKeeperNode]] = None
    var dataCache: Option[Array[Byte]] = None
    var statCache: Option[Stat] = None

    def apply(rpath: String): ZooKeeperNode = get(rpath)

    def prefix: String = if(path equals "/") "/" else path + "/"

    def children: HashMap[String, ZooKeeperNode] = childrenCache match {
      case Some(c) => c
      case None => updateChildren(false)
    }

    def data: Array[Byte] = dataCache match {
      case Some(d) => d
      case None => updateData(false)
    }

		def data_=(d: Array[Byte]): Unit = {
			conn.setData(path, d, if(statCache.isDefined) statCache.get.getVersion else -1)
		}

    def get(rpath: String): ZooKeeperNode = rpath.split("/").foldLeft(this)((n,p) => n.children(p))
    def getOrCreate(rpath: String): ZooKeeperNode = {
      rpath.split("/").foldLeft(this)((n,p) => n.children.get(p) match {
        case Some(c) => c
        case None => n.createChild(p, "".getBytes, CreateMode.PERSISTENT)
      })
    }

    def createChild(name: String, data: Array[Byte], mode: CreateMode): ZooKeeperNode = {
      conn.create(prefix + name, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode)
      updateChildren(false)
      children(name)
    }

    def updateChildren(watch: Boolean):HashMap[String, ZooKeeperNode] = {
      if(!childrenCache.isDefined)
        childrenCache = Some(new HashMap[String, ZooKeeperNode]())

      val c = conn.getChildren(path, watch)
      children.retain((p, n) => {
        if(!c.contains(p)) {
          n.watchedEvent(EventType.NodeDeleted)
          false
        }
        else {
          true
        }
      })

      (c -- children.keys.toList).foreach(k => {
        children += ((k, new ZooKeeperNode(prefix + k)))
      })
      children
    }

    def updateData(watch: Boolean): Array[Byte] = {
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
      "<znode path:" + path + ", data: '" + new String(data) + "', children: " + children.keys.toList + ">"
  }

  def process(event: WatchedEvent): Unit = {
    if(event.getType == EventType.None)
      event.getState match {
        case KeeperState.SyncConnected =>
        case KeeperState.Expired =>
      }
    else {
      root(event.getPath).watchedEvent(event.getType)
    }
  }
}