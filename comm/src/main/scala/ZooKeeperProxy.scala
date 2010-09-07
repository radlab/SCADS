package edu.berkeley.cs.scads.comm

import org.apache.zookeeper.{ZooKeeper, Watcher, WatchedEvent, CreateMode, ZooDefs}
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.data.Stat
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import org.apache.log4j.Logger

import java.util.concurrent.ConcurrentHashMap

object RClusterZoo extends ZooKeeperProxy("r2:2181")

/**
 * Scalafied interface to Zookeeper
 * TODO: Add the ability to execute callbacks on watches (possibly with weak references to callbacks)
 * TODO: Add actor handle serializing and deserializing with linking to the created ephemeral node.
 * TODO: Recreate ephemeral nodes on reconnect.
 *
 * Instances of ZooKeeperProxy and ZooKeeperNode are thread-safe 
 */
class ZooKeeperProxy(val address: String) extends Watcher {
 
  protected val logger = Logger.getLogger("scads.zookeeper")

  // must be volatile because it's set from watcher thread
  @volatile protected var conn = new ZooKeeper(address, 10000, this)

  /** 
   * maintains a canonical mapping of (full) zookeeper path to a zookeeper
   * node. 
   */
  private final val canonicalMap = new ConcurrentHashMap[String, ZooKeeperNode]

  val propagationTime = 100 //TODO Michael should read the real value from the config
  val root = getOrElseUpdateNode("/", new ZooKeeperNode("/"))

  def close() = conn.close()

  @inline private def isSequential(mode: CreateMode) = 
    mode == CreateMode.EPHEMERAL_SEQUENTIAL || mode == CreateMode.PERSISTENT_SEQUENTIAL

  @inline private def getNode(fullPath: String) =
    Option(canonicalMap.get(fullPath)) 

  private def getOrElseUpdateNode(fullPath: String, newNode: => ZooKeeperNode) = {
    val test0 = canonicalMap.get(fullPath)
    if (test0 ne null) test0
    else {
      // TODO: don't grab lock, but use futures
      synchronized {
        val test1 = canonicalMap.get(fullPath) // check again
        if (test1 ne null) test1
        else {
          val newNode0 = newNode // only can evaluate newNode once
          val test2 = canonicalMap.putIfAbsent(newNode0.path, newNode0)
          assert(test2 eq null, "newNode should not produce name collisions")
          newNode0
        }
      }
    }
  }

  @inline private def newNode(fullPath: String, data: Array[Byte] = Array.empty, mode: CreateMode = CreateMode.PERSISTENT) =
    new ZooKeeperNode(conn.create(fullPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode))

  class ZooKeeperNode(val path: String) {
    require(path.startsWith("/"), "Path must start with a slash (/)")

    lazy val name: String = path.split("/").last
    lazy val prefix: String = if (path == "/") "/" else path + "/"

    @inline private def fullPath(rpath: String) =
      prefix + rpath

    def apply(rpath: String): ZooKeeperNode = 
      get(rpath).getOrElse(throw new RuntimeException("Zookeeper node doesn't exist: " + fullPath(rpath)))

    def get(rpath: String): Option[ZooKeeperNode] =
      rpath.split("/").foldLeft(Option(this))((n,p) => n.flatMap(_.childrenMap.get(p)))

    // TODO: let getOrCreate take creation parameters (data/mode)?
    def getOrCreate(rpath: String): ZooKeeperNode =
      rpath.split("/").foldLeft(this)((n,p) => {
        val fullPath0 = n.fullPath(p)
        n.childrenMap.get(p).getOrElse(getOrElseUpdateNode(fullPath0, newNode(fullPath0)))
      })

    def waitUntilPropagated() {
      Thread.sleep(propagationTime)
    }

    @inline private def childrenMap: Map[String, ZooKeeperNode] =
      conn.getChildren(path, false).map(path => {
        val fullPath0 = fullPath(path)
        (path, getOrElseUpdateNode(fullPath0, new ZooKeeperNode(fullPath0)))
      }).toMap

    def children: List[ZooKeeperNode] =
      childrenMap.map(_._2).toList
    @deprecated("to be removed - use children instead")
    def cachedChildren: Iterable[ZooKeeperNode] = children

    def data: Array[Byte] = getData(false)
    @deprecated("to be removed - use data instead")
    def cachedData: Array[Byte] = data 

    def data_=(d: Array[Byte]) {
      conn.setData(path, d, -1)
    }

    def createChild(name: String, data: Array[Byte] = Array.empty, mode: CreateMode = CreateMode.PERSISTENT): ZooKeeperNode =
      getOrElseUpdateNode(fullPath(name), newNode(fullPath(name), data, mode))

    def deleteChild(name:String) {
      conn.delete(fullPath(name), -1)
      canonicalMap.remove(fullPath(name))
    }

    def delete() {
      conn.delete(path, -1)
      canonicalMap.remove(path)
    }

    protected def getData(watch: Boolean = false): Array[Byte] = {
      val stat = new Stat
      conn.getData(path, watch, stat)
    }

    protected[ZooKeeperProxy] def watchedEvent(evt: WatchedEvent): Unit = evt.getType match {
      case EventType.NodeChildrenChanged => /* ignore - let NodeDeleted handle this */
      case EventType.NodeCreated => /* ignore */
      case EventType.NodeDataChanged => /* ignore */
      case EventType.NodeDeleted => canonicalMap.remove(evt.getPath)
    }

    /** 2 nodes are equal iff their paths are equal */
    override def equals(that: Any) =
      if (this eq that.asInstanceOf[AnyRef]) true
      else if (that == null) false
      else that match {
        case that0: ZooKeeperNode =>
          this.path == that0.path
        case _ => false
      }

    override def hashCode = 
      path.hashCode

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
          // TODO: clear cache here?
        }
      }
    else {
      root(event.getPath).watchedEvent(event)
    }
  }
}
