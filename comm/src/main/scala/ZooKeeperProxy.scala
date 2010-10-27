package edu.berkeley.cs.scads.comm

import org.apache.zookeeper.{ZooKeeper, Watcher, WatchedEvent, CreateMode, ZooDefs}
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.data.Stat
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import net.lag.logging.Logger

import java.util.concurrent.{ ConcurrentHashMap, TimeUnit }

object RClusterZoo extends ZooKeeperProxy((1 to 3).map(i => "zoo%d.millennium.berkeley.edu".format(i)).mkString(","))

object ZooKeeperNode {
  val uriRegEx = """zk://([^/]*)/(.*)""".r
  def apply(uri: String): ZooKeeperProxy#ZooKeeperNode = uri match {
    case uriRegEx(address, "") => new ZooKeeperProxy(address).root
    case uriRegEx(address, path) => new ZooKeeperProxy(address).root.getOrCreate(path)
    case _ => throw new RuntimeException("Invalid ZooKeeper URI: " + uri)
  }
}

/**
 * Scalafied interface to Zookeeper
 * TODO: Add the ability to execute callbacks on watches (possibly with weak references to callbacks)
 * TODO: Add actor handle serializing and deserializing with linking to the created ephemeral node.
 * TODO: Recreate ephemeral nodes on reconnect.
 *
 * Instances of ZooKeeperProxy and ZooKeeperNode are thread-safe 
 */
class ZooKeeperProxy(val address: String, val timeout: Int = 30000) extends Watcher {
  self =>
  protected val logger = Logger()

  // must be volatile because it's set from watcher thread
  @volatile protected var conn = newConenction()

  def newConenction(): ZooKeeper = {
    logger.info("Opening Zookeeper Connection to %s with timeout %d", address, timeout)
    new ZooKeeper(address, timeout, this)
  }

  /** 
   * maintains a canonical mapping of (full) zookeeper path to a zookeeper
   * node. 
   * TODO: remove the canonical map
   */
  private final val canonicalMap = new ConcurrentHashMap[String, ZooKeeperNode]

  val root = getOrElseUpdateNode("/", new ZooKeeperNode("/"))

  def close() = conn.close()

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
          val test2 = canonicalMap.put(newNode0.path, newNode0)
          //assert(test2 eq null, "newNode should not produce name collisions: alreadyPresent %s, newNode %s".format(test2.path, newNode0.path))
          //logger.info("newNode was placed into canonicalMap: %s", newNode0.path)
          if (test2 ne null)
            logger.warning("replacing stale entry (most likely an ephemeral node which was deleted from another client): %s", newNode0.path)
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

    val proxy = self
    def canonicalAddress = "zk://" + proxy.address + path

    def apply(rpath: String): ZooKeeperNode = 
      get(rpath).getOrElse(throw new RuntimeException("Zookeeper node doesn't exist: " + fullPath(rpath)))

    def get(rpath: String): Option[ZooKeeperNode] =
      rpath.split("/").foldLeft(Option(this))((n,p) => n.flatMap(_.childrenMap.get(p)))

    // TODO: let getOrCreate take creation parameters (data/mode)?
    def getOrCreate(rpath: String): ZooKeeperNode =
      rpath.split("/").foldLeft(this)((n,p) => {
        val fullPath0 = n.fullPath(p)
        n.childrenMap.get(p).getOrElse(
          try getOrElseUpdateNode(fullPath0, newNode(fullPath0)) catch {
            case e: org.apache.zookeeper.KeeperException.NodeExistsException => n.childrenMap.get(p).get
          }
        )
      })

    def waitUntilPropagated() {
      Thread.sleep(timeout)
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

    @inline private def safeDelete(path: String): Boolean = {
      try {
        conn.delete(path, -1)
        true
      } catch {
        case _: org.apache.zookeeper.KeeperException.NoNodeException => false 
      }
    }

    def deleteChild(name: String) {
      if (!safeDelete(fullPath(name))) 
        logger.warning("deleteChild() - Child node %s was already deleted", name)
      if (canonicalMap.remove(fullPath(name)) eq null)
        logger.warning("deleteChild() - No canonical node existed previously for fullPath: %s", fullPath(name))
      else
        logger.info("deleteChild() - successfully deleted child: %s", fullPath(name))
    }

    def delete() {
      if (!safeDelete(path))
        logger.warning("delete() - Node %s was already deleted", path)
      if (canonicalMap.remove(path) eq null)
        logger.warning("delete() - No canonical node existed previously for path: %s", path)
      else
        logger.info("delete() - successfully deleted path: %s", path)
    }

    def deleteRecursive() {
      children.foreach(_.deleteRecursive)
      delete
    }

    def sequenceNumber: Int =
      name.drop(name.length - 10).toInt

    def awaitChild(name: String, seqNumber: Option[Int] = None, timeout: Long = 24*60*60*1000, unit: TimeUnit = TimeUnit.MILLISECONDS): ZooKeeperProxy#ZooKeeperNode = {
      val fullName = seqNumber.map(s => "%s%010d".format(name, s)).getOrElse(name)
      val childPath = fullPath(fullName)
      val blocker = new BlockingFuture[Unit] 
      val watcher = new Watcher {
        def process(evt: WatchedEvent) {
          evt.getType match {
            case EventType.NodeCreated => blocker.finish()
            case e => 
              blocker.finishWithError(new RuntimeException("Expected NodeCreated event for node %s, but got %s event instead".format(fullName, e)))
          }
        }
      }
      if (conn.exists(childPath, watcher) eq null) {
        logger.info("Waiting up to %dms for %s", timeout, childPath)
        blocker.await(unit.toMillis(timeout))
      }

      apply(fullName)
    }

    def registerAndAwait(barrierName: String, count: Int): Int = {
      val node = getOrCreate(barrierName)
      val seqNum = node.createChild("client", mode = CreateMode.EPHEMERAL_SEQUENTIAL).sequenceNumber
      node.awaitChild("client", Some(count - 1))
      seqNum
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
        case KeeperState.Expired | KeeperState.Disconnected => {
          logger.warning("Connection to Zookeeper at %s Expired.  Attempting to reconnect", address)
          conn = newConenction()
        }
      }
    else {
      root(event.getPath).watchedEvent(event)
    }
  }
}
