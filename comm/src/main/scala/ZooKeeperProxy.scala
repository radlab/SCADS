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
  val clientConnections = new scala.collection.mutable.HashMap[String,ZooKeeperProxy]()

  def getConnection(uri:String):ZooKeeperProxy = {
    synchronized {
      clientConnections.getOrElseUpdate(uri,new ZooKeeperProxy(uri))
    }
  }

  def apply(uri: String): ZooKeeperProxy#ZooKeeperNode = uri match {
    case uriRegEx(address, "") => getConnection(address).root
    case uriRegEx(address, path) => getConnection(address).root.getOrCreate(path)
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

  def servers: Seq[String] = address.split(",")

  def newConenction(): ZooKeeper = {
    logger.info("[%s] Opening Zookeeper Connection to %s with timeout %d",Thread.currentThread.getName, address, timeout)
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
    val defaultTimeout = 15*60*1000
    lazy val name: String = path.split("/").last
    lazy val prefix: String = if (path == "/") "/" else path + "/"

    @inline private def fullPath(rpath: String) =
      prefix + rpath

    val proxy = self
    def canonicalAddress = "zk://" + proxy.address + path

    def apply(rpath: String): ZooKeeperNode = 
      get(rpath).getOrElse(throw new RuntimeException("Zookeeper node doesn't exist: " + fullPath(rpath)))

    protected def retry[A](retries: Int)(f: => A): A = {
      @inline def tryAgain(e: Exception) =
	if(retries == 0)
	  throw e
	else {
	  logger.error("Zookeeper connection invalid.  Waiting to try again %s", e)
	  Thread.sleep(1000)
	  retry(retries - 1)(f)
	}

      try f catch {
	case e: org.apache.zookeeper.KeeperException.ConnectionLossException => tryAgain(e)
	case e: org.apache.zookeeper.KeeperException.SessionExpiredException => tryAgain(e)
      }
    } 

    def get(rpath: String, tries: Int = 5): Option[ZooKeeperNode] = retry(tries) {
      rpath.split("/").foldLeft(Option(this))((n,p) => n.flatMap(_.childrenMap.get(p)))
    }
    

    // TODO: let getOrCreate take creation parameters (data/mode)?
    def getOrCreate(rpath: String, tries: Int = 5): ZooKeeperNode = retry(tries) {
      rpath.split("/").foldLeft(this)((n,p) => {
        val fullPath0 = n.fullPath(p)
        try n.childrenMap.get(p).getOrElse(getOrElseUpdateNode(fullPath0, newNode(fullPath0)))
	catch {
	  case e: org.apache.zookeeper.KeeperException.NodeExistsException =>
	    n.childrenMap.get(p).get
	}
      })
    }

    def waitUntilPropagated() {
      Thread.sleep(timeout)
    }

    @inline private implicit def funcToWatcher(f: WatchedEvent => Unit): Watcher = new Watcher {
      def process(evt: WatchedEvent) { f(evt) }
    }

    private def childrenMap =
      getChildrenMap(None) 

    @inline private def getChildrenMap(watcher: Option[WatchedEvent => Unit]): Map[String, ZooKeeperNode] =
      watcher.map(w => conn.getChildren(path, w)).getOrElse(conn.getChildren(path, false)).map(path => {
        val fullPath0 = fullPath(path)
        (path, getOrElseUpdateNode(fullPath0, new ZooKeeperNode(fullPath0)))
      }).toMap

    def children: List[ZooKeeperNode] =
      childrenMap.map(_._2).toList

    def data_=(d: Array[Byte]) {
      conn.setData(path, d, -1)
    }

    def data: Array[Byte] = getData(false)

    def createChild(name: String, data: Array[Byte] = Array.empty, mode: CreateMode = CreateMode.PERSISTENT): ZooKeeperNode =
      newNode(fullPath(name), data, mode)

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

    def awaitChild(name: String, seqNumber: Option[Int] = None, timeout: Long = defaultTimeout, unit: TimeUnit = TimeUnit.MILLISECONDS): ZooKeeperProxy#ZooKeeperNode = {
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

    def onDataChange(func:() => Unit):Array[Byte] = {
      val watcher = new Watcher {
	def process(evt: WatchedEvent) {
	  evt.getType match {
            case EventType.NodeDataChanged => func()
            case e => logger.error("Watch onDataChange is unregistered): %s",e)
          }
	}
      }
      conn.getData(path, watcher, new Stat)
    }

    /** Set a watcher on the children for this node. Note that, according to
     * ZooKeeper semantics, a watcher is executed AT MOST once */
    def watchChildren(f: WatchedEvent => Unit): List[ZooKeeperNode] =
      getChildrenMap(Some(f)).map(_._2).toList

    def registerAndAwait(barrierName: String, count: Int, timeout: Long = defaultTimeout, unit: TimeUnit = TimeUnit.MILLISECONDS): Int = {
      val node = getOrCreate(barrierName)
      val seqNum = node.createChild("client", mode = CreateMode.EPHEMERAL_SEQUENTIAL).sequenceNumber

      require(seqNum < count)
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
        case KeeperState.SyncConnected => logger.info("[%s] Connected to Zookeeper at %s",Thread.currentThread.getName,address)
        case KeeperState.Disconnected => logger.warning("[%s] Connection to Zookeeper at %s Disconnected (%s).",Thread.currentThread.getName,address, event.getState.toString) 
        case KeeperState.Expired => {
          logger.warning("[%s] Connection to Zookeeper at %s Expired (%s).  Attempting to reconnect", Thread.currentThread.getName, address, event.getState.toString)
          try { conn.close() } catch {case e => logger.error("couldn't close zoo connection: %s",e)}
          conn = newConenction()
        }
      }
    else {
      root(event.getPath).watchedEvent(event)
    }
  }
}
