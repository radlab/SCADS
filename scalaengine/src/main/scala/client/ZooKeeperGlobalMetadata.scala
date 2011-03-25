package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import org.apache.avro._ 
import generic._

import org.apache.zookeeper._

trait ZooKeeperGlobalMetadata extends GlobalMetadata with Namespace with KeyRoutable {
  val root: ZooKeeperProxy#ZooKeeperNode
  val name: String

  @volatile protected var nsRoot: ZooKeeperProxy#ZooKeeperNode = _
  logger.debug("ZooKeeperGlobalMetadata Constructor: %s", namespace)

  private def initRoot(node: ZooKeeperProxy#ZooKeeperNode): Unit = {
    node.createChild("partitions", Array.empty, CreateMode.PERSISTENT)
    node.createChild("keySchema", keySchema.toString.getBytes, CreateMode.PERSISTENT)
    node.createChild("valueSchema", valueSchema.toString.getBytes, CreateMode.PERSISTENT)
  }

  // NS must NOT exist or exception is thrown
  onCreate {
    logger.debug("ZooKeeperGlobalMetadata create(): ")
    logger.debug("Creating nsRoot for namespace: " + name)

    val newRoot = root.createChild(name, Array.empty, CreateMode.PERSISTENT)
    initRoot(newRoot)
    nsRoot = newRoot
    nsRoot.createChild("initialized")//TODO: we really aren't done yet!
  }

  // if NS exists, uses that data, otherwise creates a new one
  // TODO: make allow create a parameter
  onOpen { _ =>
    logger.debug("ZooKeeperGlobalMetadata open(): ")
    nsRoot = root.getOrCreate(name)
    
    /* Check to see if the cluster metadata has been created, otherwise create it */
    val isNew = if(!nsRoot.get("initialized").isDefined) {
      try {
	/* Grab a lock so we know we are the only ones creating the namespace */
	val createLock = nsRoot.createChild("createLock", mode=CreateMode.EPHEMERAL)
	create()
	nsRoot.createChild("initialized")
	createLock.delete
	false
      } catch {
	/* Someone else has the create lock, so we should wait until they finish */
	case e: KeeperException if e.code == KeeperException.Code.NODEEXISTS => {
	  nsRoot.awaitChild("initialized")
	  false
	}
      }
    }
    else {
      nsRoot.awaitChild("initialized")
      false
    }

    assert(nsRoot ne null, "nsRoot should not be null after open")
    isNew
  }

  onDelete {
    logger.info("Deleting zookeeper metadata for namespace %s", name)

    assert(nsRoot ne null, "nsRoot is null")
    nsRoot.deleteRecursive
    nsRoot = null
  }

  onClose {
    nsRoot = null
  }

  override lazy val remoteKeySchema: Schema =
    Schema.parse(new String(nsRoot("keySchema").data))
  override lazy val remoteValueSchema: Schema =
    Schema.parse(new String(nsRoot("valueSchema").data))

  override def watchMetadata(key: String, func: () => Unit): Array[Byte] = {
    logger.info("Watching metadata %s for %s", key, namespace) 
    nsRoot(key).onDataChange(func)
  }

  override def getMetadata(key: String): Option[Array[Byte]] = {
    logger.info("Updating metadata %s for namespace %s", key, namespace)
    nsRoot.get(key).map(_.data)
  }

  override def putMetadata(key: String, value: Array[Byte]): Unit = {
    logger.info("Setting metadata %s in namespace %s", key, namespace)
    nsRoot.getOrCreate(key).data = value
  }

  override def deleteMetadata(key: String): Unit =
    nsRoot.deleteChild(key)

  override def waitUntilMetadataPropagated(): Unit =
    nsRoot.waitUntilPropagated()
}
