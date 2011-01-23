package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import org.apache.avro._ 
import generic._

import org.apache.zookeeper._

trait ZooKeeperGlobalMetadata extends GlobalMetadata with Namespace with KeyRoutable {
  val root: ZooKeeperProxy#ZooKeeperNode
  val name: String

  @volatile private var nsRoot: ZooKeeperProxy#ZooKeeperNode = _

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
  }

  // if NS exists, uses that data, otherwise creates a new one
  // false ret indicates a NS previously existed, true indicates it did not
  onOpen { _ =>
    logger.debug("ZooKeeperGlobalMetadata open(): ")

    // TODO: this is definitely racy, but it's no worse than what we had
    // before
    root.get(name).map(_ => false).getOrElse {
      try {
        val newRoot = root.createChild(name, Array.empty, CreateMode.PERSISTENT)
        initRoot(newRoot)
        newRoot
        true
      } catch {
        case e: KeeperException if e.code == KeeperException.Code.NODEEXISTS => 
          root(name)
          false
        case e => throw e
      }
    }
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

  override def getMetadata(key: String): Option[Array[Byte]] = 
    nsRoot.get(key).map(_.data)

  override def putMetadata(key: String, value: Array[Byte]): Unit =
    nsRoot.getOrCreate(key).data = value

  override def deleteMetadata(key: String): Unit =
    nsRoot.deleteChild(key)

  override def waitUntilMetadataPropagated(): Unit =
    nsRoot.waitUntilPropagated()
}
