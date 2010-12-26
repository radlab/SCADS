package edu.berkeley.cs.scads.storage.newclient

import edu.berkeley.cs.scads.comm._

import org.apache.avro._ 
import generic._

import org.apache.zookeeper._

trait ZooKeeperGlobalMetadata extends GlobalMetadata with Namespace with KeyRoutable {
  val root: ZooKeeperProxy#ZooKeeperNode
  val name: String

  @volatile private var nsRoot: ZooKeeperProxy#ZooKeeperNode = _

  onCreate {
    logger.debug("Creating nsRoot for namespace: " + name)
    nsRoot = root.createChild(name, Array.empty, CreateMode.PERSISTENT)
    nsRoot.createChild("keySchema", keySchema.toString.getBytes, CreateMode.PERSISTENT)
    nsRoot.createChild("valueSchema", valueSchema.toString.getBytes, CreateMode.PERSISTENT)
  }

  onOpen {
    // TODO
  }

  onDelete {
    logger.info("Deleting zookeeper metadata for namespace %s", name)
    nsRoot.deleteRecursive
    nsRoot = null
  }

  onClose {
    // TODO
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
