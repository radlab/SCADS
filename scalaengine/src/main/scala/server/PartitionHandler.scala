package edu.berkeley.cs.scads.storage

import com.sleepycat.je.{Database}
import edu.berkeley.cs.scads.comm._

class PartitionHandler(db: Database, partitionIdLock: ZooKeeperProxy#ZooKeeperNode, startKey: Option[Array[Byte]], endKey: Option[Array[Byte]], nsRoot: ZooKeeperProxy#ZooKeeperNode) extends ServiceHandler {
  implicit val remoteHandle = MessageHandler.registerService(this).toPartitionService

  def receiveMessage(src: Option[RemoteActor], msg: MessageBody): Unit = src.foreach(_ ! ProcessingException("Not Implemented", ""))
}
