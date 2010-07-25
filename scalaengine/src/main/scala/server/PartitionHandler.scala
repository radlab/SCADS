package edu.berkeley.cs.scads.storage

import com.sleepycat.je.{Database}
import org.apache.log4j.Logger

import edu.berkeley.cs.scads.comm._

class PartitionHandler(db: Database, partitionIdLock: ZooKeeperProxy#ZooKeeperNode, startKey: Option[Array[Byte]], endKey: Option[Array[Byte]], nsRoot: ZooKeeperProxy#ZooKeeperNode) extends ServiceHandler[PartitionServiceOperation] {
  protected val logger = Logger.getLogger("scads.partitionhandler")

  protected def startup(): Unit = null
  protected def shutdown(): Unit = db.close()

  protected def process(src: Option[RemoteActorProxy], msg: PartitionServiceOperation): Unit = src.foreach(_ ! ProcessingException("Not Implemented", ""))
}
