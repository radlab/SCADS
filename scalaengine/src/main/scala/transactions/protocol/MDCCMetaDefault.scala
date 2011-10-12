package edu.berkeley.cs.scads.storage.transactions.mdcc

import _root_.edu.berkeley.cs.scads.comm._
import _root_.edu.berkeley.cs.scads.storage.PartitionHandler
import _root_.edu.berkeley.cs.scads.storage.transactions.conflict.PendingUpdates
import _root_.edu.berkeley.cs.scads.storage.transactions.MDCCMetaHelper
import net.lag.logging.Logger
import org.apache.zookeeper._
import collection.mutable.HashMap
import java.util.concurrent.ConcurrentHashMap

class MDCCMetaDefault(nsRoot: ZooKeeperProxy#ZooKeeperNode) {
  import MDCCMetaDefault._

  protected lazy val logger = Logger()

  var defaultMeta : MDCCMetadata = null
  loadDefault()

  def loadDefault() : MDCCMetadata = {
    val defaultNode =
      nsRoot.get(MDCC_DEFAULT_META, tries=1) match{
        case None => {
          nsRoot.awaitChild(MDCC_DEFAULT_META, timeout=5 * 1000)
          val tmp = nsRoot.get(MDCC_DEFAULT_META)
          assert(tmp.isDefined)
          tmp.get
        }
        case Some(x) => x
      }
    defaultMeta.parse(defaultNode.onDataChange(loadDefault))
  }

  def defaultMetaData : MDCCMetadata = {
    assert(defaultMeta != null)
    defaultMeta
  }

  def defaultBallot : MDCCBallot = {
    assert(defaultMeta != null)
    MDCCMetaHelper.currentBallot(defaultMeta)
  }


  def init(defaultPartition : SCADSService) : MDCCMetadata = {
     if(!nsRoot.get(MDCC_DEFAULT_META).isDefined) {
      try {
        val createLock = nsRoot.createChild("trxLock", mode=CreateMode.EPHEMERAL)
        defaultMeta = MDCCMetadata(0, MDCCBallotRange(0,0,0,defaultPartition, true) :: Nil)
        logger.info("Default Metadata: " + defaultMeta)
        nsRoot.createChild(MDCC_DEFAULT_META, defaultMeta.toBytes)
        nsRoot("trxLock").delete()
      } catch {
        /* Someone else has the create lock, so we should wait until they finish */
        case e: KeeperException if e.code == KeeperException.Code.NODEEXISTS => {
          logger.info("Failed to grab trxLock for meta data. Waiting for creation for finish.")
        }
      }
    }
    loadDefault()
  }

}

object MDCCMetaDefault {
  protected val MDCC_DEFAULT_META = "mdccdefaultmeta"

  protected lazy val defaults = new ConcurrentHashMap[ZooKeeperProxy#ZooKeeperNode,  MDCCMetaDefault]

  def getOrCreateDefault(nsRoot : ZooKeeperProxy#ZooKeeperNode, defaultPartition : SCADSService) : MDCCMetaDefault  = {
    var default = defaults.get(nsRoot)
    if (default == null) {
      defaults.synchronized{
        default = new MDCCMetaDefault(nsRoot)
        default.init(defaultPartition)
        defaults.put(nsRoot, default)
      }
    }
    default
  }

  def getDefault(nsRoot : ZooKeeperProxy#ZooKeeperNode) : MDCCMetaDefault  = {
    var default = defaults.get(nsRoot)
    if (default == null) {
      defaults.synchronized{
        default = new MDCCMetaDefault(nsRoot)
        default.loadDefault()
        defaults.put(nsRoot, default)
      }
    }
    default
  }


}
