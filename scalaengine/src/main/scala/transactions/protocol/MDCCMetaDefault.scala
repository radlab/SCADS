package edu.berkeley.cs.scads
package storage
package transactions
package mdcc

import comm._

import net.lag.logging.Logger
import org.apache.zookeeper._
import collection.mutable.HashMap
import java.util.concurrent.ConcurrentHashMap
import config.Config

class MDCCMetaDefault(nsRoot: ZooKeeperProxy#ZooKeeperNode) {
  import MDCCMetaDefault._

  protected lazy val logger = Logger()

  @volatile var _defaultMeta : MDCCMetadata = null

  protected val fastDefault = Config.config.getBool("scads.mdcc.fastDefault").getOrElse({logger.error("Config does not define scads.mdcc.fastDefault. Using fastDefault = true as default"); true})


  def loadDefault() : MDCCMetadata = {
    val defaultNode =
      nsRoot.get(MDCC_DEFAULT_META, tries=1) match {
        case None => {
          nsRoot.awaitChild(MDCC_DEFAULT_META, timeout=5 * 1000)
          val tmp = nsRoot.get(MDCC_DEFAULT_META)
          assert(tmp.isDefined)
          tmp.get
        }
        case Some(x) => x
      }

    val reader = new AvroSpecificReaderWriter[MDCCMetadata](None)
    _defaultMeta = reader.deserialize(defaultNode.onDataChange(loadDefault))
    logger.debug("Reloaded default metadata: %s", _defaultMeta)
    _defaultMeta
  }

  def defaultMetaData : MDCCMetadata = {
    assert(_defaultMeta != null)
    _defaultMeta
  }

  def defaultBallot : MDCCBallot = {
    assert(_defaultMeta != null)
    _defaultMeta.currentVersion
  }


  def init(defaultPartition : SCADSService,  forceNewMeta : Boolean = false) : MDCCMetadata = {
    if(!nsRoot.get(MDCC_DEFAULT_META).isDefined || forceNewMeta) {
      try {
        val createLock = nsRoot.createChild("trxLock", mode=CreateMode.EPHEMERAL)
        _defaultMeta = MDCCMetadata(MDCCBallot(0,0, defaultPartition, fastDefault), MDCCBallotRange(0,0,0,defaultPartition, fastDefault) :: Nil, true, true)
        logger.info("Default Metadata: " + _defaultMeta)
        val writer = new AvroSpecificReaderWriter[MDCCMetadata](None)
        val defaultBytes = writer.serialize(_defaultMeta)
        val defaultNode = nsRoot.getOrCreate(MDCC_DEFAULT_META)
        defaultNode.data = defaultBytes
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

  protected val defaults = new ConcurrentHashMap[ZooKeeperProxy#ZooKeeperNode,  MDCCMetaDefault]

  def getOrCreateDefault(nsRoot : ZooKeeperProxy#ZooKeeperNode, defaultPartition : SCADSService, forceNewMeta : Boolean = false) : MDCCMetaDefault = {
    var default = defaults.get(nsRoot)
    if (default == null) {
      defaults.synchronized {
        default = new MDCCMetaDefault(nsRoot)
        default.init(defaultPartition)
        defaults.put(nsRoot, default)
      }
    }
    if(forceNewMeta)
      default.init(defaultPartition, forceNewMeta)
    default
  }

  def getDefault(nsRoot : ZooKeeperProxy#ZooKeeperNode) : MDCCMetaDefault = {
    var default = defaults.get(nsRoot)
    if (default == null) {
      defaults.synchronized {
        default = new MDCCMetaDefault(nsRoot)
        default.loadDefault()
        defaults.put(nsRoot, default)
      }
    }
    default
  }

}
