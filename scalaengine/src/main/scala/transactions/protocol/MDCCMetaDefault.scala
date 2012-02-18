package edu.berkeley.cs.scads
package storage
package transactions
package mdcc

import comm._

import edu.berkeley.cs.avro.marker.{AvroPair, AvroRecord, AvroUnion}
import net.lag.logging.Logger
import org.apache.zookeeper._
import collection.mutable.HashMap
import java.util.concurrent.ConcurrentHashMap
import config.Config
import _root_.transactions.protocol.MDCCRoutingTable

case class ServiceList(var services: Seq[SCADSService]) extends AvroRecord

class MDCCMetaDefault(nsRoot: ZooKeeperProxy#ZooKeeperNode) {
  import MDCCMetaDefault._

  protected lazy val logger = Logger()

  @volatile var _defaultMeta : MDCCMetadata = null
  @volatile var _serviceMap: Map[String, Seq[SCADSService]] = null

  protected val fastDefault = Config.config.getBool("scads.mdcc.fastDefault").getOrElse({
    logger.error("Config does not define scads.mdcc.fastDefault.")
    val sysVal = System.getProperty("scads.mdcc.fastDefault")
    if (sysVal != null) {
      logger.error("Using system property for scads.mdcc.fastDefault = " + sysVal)
      sysVal == "true"
    } else {
      logger.error("Config and system property do not define scads.mdcc.fastDefault. Using fastDefault = true as default")
      true
    }
  })
  protected val defaultRounds : Long =  Config.config.getLong("scads.mdcc.DefaultRounds").getOrElse({
    logger.error("Config does not define scads.mdcc.DefaultRounds.")
    val sysVal = System.getProperty("scads.mdcc.DefaultRounds")
    if (sysVal != null) {
      logger.error("Using system property for scads.mdcc.DefaultRounds = " + sysVal)
      sysVal.toLong
    } else {
      logger.error("Config and system property do not define scads.mdcc.DefaultRounds. Using DefaultRounds = 1 as default")
      1
    }
  })
  protected val localMasterPercentage : Long =  Config.config.getLong("scads.mdcc.localMasterPercentage").getOrElse({
    logger.error("Config does not define scads.mdcc.localMasterPercentage.")
    val sysVal = System.getProperty("scads.mdcc.localMasterPercentage")
    if (sysVal != null) {
      logger.error("Using system property for scads.mdcc.localMasterPercentage = " + sysVal)
      sysVal.toLong
    } else {
      logger.error("Config and system property do not define scads.mdcc.localMasterPercentage. Using localMasterPercentage = 20 as default")
      20
    }
  })


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

    val listWriter = new AvroSpecificReaderWriter[ServiceList](None)
    val serviceList = nsRoot.get(MDCC_META_LIST) match {
      case None => ServiceList(Nil)
      case Some(m) => listWriter.deserialize(m.data)
    }

    _serviceMap = serviceList.services.zipWithIndex.groupBy(t => t._1.host.split("\\.")(1)).map {
      case (host, results) => {
        // For each distinct ip, find the newest result.
        // Basically, gets the newest service per hostname.
        val uniques = results.map(r => r._1.host.split("\\.")(0)).distinct.map(d => results.sortWith((a, b) => b._2 < a._2).find(x => d == x._1.host.split("\\.")(0)).get).map(x => x._1)
        (host, uniques)
      }
    }.toMap

    val reader = new AvroSpecificReaderWriter[MDCCMetadata](None)
    _defaultMeta = reader.deserialize(defaultNode.onDataChange(loadDefault))
    logger.debug("Reloaded default metadata: %s", _defaultMeta)
    _defaultMeta
  }

  val rand = new scala.util.Random
  val routingTable = new MDCCRoutingTable(nsRoot)

  // "us-west-1", "compute-1", "eu-west-1", "ap-northeast-1", "ap-southeast-1"
  def defaultMetaData(key: Array[Byte]) : MDCCMetadata = {
    assert(_defaultMeta != null)

    val hash = java.util.Arrays.hashCode(key)
    val randVal = scala.math.abs(hash) % 100

    val randomRegion = if (localMasterPercentage < 0) {
      "ap-southeast-1"
    } else if (randVal < localMasterPercentage) {
      "us-west-1"
    } else {
      val l = List("compute-1", "eu-west-1", "ap-northeast-1", "ap-southeast-1")
      l(rand.nextInt(l.size))
    }
    val randomHost = routingTable.serversForKey(key).find(x => x.host.split("\\.")(1) == randomRegion).get.host
    val randomService = _serviceMap(randomRegion).find(x => x.host == randomHost) match {
      case None =>
        // This should never happen.
        logger.error("serviceMap does not have correct partition. serviceMap: " + _serviceMap(randomRegion) + " host: " + randomHost)
        _serviceMap(randomRegion)(randVal % _serviceMap(randomRegion).size)
      case Some(h) => h
    }

    val r = MDCCMetadata(MDCCBallot(0, 0, randomService, fastDefault), MDCCBallotRange(0, defaultRounds-1, 0, randomService, fastDefault) :: Nil, true, true)
    r
  }

  // Returns true if metadata was changed.
  def init(defaultPartition : SCADSService,  forceNewMeta : Boolean = false) : Boolean = {
    var changed = false
    if(!nsRoot.get(MDCC_DEFAULT_META).isDefined || forceNewMeta) {
      try {
        val createLock = nsRoot.createChild("trxLock", mode=CreateMode.EPHEMERAL)
        _defaultMeta = MDCCMetadata(MDCCBallot(0,0, defaultPartition, fastDefault), MDCCBallotRange(0,defaultRounds-1,0,defaultPartition, fastDefault) :: Nil, true, true)
        logger.info("Default Metadata: " + _defaultMeta)
        val writer = new AvroSpecificReaderWriter[MDCCMetadata](None)
        val defaultBytes = writer.serialize(_defaultMeta)
        val defaultNode = nsRoot.getOrCreate(MDCC_DEFAULT_META)
        defaultNode.data = defaultBytes

        val listWriter = new AvroSpecificReaderWriter[ServiceList](None)
        val services = nsRoot.get(MDCC_META_LIST) match {
          case None => ServiceList(Nil)
          case Some(m) => listWriter.deserialize(m.data)
        }
        val listNode = nsRoot.getOrCreate(MDCC_META_LIST)
        val newServices = ServiceList(services.services ++ List(defaultPartition))
        logger.info("newServices: " + newServices)
        listNode.data = listWriter.serialize(newServices)

        nsRoot("trxLock").delete()
        changed = true
      } catch {
        /* Someone else has the create lock, so we should wait until they finish */
        case e: KeeperException if e.code == KeeperException.Code.NODEEXISTS => {
          logger.info("Failed to grab trxLock for meta data. Waiting for creation for finish.")
        }
      }
    }
    loadDefault()
    changed
  }

}

object MDCCMetaDefault {
  protected val MDCC_DEFAULT_META = "mdccdefaultmeta"
  protected val MDCC_META_LIST = "mdccservicelist"

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
    if(forceNewMeta) {
      while (default.init(defaultPartition, forceNewMeta) == false) {}
    }
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
    } else {
      default.loadDefault()
    }
    default
  }

}
