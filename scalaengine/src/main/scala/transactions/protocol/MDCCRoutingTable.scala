package transactions.protocol


import _root_.edu.berkeley.cs.scads.comm.ZooKeeperProxy
import _root_.edu.berkeley.cs.scads.storage.{RoutingTableMessage, PartitionService}
import _root_.edu.berkeley.cs.scads.util.{RangeTable, RangeType}
import org.apache.avro.io.BinaryData
import org.apache.avro.Schema
import edu.berkeley.cs.scads.storage.DefaultKeyRoutableLike
import net.lag.logging.Logger


/**
 * FIXME This whole class is a hack!!!!
 */
class MDCCRoutingTable(nsRoot: ZooKeeperProxy#ZooKeeperNode) {
  private var _routingTable : RangeTable[Array[Byte], PartitionService] = null

  private val keySchema = new Schema.Parser().parse(new String(nsRoot("keySchema").data))

  protected lazy val logger = Logger()

  def compareKey(x: Array[Byte], y: Array[Byte]): Int =  BinaryData.compare(x, 0, y, 0, keySchema)

  def mergeCond(a : Seq[PartitionService], b : Seq[PartitionService]) : Boolean = false //We never merge from the server

  def routingTable = {
    if (_routingTable == null) loadRoutingTable
    _routingTable
  }

  def loadRoutingTable() : Unit = {
    val rangeSeq = classOf[RoutingTableMessage].newInstance
    rangeSeq.parse(watchMetadata(DefaultKeyRoutableLike.ZOOKEEPER_ROUTING_TABLE, loadRoutingTable))
    val partition : Array[RangeType[Array[Byte], PartitionService]] = rangeSeq.partitions.map(a => new RangeType(a.startKey, a.servers)).toArray
    _routingTable = new RangeTable[Array[Byte], PartitionService](
      partition,
      compareKey _,
      mergeCond _)
  }

  def watchMetadata(key: String, func: () => Unit): Array[Byte] = {
    logger.info("Watching metadata %s for %s", key, nsRoot)
    nsRoot(key).onDataChange(func)
  }

  def serversForKey(key: Array[Byte]): Seq[PartitionService] = {
    routingTable.valuesForKey(key)
  }

}
