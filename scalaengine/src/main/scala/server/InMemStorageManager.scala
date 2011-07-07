package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.avro.runtime.ScalaSpecificRecord
import java.nio.ByteBuffer
import java.io.{ObjectOutputStream,ObjectInputStream,Serializable,ByteArrayOutputStream}
import java.util.{Comparator, TreeMap, Arrays => JArrays}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.util._
import org.apache.avro.Schema
import net.lag.logging.Logger

@serializable
private case class EQArray(bytes:Array[Byte])  {
  override def equals(other:Any):Boolean = JArrays.equals(bytes,other.asInstanceOf[EQArray].bytes)
  override def hashCode():Int = JArrays.hashCode(bytes)
}
@serializable
private case class EQCmp(keySchema:Schema) extends Comparator[EQArray] with Serializable{
  override def compare(o1:EQArray, o2:EQArray):Int = {
    org.apache.avro.io.BinaryData.compare(o1.bytes, 0, o2.bytes, 0, keySchema)
  }
  override def equals(other: Any): Boolean = other match {
    case ac:EQCmp => keySchema equals ac.keySchema
    case _ => false
  }

  /* TODO: In case we ever want to serialize the treemap, the comparator
   * needs to be serializable.  Schema is not.  So we should write it out
   * as a string.  For now, we do nothing. */
  private def writeObject(out:ObjectOutputStream) = {}
  private def readObject(in:ObjectInputStream) = {}
  private def readObjectNoData() = {}
}

/* (fairly) efficiently add 0ed metadata to a ScalaSpecificRecord.
 * We should really add a way to say that a partition isn't storing metadata though.
 */
object MetaDizer {
  private val stream:ByteArrayOutputStream = new ByteArrayOutputStream()
  private val dummyMD = new Array[Byte](16)
  def metadize(rec:ScalaSpecificRecord):Array[Byte]  = {
    stream.reset
    stream.write(dummyMD)
    rec.toBytes(stream)
    stream.toByteArray
  }
  
}

class InMemStorageManager
  (val partitionIdLock: ZooKeeperProxy#ZooKeeperNode, 
   val startKey:Option[Array[Byte]],
   val endKey:Option[Array[Byte]],
   val root: ZooKeeperProxy#ZooKeeperNode, 
   val keySchema: Schema, 
   valueSchema: Schema,
   valueType:String)
  extends StorageManager with AvroComparator
{
  private implicit def bytes2eqarray(bytes:Array[Byte]):EQArray = new EQArray(bytes)
  private val iterateValsBreakable = new Breaks
  private val valueClass = Class.forName(valueType)

  private val map = new TreeMap[EQArray,ScalaSpecificRecord](new EQCmp(keySchema).asInstanceOf[Comparator[EQArray]])

  def get(key:Array[Byte]):Option[Array[Byte]] = {
    // can't reply on implicit here because .get just takes an Object
    val v = map.get(bytes2eqarray(key))
    if (v != null) {
      return Option(MetaDizer.metadize(v))
    }
    else
      return Option(null)
  }

  def put(key:Array[Byte],value:Option[Array[Byte]]):Unit = {
    value match {
      case Some(v) => {
        val rec = valueClass.newInstance.asInstanceOf[ScalaSpecificRecord]
        rec.parse(v)
        map.put(key,rec)
      }
      case None => map.remove(key)
    }
  }

  def testAndSet(key:Array[Byte], value:Option[Array[Byte]], expectedValue:Option[Array[Byte]]):Boolean = {
    synchronized { 
      val existing = map.get(bytes2eqarray(key))
      val exbytes = 
        if (existing != null)
          MetaDizer.metadize(existing)
        else
          null
      if(JArrays.equals(expectedValue.orNull, exbytes)) {
        value match {
          case Some(v) => {
            val rec = valueClass.newInstance.asInstanceOf[ScalaSpecificRecord]
            rec.parse(v)
            map.put(key,rec)
          }
          case None => map.remove(key)
        }
        true
      } 
      else
        false
    }
  }

  def bulkUrlPut(parser:RecParser, locations:Seq[String]) = {
    locations foreach(location => {
      parser.setLocation(location)
      val url = new java.net.URL(location)
      parser.setInput(url.openStream)
      var kv = parser.getNext()
      while (kv != null) {
        map.put(kv._1.asInstanceOf[Array[Byte]], kv._2.asInstanceOf[ScalaSpecificRecord])
        kv = parser.getNext()
      }
    })
  }

  def bulkPut(records:Seq[PutRequest]) = {
    records.foreach(rec => { 
      val recval = rec.value.get
      if (recval != null) {
        val nrec = valueClass.newInstance.asInstanceOf[ScalaSpecificRecord]
        nrec.parse(recval)
        map.put(rec.key,nrec)
      } else
        map.remove(rec.key)
    })
  }

  def getRange(minKey:Option[Array[Byte]], maxKey:Option[Array[Byte]], limit:Option[Int], offset:Option[Int], ascending:Boolean):Seq[Record] = {
    throw new RuntimeException("InMemStorageManager does not support ranges")
  }
  def getBatch(ranges:Seq[MessageBody]):ArrayBuffer[GetRangeResponse]  = {
    throw new RuntimeException("InMemStorageManager does not support ranges")
  }
  def countRange(minKey:Option[Array[Byte]], maxKey:Option[Array[Byte]]):Int  = {
    var count = 0
    minKey match {
      case Some(b) => throw new RuntimeException("InMemStorageManager can only count entire key set")
      case None => {
        maxKey match {
          case Some(b) => throw new RuntimeException("InMemStorageManager can only count entire key set")
          case None => {
            iterateVals((_) => count += 1)
          }
        }
      }
    }
    count
  }
  def getResponsibility():(Option[Array[Byte]],Option[Array[Byte]]) = (startKey, endKey)

  def copyData(src:PartitionService, overwrite:Boolean)  = {
    throw new RuntimeException("Not Implemented")
  }


  def applyAggregate(groups:Seq[String],
                     keyType:String,
                     valType:String,
                     filters:Seq[AggFilter],
                     aggs:Seq[AggOp]):Seq[GroupedAgg] = {
    val filterFunctions = filters.map(f => {
      val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(f.obj))
      ois.readObject.asInstanceOf[Filter[ScalaSpecificRecord]]
    })
    var filterPassed = true

    val aggregates =
      aggs.map(aggOp => {
        val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(aggOp.obj))
        ois.readObject.asInstanceOf[RemoteAggregate[ScalaSpecificRecord,ScalaSpecificRecord,ScalaSpecificRecord]]
      })
            

    val remKey = Class.forName(keyType).newInstance().asInstanceOf[ScalaSpecificRecord]
    val remVal = Class.forName(valType).newInstance().asInstanceOf[ScalaSpecificRecord]
    
    val groupSchemas = groups.map(valueSchema.getField(_).schema)
    val groupInts = groups.map(valueSchema.getField(_).pos)
    
    var groupMap:Map[CharSequence, scala.collection.mutable.ArraySeq[ScalaSpecificRecord]] = null
    var groupBytesMap:Map[CharSequence, Array[Byte]] = null
    var groupKey:CharSequence = null
    
    var aggVals:Seq[ScalaSpecificRecord] = null
    if (groups.size == 0) { // no groups, so let's init values now
      aggVals = aggregates.map(_.init())
    }
    else {
      groupMap = new scala.collection.immutable.HashMap[CharSequence,scala.collection.mutable.ArraySeq[ScalaSpecificRecord]]
      groupBytesMap = new scala.collection.immutable.HashMap[CharSequence, Array[Byte]]
    }
    
    var curAggVal =
      if (groups.size == 0)
        scala.collection.mutable.ArraySeq(aggVals:_*)
      else
        null

    var stop = false

    iterateVals((remVal) => {
      filterPassed = true
      filterFunctions.foreach(ff => { 
        if (filterPassed) { // once one failed just ignore the rest
          filterPassed = ff.applyFilter(remVal)
        }
      })
      if (filterPassed) {  // record passes filters
        if (groups.size != 0) {
          groupKey = AnalyticsUtils.getGroupKey(groupInts,remVal)
          curAggVal = groupMap.get(groupKey) match {
            case Some(thing) => thing
            case None => { // this is a new group, so init the starting values for the agg
              groupBytesMap += ((groupKey,AnalyticsUtils.getGroupBytes(groupInts,groupSchemas,remVal)))
              scala.collection.mutable.ArraySeq(aggregates.map(_.init()):_*)
            }
          }
        }
        stop = true
        aggregates.view.zipWithIndex foreach(aggregate => {
          curAggVal(aggregate._2) = aggregate._1.applyAggregate(curAggVal(aggregate._2),null,remVal)
          stop &= aggregate._1.stop
        })
        if (groups.size != 0)
          groupMap += ((groupKey,curAggVal))
        if (stop)
          iterateValsBreakable.break
      }
    })

    if (groups.size == 0) {
      List(GroupedAgg(None,curAggVal.map(_.asInstanceOf[ScalaSpecificRecord].toBytes)))
    } else {
      groupMap.map(kv => {
        GroupedAgg(Option(groupBytesMap(kv._1)),kv._2.map(_.asInstanceOf[ScalaSpecificRecord].toBytes))
      }).toSeq
    }
  }


  private def iterateVals(func:(ScalaSpecificRecord)=>Unit):Unit = {
    val it = map.values.iterator
    iterateValsBreakable.breakable { // used to allow func to break out early
      while(it.hasNext) 
        func(it.next)
    }
  }

  protected var currentStats = PartitionWorkloadStats(0,0)
  protected var completedStats = PartitionWorkloadStats(0,0)
  private var statsClearedTime = System.currentTimeMillis
  def getWorkloadStats():(PartitionWorkloadStats,Long) = (completedStats,statsClearedTime)
  def resetWorkloadStats():PartitionWorkloadStats = {
    val ret = completedStats
    completedStats = currentStats
    statsClearedTime = System.currentTimeMillis()
    currentStats = PartitionWorkloadStats(0,0)
    ret
  }
  def startup():Unit = {}
  def shutdown():Unit = {}

}

