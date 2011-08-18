package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.avro.runtime.ScalaSpecificRecord
import java.nio.ByteBuffer
import java.io.{ObjectOutputStream,ObjectInputStream,Serializable,ByteArrayOutputStream,ByteArrayInputStream}
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



class InMemStorageManager
  (val partitionIdLock: ZooKeeperProxy#ZooKeeperNode, 
   val startKey:Option[Array[Byte]],
   val endKey:Option[Array[Byte]],
   val root: ZooKeeperProxy#ZooKeeperNode, 
   val keySchema: Schema, 
   valueSchema: Schema,
   valueType:String)
  extends StorageManager with AvroComparator with SimpleRecordMetadataExtractor
{
  private implicit def bytes2eqarray(bytes:Array[Byte]):EQArray = new EQArray(bytes)
  private val iterateValsBreakable = new Breaks
  private val valueClass = Class.forName(valueType)
  
  private val map = new TreeMap[EQArray,(Array[Byte],ScalaSpecificRecord)](new EQCmp(keySchema).asInstanceOf[Comparator[EQArray]])

  // this method puts a scala record and some metadata together into a byte array
  private def toBytes(vals:(Array[Byte], ScalaSpecificRecord)): Array[Byte] = {
    val bytes = vals._2.toBytes
    val result = java.util.Arrays.copyOf(vals._1, vals._1.length + bytes.length);
    System.arraycopy(bytes, 0, result, vals._1.length, bytes.length);
    result
  }
  

  def get(key:Array[Byte]):Option[Array[Byte]] = {
    // can't rely on implicit here because .get just takes an Object
    val v = map.get(bytes2eqarray(key))
    if (v != null) {
      return Option(toBytes(v))
    }
    else
      return Option(null)
  }

  def put(key:Array[Byte],value:Option[Array[Byte]]):Unit = {
    value match {
      case Some(v) => {
        val rec = valueClass.newInstance.asInstanceOf[ScalaSpecificRecord]
        rec.parse(getRecordInputStreamFromValue(v))
        map.put(key,(extractMetadataFromValue(v),rec))
      }
      case None => map.remove(bytes2eqarray(key))
    }
  }

  def testAndSet(key:Array[Byte], value:Option[Array[Byte]], expectedValue:Option[Array[Byte]]):Boolean = {
    synchronized { 
      val existing = map.get(bytes2eqarray(key))
      val exbytes = 
        if (existing != null)
          toBytes(existing)
        else
          null
      if(JArrays.equals(expectedValue.orNull, exbytes)) {
        value match {
          case Some(v) => {
            val rec = valueClass.newInstance.asInstanceOf[ScalaSpecificRecord]
            rec.parse(getRecordInputStreamFromValue(v))
            map.put(key,(extractMetadataFromValue(v),rec))
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
        val mv = kv._2.asInstanceOf[(Array[Byte],ScalaSpecificRecord)]
        map.put(kv._1.asInstanceOf[Array[Byte]], mv)
        kv = parser.getNext()
      }
    })
  }

  def bulkPut(records:Seq[PutRequest]) = {
    records.foreach(rec => { 
      val recval = rec.value.get
      if (recval != null) {
        val nrec = valueClass.newInstance.asInstanceOf[ScalaSpecificRecord]
        nrec.parse(getRecordInputStreamFromValue(recval))
        map.put(rec.key,(extractMetadataFromValue(recval),nrec))
      } else
        map.remove(rec.key)
    })
  }

  def getRange(minKey:Option[Array[Byte]], maxKey:Option[Array[Byte]], limit:Option[Int], offset:Option[Int], ascending:Boolean):Seq[Record] = {
    val records = new ArrayBuffer[Record]
    limit.foreach(records.sizeHint(_))
    iterateVals(minKey map bytes2eqarray, maxKey map bytes2eqarray, limit, offset, ascending)((key, value) => {
      records += Record(key.bytes,Option(toBytes(value)))
    })
    records
  }
  def getBatch(ranges:Seq[MessageBody]):ArrayBuffer[GetRangeResponse]  = {
    throw new RuntimeException("InMemStorageManager does not support ranges")
  }
  def countRange(minKey:Option[Array[Byte]], maxKey:Option[Array[Byte]]):Int  = {
    var count = 0
    iterateVals(minKey map bytes2eqarray,maxKey map bytes2eqarray)((k,v) => count += 1)
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
    
    var groupMap:scala.collection.mutable.HashMap[CharSequence, scala.collection.mutable.ArraySeq[ScalaSpecificRecord]] = null
    var groupBytesMap:Map[CharSequence, Array[Byte]] = null
    var groupKey:CharSequence = null
    
    var aggVals:Seq[ScalaSpecificRecord] = null
    if (groups.size == 0) { // no groups, so let's init values now
      aggVals = aggregates.map(_.init())
    }
    else {
      groupMap = new scala.collection.mutable.HashMap[CharSequence,scala.collection.mutable.ArraySeq[ScalaSpecificRecord]]
      groupBytesMap = new scala.collection.immutable.HashMap[CharSequence, Array[Byte]]
    }
    
    var curAggVal =
      if (groups.size == 0)
        scala.collection.mutable.ArraySeq(aggVals:_*)
      else
        null

    var stop = false

    iterateVals(None,None)((remKey,mdplusval) => {
      val remVal = mdplusval._2
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


  private def iterateVals(minKey: Option[EQArray],
                          maxKey: Option[EQArray],
                          limit: Option[Int] = None,
                          offset: Option[Int] = None,
                          ascending: Boolean = true)
  (func:(EQArray,(Array[Byte],ScalaSpecificRecord))=>Unit):Unit = {
    val eMap = (minKey,maxKey) match {
      case (None, None) => map
      case (None, Some(endKey)) => map.headMap(endKey,false)
      case (Some(startKey), None) => map.tailMap(startKey,true)
      case (Some(startKey), Some(endKey)) => map.subMap(startKey,true,endKey,false)
    }

    val it = 
      if (ascending)
        eMap.entrySet.iterator
      else
        eMap.descendingMap.entrySet.iterator
    var toSkip = offset.getOrElse(0)
    var lim = limit.getOrElse(-1)
    iterateValsBreakable.breakable { // used to allow func to break out early
      while(it.hasNext) {
        if (lim == 0) return
        lim -= 1
        val mapEntry = it.next
        if (toSkip > 0) 
          toSkip -=1
        else
          func(mapEntry.getKey,mapEntry.getValue)
      }
    }
  }

  def startup():Unit = {}
  def shutdown():Unit = {}

}

