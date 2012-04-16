package edu.berkeley.cs.scads.storage
package client
package index

import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, IndexedRecord }
import org.apache.zookeeper.{ CreateMode, WatchedEvent }
import org.apache.zookeeper.Watcher.Event.EventType

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.avro.marker._
import org.apache.avro.util.Utf8

trait ViewManager[BulkType <: AvroPair] extends RangeKeyValueStoreLike[IndexedRecord, IndexedRecord, BulkType] {

  type ViewDelta = (IndexedRecord) => Seq[IndexedRecord]
  @volatile var updateRules = Map[(String, String), (IndexNamespace, ViewDelta)]()

  // delta unique for every (relationAlias, view.name)
  def registerView(relationAlias: String, view: IndexNamespace, delta: ViewDelta) = {
    updateRules += (((relationAlias, view.name), (view, delta)))
  }

  override abstract def put(key: IndexedRecord, value: Option[IndexedRecord]): Unit = {
    // order is important for self-joins
    value match {
      case None =>
        updateViews(key, None)
        super.put(key, value)
      case Some(_) =>
        super.put(key, value)
        updateViews(key, nilValueBytes)
    }
  }

  override abstract def ++=(that: TraversableOnce[BulkType]): Unit = {
    val traversable = that.toTraversable
    super.++=(traversable)
    updateViews(traversable, nilValueBytes)
  }

  override abstract def --=(that: TraversableOnce[BulkType]): Unit = {
    val traversable = that.toTraversable
    updateViews(traversable, None)
    super.--=(traversable)
  }

  private implicit def bulkToKey(b: BulkType): IndexedRecord = b.key
  private implicit def toMany(r: IndexedRecord): Traversable[IndexedRecord] = Traversable(r)

  private def updateViews(that: TraversableOnce[IndexedRecord],
                          valueBytes: Option[Array[Byte]]): Unit = {
    val rules = updateRules.values
    for (t <- that) {
      for ((view, delta) <- rules) {
        for (u <- delta(t)) {
          view.putBulkBytes(view.keyToBytes(u), valueBytes)
        }
      }
    }
    for ((view, _) <- rules) {
      view.flushBulkBytes
    }
  }

  private val nilValueBytes = {
    val schema = Schema.createRecord("NilValue", "", "", false)
    schema.setFields(Nil)
    val exemplar = new GenericData.Record(schema)
    val valueReaderWriter = new AvroGenericReaderWriter[IndexedRecord](None, schema)
    val exemplarBytes = valueReaderWriter.serialize(exemplar)
    Some(exemplarBytes)
  }
}
