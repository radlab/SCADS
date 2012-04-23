package edu.berkeley.cs.scads.storage
package client
package index

import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, IndexedRecord }
import org.apache.zookeeper.{ CreateMode, WatchedEvent }
import org.apache.zookeeper.Watcher.Event.EventType

import scala.collection.JavaConversions._
import scala.collection.mutable.LinkedHashMap

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.avro.marker._
import org.apache.avro.util.Utf8

trait ViewManager[BulkType <: AvroPair] extends RangeKeyValueStoreLike[IndexedRecord, IndexedRecord, BulkType] {

  type ViewDelta = (IndexedRecord) => Seq[IndexedRecord]

  /* order matters for delta queries created recursively */
  @volatile var updateRules = LinkedHashMap[(String, String), (IndexNamespace, ViewDelta)]()

  // delta unique for every (relationAlias, view.name)
  def registerView(relationAlias: String, view: IndexNamespace, delta: ViewDelta) = {
    synchronized {
      var newRules = updateRules.clone()
      newRules += (((relationAlias, view.name), (view, delta)))
      updateRules = newRules
    }
  }

  override abstract def put(key: IndexedRecord, value: Option[IndexedRecord]): Unit = {
    // put/maintain order is important for self-joins
    value match {
      case None =>
        updateViews(key, None)
        super.put(key, value)
      case Some(_) =>
        super.put(key, value)
        updateViews(key, dummyValueBytes)
    }
  }

  override abstract def ++=(that: TraversableOnce[BulkType]): Unit = {
    val traversable = that.toTraversable
    super.++=(traversable)
    updateViews(traversable, dummyValueBytes)
  }

  override abstract def --=(that: TraversableOnce[BulkType]): Unit = {
    val traversable = that.toTraversable
    updateViews(traversable, None)
    super.--=(traversable)
  }

  private implicit def bulkToKey(b: BulkType): IndexedRecord = b.key
  private implicit def toMany(r: IndexedRecord): Traversable[IndexedRecord] = Traversable(r)

  private def updateViews(records: Traversable[IndexedRecord],
                          valueBytes: Option[Array[Byte]]): Unit = {
    // order matters for puts vs dels
    val rules =
      if (valueBytes.isEmpty)
        updateRules.values.toList.reverse
      else
        updateRules.values

    // It would be more efficient to have 'records' on the outer loop,
    // but we need to update views serially in the general case.
    // TODO future optimization: insert barriers to parallelize safely
    for ((view, delta) <- rules) {
      for (t <- records) {
        for (u <- delta(t)) {
          view.putBulkBytes(view.keyToBytes(u), valueBytes)
        }
      }
      view.flushBulkBytes
    }
  }

  // imitate schema of IndexManager
  private val dummyValueBytes = {
    val schema = Schema.createRecord("DummyValue", "", "", false)
    schema.setFields(Seq(new Schema.Field("b", Schema.create(Schema.Type.BOOLEAN), "", null)))
    val exemplar = new GenericData.Record(schema)
    exemplar.put(0, false)
    val valueReaderWriter = new AvroGenericReaderWriter[IndexedRecord](None, schema)
    val exemplarBytes = valueReaderWriter.serialize(exemplar)
    Some(exemplarBytes)
  }
}
