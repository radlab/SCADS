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
  import IndexManager._

  type ViewDelta = (IndexedRecord) => Seq[IndexedRecord]
  @volatile var updateRules = Map[(String, String), (IndexNamespace, ViewDelta)]()

  // deltaId is unique for every (relationAlias, view.name)
  def registerView(relationAlias: String, view: IndexNamespace, delta: ViewDelta) = {
    updateRules += (((relationAlias, view.name), (view, delta)))
  }

  override abstract def put(key: IndexedRecord, value: Option[IndexedRecord]): Unit = {
    // do deletes before base update (for self joins)
    value match {
      case None =>
        for ((view, delta) <- updateRules.values) {
          for (t <- delta(key)) {
            view.put(t, None)
          }
        }
      case _ =>
    }

    super.put(key, value)

    // do puts after base update (for self joins)
    value match {
      case Some(value) =>
        for ((view, delta) <- updateRules.values) {
          for (t <- delta(key)) {
            view.put(t, Some(dummyIndexValue))
          }
        }
      case _ =>
    }
  }
}
