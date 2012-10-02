package edu.berkeley.cs
package scads
package storage
package client
package index

import avro._
import avro.runtime._
import avro.marker.AvroPair
import org.apache.avro.generic.IndexedRecord
import storage.PairNamespace

/**
 * Simple support for calling user defined trigger functions when records are added or deleted.
 */
trait TriggerManager[PairType <: AvroPair] extends RangeKeyValueStoreLike[IndexedRecord, IndexedRecord, PairType] {
  this: PairNamespace[PairType] =>

  type AddTriggerFunction = (Traversable[PairType]) => Unit
  type DeleteTriggerFunction = (Traversable[IndexedRecord]) => Unit

  var addTriggers = List.empty[AddTriggerFunction]
  var deleteTriggers = List.empty[DeleteTriggerFunction]

  override abstract def put(key: IndexedRecord, value: Option[IndexedRecord]): Unit = {
    // put/maintain order is important for self-joins
    value match {
      case None =>
        deleteTriggers.foreach(_(Seq(key)))
        super.put(key, value)
      case Some(v) =>
        /* OH GOD WHY?!?! This isn't very efficient, but I'm not really sure how to do this more elegantly */
        /* Clearly if you are using the single put interface you don't care about efficiency though... right? */
        val rec = pairManifest.erasure.newInstance().asInstanceOf[PairType]
        rec.key.parse(key.toBytes)
        rec.value.parse(v.toBytes)
        addTriggers.foreach(_(Seq(rec)))
        super.put(key, value)
    }
  }

  override abstract def ++=(that: TraversableOnce[PairType]): Unit = {
    val traversable = that.toList
    super.++=(traversable)
    addTriggers.foreach(_(traversable))
  }

  override abstract def --=(that: TraversableOnce[PairType]): Unit = {
    val traversable = that.toList
    deleteTriggers.foreach(_(traversable.map(_.key)))
    super.--=(traversable)
  }
}
