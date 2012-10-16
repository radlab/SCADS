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
import comm.ScadsFuture

/**
 * Simple support for calling user defined trigger functions when records are added or deleted.
 */
trait TriggerManager[PairType <: AvroPair] extends RangeKeyValueStoreLike[IndexedRecord, IndexedRecord, PairType] {
  this: PairNamespace[PairType] =>

  type AddTriggerFunction = (Traversable[PairType]) => Unit
  type DeleteTriggerFunction = (Traversable[IndexedRecord]) => Unit

  var addTriggers = List.empty[AddTriggerFunction]
  var deleteTriggers = List.empty[DeleteTriggerFunction]
  var triggersActive = true

  override abstract def put(key: IndexedRecord, value: Option[IndexedRecord]): Unit = {
    if(triggersActive)
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
    else
      super.put(key, value)
  }

  override abstract def asyncPut(key: IndexedRecord, value: Option[IndexedRecord]): ScadsFuture[Unit] = {
    if(triggersActive)
      // put/maintain order is important for self-joins
      value match {
        case None =>
          deleteTriggers.foreach(_(Seq(key)))
          super.asyncPut(key, value)
        case Some(v) =>
          /* OH GOD WHY?!?! This isn't very efficient, but I'm not really sure how to do this more elegantly */
          /* Clearly if you are using the single put interface you don't care about efficiency though... right? */
          val rec = pairManifest.erasure.newInstance().asInstanceOf[PairType]
          rec.key.parse(key.toBytes)
          rec.value.parse(v.toBytes)
          addTriggers.foreach(_(Seq(rec)))
          super.asyncPut(key, value)
      }
    else
      super.asyncPut(key, value)
  }

  override abstract def ++=(that: TraversableOnce[PairType]): Unit = {
    if(triggersActive) {
      val traversable = that.toBuffer
      super.++=(traversable)
      addTriggers.foreach(_(traversable))
    }
    else
      super.++=(that)

  }

  override abstract def --=(that: TraversableOnce[PairType]): Unit = {
    if(triggersActive) {
      val traversable = that.toBuffer
      deleteTriggers.foreach(_(traversable.map(_.key)))
      super.--=(traversable)
    }
    else
      super.--=(that)
  }
}
