package edu.berkeley.cs
package scads
package storage
package client

import comm._
import avro.marker.AvroPair
import org.apache.avro.generic.IndexedRecord

trait PerformanceLogger[BulkType <: AvroPair] extends Namespace
    with Protocol
    with RangeKeyValueStoreLike[IndexedRecord, IndexedRecord, BulkType] {
  private var logGetFn: Option[Long => Unit] = None
  private var logGetRangeFn: Option[Long => Unit] = None

  def logGetPerformance(fn: Long => Unit): Unit = {
    logGetFn = Some(fn)
  }

  def logGetRangePerformance(fn: Long => Unit): Unit = {
    logGetRangeFn = Some(fn)
  }

  abstract override def getBytes(key: Array[Byte]): Option[Array[Byte]] = {
    val t0 = System.currentTimeMillis
    val resp = super.getBytes(key)
    logGetFn.foreach(_(System.currentTimeMillis - t0))
    resp
  }

  abstract override def asyncGetBytes(key: Array[Byte]): ScadsFuture[Option[Array[Byte]]] = {
    val t0 = System.currentTimeMillis
    val fut = super.asyncGetBytes(key)
    fut.respond(_ => {
      logGetFn.foreach(_(System.currentTimeMillis - t0))
    })
    fut
  }

  abstract override def getRange(start: Option[IndexedRecord],
                                 end: Option[IndexedRecord],
                                 limit: Option[Int] = None,
                                 offset: Option[Int] = None,
                                 ascending: Boolean = true): Seq[BulkType] = {
    val t0 = System.currentTimeMillis
    val resp = super.getRange(start, end, limit, offset, ascending)
    logGetRangeFn.foreach(_(System.currentTimeMillis - t0))
    resp
  }

  abstract override def asyncGetRange(start: Option[IndexedRecord],
                                      end: Option[IndexedRecord],
                                      limit: Option[Int] = None,
                                      offset: Option[Int] = None,
                                      ascending: Boolean = true): ScadsFuture[Seq[BulkType]] = {
    val t0 = System.currentTimeMillis
    val fut = super.asyncGetRange(start, end, limit, offset, ascending)
    fut.respond(_ => {
      logGetRangeFn.foreach(_(System.currentTimeMillis - t0))
    })
    fut
  }
}
