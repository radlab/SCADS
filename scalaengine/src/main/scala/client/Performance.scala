package edu.berkeley.cs
package scads
package storage
package client

import comm._
import avro.marker.AvroPair
import org.apache.avro.generic.IndexedRecord
import perf._

trait PerformanceLogger[BulkType] extends Namespace
    with Protocol
    with RangeKeyValueStoreLike[IndexedRecord, IndexedRecord, BulkType] {

  val getTimes = Histogram(1, 5000)
  val getRangeTimes = Histogram(1, 5000)

  onOpen { isNew =>
    getTimes.name = name + "_getTimes"
    getRangeTimes.name = name + "_getRangeTimes"
    isNew
  }

  abstract override def getBytes(key: Array[Byte]): Option[Array[Byte]] = {
    val t0 = System.currentTimeMillis
    val resp = super.getBytes(key)
    val t1 = System.currentTimeMillis()
    getTimes.add(t1 - t0)
    resp
  }

  abstract override def asyncGetBytes(key: Array[Byte]): ScadsFuture[Option[Array[Byte]]] = {
    val t0 = System.currentTimeMillis
    val fut = super.asyncGetBytes(key)
    fut.respond(_ => {
      val t1 = System.currentTimeMillis()
      getTimes.add(t1 - t0)
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
    val t1 = System.currentTimeMillis()
    getRangeTimes.add(t1 - t0)
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
      val t1 = System.currentTimeMillis()
      getRangeTimes.add(t1 - t0)
    })
    fut
  }
}
