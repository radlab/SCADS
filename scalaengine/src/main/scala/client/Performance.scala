package edu.berkeley.cs
package scads
package storage
package client

import comm._
import avro.marker.AvroPair

trait PerformanceLogger[BulkType <: AvroPair] extends Namespace with Protocol {
  private var logFn: Option[Long => Unit] = None

  def logPerformanceData(fn: Long => Unit): Unit = {
    logFn = Some(fn)
  }

  abstract override def getBytes(key: Array[Byte]): Option[Array[Byte]] = {
    val start = System.currentTimeMillis
    val resp = super.getBytes(key)
    logFn.map(_(System.currentTimeMillis - start))
    resp
  }

  abstract override def asyncGetBytes(key: Array[Byte]): ScadsFuture[Option[Array[Byte]]] = {
    val start = System.currentTimeMillis
    val fut = super.asyncGetBytes(key)
    fut.respond(_ => {
      logFn.map(_(System.currentTimeMillis - start))
    })
    fut
  }
}
