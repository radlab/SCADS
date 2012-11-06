package edu.berkeley.cs
package scads

import avro.runtime._

package object storage {
  implicit object StorageRegistry extends comm.ServiceRegistry[StorageMessage]

  /* Global flag that disables transmission/counting of tags. */
  val tracingEnabled = true

  /* Global thread-local tag for performance analysis of rpc messages */
  private val currentTag = new ThreadLocal[Option[String]]() {
    override def initialValue(): Option[String] = None
  }

  /**
   * Returns the trace tags in the current thread scope.
   */
  def getTag(): Option[String] = {
    currentTag.get
  }

  /**
   * Executes a block with a trace tag defined for the duration of execution.
   */
  def trace[A,B](tag: String)(block: => B): B = {
    try {
      pushTag(tag)
      block
    } finally {
      popTag
    }
  }

  private def pushTag(tag: String): Unit = if (tracingEnabled) {
    val cur = currentTag.get
    if (cur.isDefined) {
      val suffix = cur.get
      if (suffix.length < 100) {
        currentTag.set(Some(tag + ":" + cur.get))
      }
    } else {
      currentTag.set(Some(tag))
    }
  }

  private def popTag(): Unit = if (tracingEnabled) {
    val arr = currentTag.get.getOrElse("").split(":", 2)
    if (arr.length == 2) {
      currentTag.set(Some(arr(1)))
    } else {
      currentTag.set(None)
    }
  }
}
