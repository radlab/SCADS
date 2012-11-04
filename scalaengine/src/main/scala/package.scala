package edu.berkeley.cs
package scads

import avro.runtime._

package object storage {
  implicit object StorageRegistry extends comm.ServiceRegistry[StorageMessage]

  /* Global flag that disables transmission/counting of tags. */
  val samplingEnabled = false

  /* Global thread-local tag for performance analysis of rpc messages */
  private val currentTag = new ThreadLocal[Option[String]]() {
    override def initialValue(): Option[String] = None
  }

  def getTag(): Option[String] = {
    if (samplingEnabled) {
      currentTag.get
    } else {
      None
    }
  }

  def pushTag(tag: String): Unit = {
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

  def popTag(): Unit = {
    val arr = currentTag.get.getOrElse("").split(":", 2)
    if (arr.length == 2) {
      currentTag.set(Some(arr(1)))
    } else {
      currentTag.set(None)
    }
  }
}
