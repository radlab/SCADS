package edu.berkeley.cs
package scads
package storage
package client

import comm._
import avro.marker.AvroPair
import org.apache.avro.generic.IndexedRecord
import java.util
import java.io.Serializable
import util.concurrent.TimeUnit
import util.{Arrays => JArrays}

/** Caches values */
trait CacheManager[BulkType <: AvroPair] extends Namespace
  with Protocol
  with RangeKeyValueStoreLike[IndexedRecord, IndexedRecord, BulkType]
  with Serializer[IndexedRecord, IndexedRecord, BulkType]
  with ZooKeeperGlobalMetadata
  with RecordStore[BulkType] {

  protected case class EQArray(bytes:Array[Byte]) extends Serializable {
    require(bytes != null)

    override def equals(other:Any):Boolean = {
      require(bytes != null)
      if(other == null) return false
      JArrays.equals(bytes,other.asInstanceOf[EQArray].bytes)
    }
    override def hashCode():Int = JArrays.hashCode(bytes)
  }

  protected class CachingFuture(key: Array[Byte], future: ScadsFuture[Option[Array[Byte]]]) extends ScadsFuture[Option[Array[Byte]]] {
    /**
     * Cancels the current future
     */
    def cancel() = future.cancel()

    /**
     * Block on the future until T is ready
     */
    def get() = {
      addToCache(key, future.get())
    }

    /**
     * Block for up to timeout.
     */
    def get(timeout: Long, unit: TimeUnit) = {
      future.get(timeout, unit) match {
        case f @ Some(v) => addToCache(key, v); f
        case None => None
      }
    }

    /**
     * True iff the future has already been set
     */
    def isSet = future.isSet
  }

  var cacheActive: Boolean = false
  //TODO: Use a more intelligent data structure...
  val cachedValues = new util.HashMap[EQArray, Array[Byte]]

  protected def addToCache(key: Array[Byte], value: Option[Array[Byte]]): Option[Array[Byte]] = {
    if (cacheActive)
      value match {
        case Some(v) => {
          logger.warning("Cache size is now %d", cachedValues.size)
          cachedValues.put(EQArray(key), v)
        }
        case None =>
          cachedValues.remove(EQArray(key))
      }
    value
  }

  protected def lookupInCache(key: Array[Byte]) = {
    if(cacheActive) {
      val cv = cachedValues.get(EQArray(key))
      if(cv == null)
        logger.trace("CacheMiss %s", name)
      else
        logger.trace("CacheHit %s", name)
      cv
    }
    else
      null
  }

  abstract override def getBytes(key: Array[Byte]): Option[Array[Byte]] = {
    val cachedValue = lookupInCache(key)
    if(cachedValue == null)
      addToCache(key, super.getBytes(key))
    else {
      Option(cachedValue)
    }
  }

  abstract override def putBytes(key: Array[Byte], value: Option[Array[Byte]]): Unit = {
    addToCache(key,value)
    super.putBytes(key, value)
  }

  abstract override def asyncPutBytes(key: Array[Byte], value: Option[Array[Byte]]): ScadsFuture[Unit] = {
    addToCache(key, value)
    super.asyncPutBytes(key, value)
  }

  abstract override def asyncGetBytes(key: Array[Byte]): ScadsFuture[Option[Array[Byte]]] =
    if (cacheActive) {
      val cachedValue = lookupInCache(key)
      if (cachedValue == null)
        new CachingFuture(key, super.asyncGetBytes(key))
      else {
        FutureWrapper(Option(cachedValue))
      }
    }
    else
      super.asyncGetBytes(key)

  abstract override def incrementFieldBytes(key: Array[Byte], fieldName: String, amount: Int): Unit = {
    //TODO: we could locally update the field, but for now we'll just invalidate the cache.
    addToCache(key, None)
    super.incrementFieldBytes(key, fieldName, amount)
  }

  abstract override def bulkPutBytes(key: Array[Byte], value: Option[Array[Byte]]): Unit = {
    addToCache(key, value)
    super.bulkPutBytes(key,value)
  }
}
