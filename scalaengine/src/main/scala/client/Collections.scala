package edu.berkeley.cs.scads.storage

import org.apache.avro.generic._
import collection.mutable._

/**
 * Provides a set of implicit conversions on namespaces to be used
 * as scala collections
 */
object Collections {
  //implicit def nsToIterable[K <: IndexedRecord, V <: IndexedRecord](ns: RangeKeyValueStore[K, V]) = new Iterable[(K, V)] {
  //  override def iterator = ns.iterator
  //}
  implicit def nsToMap[K <: IndexedRecord, V <: IndexedRecord](ns: RangeKeyValueStore[K, V]) = new Map[K, V] {
    override def get(k: K) = ns.get(k)
    /** WARNING: ns.put(k, null) is a delete */
    override def put(k: K, v: V) = {
      val old = ns.get(k)
      ns.put(k, Option(v))
      old
    }
    /** WARNING: ns.update(k, null) is a delete */
    override def update(k: K, v: V) = put(k, v)
    override def remove(k: K) = {
      val old = ns.get(k)
      ns.put(k, None)
      old
    }
    /** WARNING: ns += Tuple2(k, null) is a delete */
    override def +=(kv: (K, V)) = {
      ns.put(kv._1, Some(kv._2))
      this
    }
    override def -=(k: K) = { 
      ns.put(k, None) 
      this
    } 
    override def iterator = ns.iterator
  }
}
