package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm.ScadsFuture
import org.apache.avro.generic.IndexedRecord

trait KeyValueStore[KeyType <: IndexedRecord, ValueType <: IndexedRecord] {
  def put[K <: KeyType, V <: ValueType](key: K, value: V): Unit = put(key, Option(value))
  def put[K <: KeyType, V <: ValueType](key: K, value: Option[V]): Unit
  def get[K <: KeyType](key: K): Option[ValueType]
  def asyncGet[K <: KeyType](key: K): ScadsFuture[Option[ValueType]]
  def getRange(start: Option[KeyType], end: Option[KeyType], limit: Option[Int] = None, offset: Option[Int] = None, ascending: Boolean = true): Seq[(KeyType,ValueType)]
  def size():Int
  def ++=(that:Iterable[(KeyType,ValueType)]): Unit
}
