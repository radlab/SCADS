package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm.ScadsFuture
import org.apache.avro.generic.IndexedRecord

trait KeyValueStore[KeyType <: IndexedRecord, ValueType <: IndexedRecord, RetType <: IndexedRecord] {
  def put(key: KeyType, value: ValueType): Unit = put(key, Option(value))
  def put(key: KeyType, value: Option[ValueType]): Unit
  def get(key: KeyType): Option[RetType]
  def asyncGet(key: KeyType): ScadsFuture[Option[RetType]]
  def getRange(start: Option[KeyType], 
               end: Option[KeyType], 
               limit: Option[Int] = None, 
               offset: Option[Int] = None, 
               ascending: Boolean = true): Seq[(KeyType, RetType)]
  def asyncGetRange(start: Option[KeyType], 
                    end: Option[KeyType], 
                    limit: Option[Int] = None, 
                    offset: Option[Int] = None, 
                    ascending: Boolean = true): ScadsFuture[Seq[(KeyType, RetType)]]
  def size(): Int
  /** TODO: make TraversableOnce[(KeyType, ValueType)] */
  def ++=(that: Iterable[(KeyType, ValueType)]): Unit
}
