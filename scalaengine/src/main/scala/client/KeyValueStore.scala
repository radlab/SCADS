package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm.ScadsFuture
import org.apache.avro.generic.IndexedRecord

/**
 * The main interface to interacting with SCADS.
 * The type parameters for this trait are explained as follows
 * `KeyType` - The type of key for this store. Must be an indexed record
 * `ValueType` - The type of value for this store. Must be an indexed record
 * `RecordType` - The type of object returned from a get operation. Must be an
 *                indexed record.
 * `RangeType` - The type of object returned from a getRange operation. Should
 *               either be an indexed record itself, or a collection of
 *               indexed records. unfortunately there is no easy way to
 *               enforce this requirement in the type system, so it has no
 *               upper type bound
 */
trait KeyValueStore[KeyType <: IndexedRecord,
                    ValueType <: IndexedRecord, 
                    RecordType <: IndexedRecord,
                    RangeType] {
  def put(key: KeyType, value: ValueType): Unit = put(key, Option(value))
  def put(key: KeyType, value: Option[ValueType]): Unit
  def get(key: KeyType): Option[RecordType]
  def asyncGet(key: KeyType): ScadsFuture[Option[RecordType]]
  def getRange(start: Option[KeyType], 
               end: Option[KeyType], 
               limit: Option[Int] = None, 
               offset: Option[Int] = None, 
               ascending: Boolean = true): Seq[RangeType]
  def asyncGetRange(start: Option[KeyType], 
                    end: Option[KeyType], 
                    limit: Option[Int] = None, 
                    offset: Option[Int] = None, 
                    ascending: Boolean = true): ScadsFuture[Seq[RangeType]]
  def size(): Int
  def ++=(that: TraversableOnce[RangeType]): Unit
}
