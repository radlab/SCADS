package edu.berkeley.cs
package scads

import org.apache.avro.generic.{IndexedRecord, GenericRecord}
import avro.marker._
import storage._

package object piql {
  type Namespace = edu.berkeley.cs.scads.storage.PairNamespace[AvroPair]
  type KeyGenerator = Seq[Value]

  type Key = IndexedRecord
  type Record = IndexedRecord
  type Tuple = IndexedSeq[Record]
  type QueryResult = Seq[Tuple]

  //Query State Serailization
  type CursorPosition = Seq[Any]
  
  implicit def toRichTuple(t: Tuple) = new RichTuple(t)
  
  //PIQL Scala Language Integration
  implicit def namespaceToRelation[T <: AvroPair](ns: PairNamespace[T]) = Relation(ns.asInstanceOf[Namespace])
}
