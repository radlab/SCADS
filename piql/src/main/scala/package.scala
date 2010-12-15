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
  implicit def toAttr(s: String) = new {
    def a = UnboundAttributeValue(s)
  }

  implicit def toParameter(o: Double) = new {
    def ? = ParameterValue(o.toInt)
  }

  implicit def toConstantValue(a: Any) = ConstantValue(a)

  implicit def toPiql(logicalPlan: Queryable)(implicit executor: QueryExecutor) = new {
    def toPiql = {
      val physicalPlan = Optimizer(logicalPlan)
      (args: Seq[Any]) => {
	val iterator = executor(physicalPlan, args:_*)
	iterator.open
	val ret = iterator.toList
	iterator.close
	ret
      }
    }
  }

}
