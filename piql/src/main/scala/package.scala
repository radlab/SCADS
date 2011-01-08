package edu.berkeley.cs
package scads

import org.apache.avro.Schema
import org.apache.avro.generic.{IndexedRecord, GenericRecord}
import org.apache.avro.util.Utf8
import net.lag.logging.Logger

import avro.marker._
import storage._

package object piql {
  protected val logger = Logger()

  type Namespace = edu.berkeley.cs.scads.storage.PairNamespace[AvroPair]
  type KeyGenerator = Seq[Value]

  type Key = IndexedRecord
  type Record = IndexedRecord
  type Tuple = IndexedSeq[Record]
  type TupleSchema = Seq[Namespace]

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

  class OptimizedQuery(val physicalPlan: QueryPlan, executor: QueryExecutor) {
    def apply(args: Any*): QueryResult = {
      val encodedArgs = args.map {
	case s: String => new Utf8(s)
	case o => o
      }
      val iterator = executor(physicalPlan, encodedArgs:_*)
      iterator.open
      val ret = iterator.toList
      iterator.close
      ret
    }
  }

  implicit def toPiql(logicalPlan: Queryable)(implicit executor: QueryExecutor) = new {
    def toPiql = {
      val physicalPlan = Optimizer(logicalPlan).physicalPlan
      logger.info("Optimized piql query: %s", physicalPlan)
      new OptimizedQuery(physicalPlan, executor)
    }
  }

}
