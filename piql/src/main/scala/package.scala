package edu.berkeley.cs
package scads

import org.apache.avro.generic.IndexedRecord
import net.lag.logging.Logger

import avro.marker._
import storage._
import storage.client._
import org.apache.avro.util.Utf8


package object piql {

  import language._
  import exec._
  import opt._
  import plans._

  protected val logger = Logger()

  type Namespace = storage.Namespace with RecordStore[IndexedRecord] with GlobalMetadata
  type IndexedNamespace = storage.Namespace with RecordStore[IndexedRecord] with GlobalMetadata with index.IndexManager[AvroPair]

  type KeyGenerator = Seq[Value]

  type Key = IndexedRecord
  type Record = IndexedRecord
  type Tuple = IndexedSeq[Record]
  type TupleSchema = Seq[Namespace]
  type Field = org.apache.avro.Schema.Field

  type QueryResult = Seq[Tuple]
  type QueryExecutor = exec.QueryExecutor
  type ParallelExecutor = exec.ParallelExecutor

  //Query State Sterilization
  type CursorPosition = Seq[Any]

  implicit def toRichTuple(t: Tuple) = new debug.RichTuple(t)

  //TODO: Remove hack
  //HACK: to deal with problem in namespace variance
  implicit def pairToNamespace[T <: AvroPair](ns: PairNamespace[T]) = ns.asInstanceOf[IndexedNamespace]

  //PIQL Scala Language Integration
  implicit def namespaceToRelation[T <: AvroPair](ns: PairNamespace[T]) = Relation(ns.asInstanceOf[IndexedNamespace])

  implicit def toAttr(s: String) = new {
    def a = UnboundAttributeValue(s)
  }

  implicit def toParameter(o: Double) = new {
    def ? = ParameterValue(o.toInt)
  }

  implicit def toConstantValue(a: Any) = ConstantValue(a)

  implicit def toOption[A](a: A) = Option(a)


  /* Helper functions for running logical plans through optimizer pipeline */

  class OptimizedQuery(val name: Option[String], val logicalPlan: LogicalPlan, val physicalPlan: QueryPlan, executor: QueryExecutor) {
    def apply(args: Any*): QueryResult = {
      val encodedArgs = args.map {
        case s: String => new Utf8(s)
        case o => o
      }
      val iterator = executor(physicalPlan, encodedArgs: _*)
      iterator.open
      val ret = iterator.toList
      iterator.close
      ret
    }

    def toHtml: xml.NodeSeq = {
      <b>
        {physicalPlan}
      </b>
    }
  }

  implicit def toPiql(logicalPlan: LogicalPlan)(implicit executor: QueryExecutor) = new {
    def toPiql(queryName: Option[String] = None) = {
      logger.info("Begining Optimization of query %s: %s", queryName, logicalPlan)
      val boundPlan = new Binder(logicalPlan).qualifiedPlan
      logger.info("Plan after binding: %s", boundPlan)
      val physicalPlan = Optimizer(boundPlan).physicalPlan
      logger.info("Optimized piql query %s: %s", queryName, physicalPlan)
      new OptimizedQuery(queryName, logicalPlan, physicalPlan, executor)
    }
  }

}
