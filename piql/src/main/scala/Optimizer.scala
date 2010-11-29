package edu.berkeley.cs
package scads
package piql

import org.apache.avro.Schema
import scala.collection.JavaConversions._
import net.lag.logging.Logger

case class ImplementationLimitation(desc: String) extends Exception

object Optimizer {
  val logger = Logger()

  case class OptimizedSubPlan(physicalPlan: QueryPlan, schema: TupleSchema)
  def apply(logicalPlan: Queryable): OptimizedSubPlan = {
    logger.info("Begining optimization of plan: %s", logicalPlan)

    logicalPlan match {
      //TODO: Check to make sure we have the whole key in predicates
      case GetOperation(ns, equalityPreds, None, None) =>
        OptimizedSubPlan(
          IndexLookup(ns, makeKeyGenerator(ns, equalityPreds)),
          ns.keySchema :: Nil)
      case GetOperation(ns, equalityPreds, Some(limit), None) =>
        OptimizedSubPlan(
          IndexScan(ns, makeKeyGenerator(ns, equalityPreds), limit, true),
          ns.keySchema :: Nil)
      case Selection(pred, child) => {
        val optChild = apply(child)
        val boundPred = bindPredicate(pred, optChild.schema)
        optChild.copy(physicalPlan = LocalSelection(boundPred, optChild.physicalPlan))
      }
    }
  }

  protected def bindPredicate(predicate: Predicate, schema: TupleSchema): Predicate = predicate match {
    case EqualityPredicate(l, r) => EqualityPredicate(bindValue(l, schema), bindValue(r, schema))
  }

  protected def bindValue(value: Value, schema: TupleSchema): Value = value match {
    case UnboundAttributeValue(name: String) => {
      val recordPosition = schema.findIndexOf(_.getFields.map(_.name) contains name)
      val fieldPosition = schema(recordPosition).getFields.findIndexOf(_.name equals name)
      AttributeValue(recordPosition, fieldPosition)
    }
    case otherValue => otherValue
  }

  /**
   * Given a namespace and a set of attribute equality predicates return
   * at the keyGenerator
   */
  protected def makeKeyGenerator(ns: Namespace, equalityPreds: Seq[AttributeEquality]): KeyGenerator = {
    ns.keySchema.getFields.take(equalityPreds.size).map(f => {
      logger.info("Looking for attribute %s", f.name)
      equalityPreds.find(_.attribute.name equals f.name).getOrElse(throw new ImplementationLimitation("Invalid prefix")).value
    })
  }

  case class AttributeEquality(attribute: UnboundAttributeValue, value: FixedValue)
  case class Ordering(attributes: Seq[Value], ascending: Boolean)
  /**
   * Groups sets of logical operations that can be executed as a
   * single get operations against the key value store
   */
  protected object GetOperation {
    def unapply(logicalPlan: Queryable): Option[(Namespace, Seq[AttributeEquality], Option[Limit], Option[Ordering])] = {
      var predicates: List[AttributeEquality] = Nil
      val (limit, planWithoutStop) = logicalPlan match {
        case StopAfter(count, child) => (Some(FixedLimit(count)), child)
        case otherOp => (None, otherOp)
      }

      val (ordering, planWithoutSort) = planWithoutStop match {
        case Sort(attrs, asc, child) => (Some(Ordering(attrs, asc)), child)
        case otherOp => (None, otherOp)
      }

      //TODO:More functional
      planWithoutStop.walkPlan {
        case Selection(EqualityPredicate(fv: FixedValue, a: UnboundAttributeValue), _) => predicates ::= AttributeEquality(a, fv)
        case Selection(EqualityPredicate(a: UnboundAttributeValue, fv: FixedValue), _) => predicates ::= AttributeEquality(a, fv)
        case Relation(ns) => {
          val getOp = (ns, predicates, limit, ordering)
          logger.info("Matched GetOperation%s", getOp)
          return Some(getOp)
        }
        case otherOp => return None
      }
      return None
    }
  }
}
