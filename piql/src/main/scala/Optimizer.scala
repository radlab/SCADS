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
      case IndexRange(equalityPreds, None, None, Relation(ns)) if (equalityPreds.size == ns.keySchema.getFields.size) =>
        OptimizedSubPlan(
          IndexLookup(ns, makeKeyGenerator(ns, equalityPreds)),
          ns :: Nil)
      case IndexRange(equalityPreds, Some(limit), None, Relation(ns)) =>
        OptimizedSubPlan(
          IndexScan(ns, makeKeyGenerator(ns, equalityPreds), limit, true),
          ns :: Nil)
      case IndexRange(equalityPreds, None, None, Join(child, Relation(ns))) if (equalityPreds.size == ns.keySchema.getFields.size) => {
        val optChild = apply(child)
        OptimizedSubPlan(
          IndexLookupJoin(ns, makeKeyGenerator(ns, equalityPreds), optChild.physicalPlan),
          optChild.schema :+ ns)
      }
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

  protected val qualifiedAttribute = """([^\.]+)\.([^\.]+)""".r
  protected def bindValue(value: Value, schema: TupleSchema): Value = value match {
    case UnboundAttributeValue(qualifiedAttribute(relationName, attrName)) => {
      logger.info("attempting to bind qualified attribute: %s.%s in %s", relationName, attrName, schema)
      val relationNames = schema.map(_.namespace)
      logger.info("selecting from relationName options: %s", relationNames)
      val recordPosition = relationNames.findIndexOf(_ equals relationName)
      val fieldPosition = schema(recordPosition).pairSchema.getFields.findIndexOf(_.name equals attrName)
      AttributeValue(recordPosition, fieldPosition)
    }
    case UnboundAttributeValue(name: String) => {
      //TODO: Throw execption when ambiguious
      logger.info("attempting to bind %s in %s", name, schema)
      val recordSchemas = schema.map(_.pairSchema)
      val recordPosition = recordSchemas.findIndexOf(_.getFields.map(_.name) contains name)
      val fieldPosition = recordSchemas(recordPosition).getFields.findIndexOf(_.name equals name)
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
  protected object IndexRange {
    def unapply(logicalPlan: Queryable): Option[(Seq[AttributeEquality], Option[Limit], Option[Ordering], Queryable)] = {
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
        case otherOp => {
          val getOp = (predicates, limit, ordering, otherOp)
          logger.info("Matched IndexRange%s", getOp)
          return Some(getOp)
        }
      }
      return None
    }
  }
}
