package edu.berkeley.cs
package scads
package piql

import org.apache.avro.Schema
import scala.collection.JavaConversions._
import net.lag.logging.Logger

case class ImplementationLimitation(desc: String) extends Exception

object Optimizer {
  val logger = Logger()

  def apply(logicalPlan: Queryable): QueryPlan = {
    logger.info("Begining optimization of plan: %s", logicalPlan)
    val resultSchema = getSchema(logicalPlan)
    logger.info("Query Result Schema: %s", resultSchema)

    logicalPlan match {
      //TODO: Check to make sure we have the whole key in predicates
      case GetOperation(ns, equalityPreds, None, None) => IndexLookup(ns, makeKeyGenerator(ns, equalityPreds))
      case GetOperation(ns, equalityPreds, Some(limit), None) => IndexScan(ns, makeKeyGenerator(ns, equalityPreds), limit, true)
      case Selection(pred, child) => LocalSelection(pred, apply(child))
    }
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
    def unapply(logicalPlan: Queryable): Option[(Namespace, Seq[AttributeEquality], Option[Limit],  Option[Ordering])] = {
      var predicates: List[AttributeEquality] = Nil
      val (limit, planWithoutStop) = logicalPlan match {
        case StopAfter(count, child) => (Some(FixedLimit(count)), child)
        case otherOp => (None, otherOp)
      }

      val (ordering, planWithoutSort) = planWithoutStop match {
        case Sort(attrs , asc, child) => (Some(Ordering(attrs, asc)), child)
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

  protected def getSchema(logicalPlan: Queryable, currentSchema: List[Schema] = Nil): List[Schema] = {
    logicalPlan match {
      case in: InnerNode => getSchema(in.child, currentSchema)
      case j@Join(in: InnerNode, r: Relation) => getSchema(in.child, r.ns.pairSchema :: currentSchema)
      case Relation(ns) => ns.pairSchema :: Nil
    }
  }
}
