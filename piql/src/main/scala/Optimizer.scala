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
      case GetOperation(ns, predicates, None, None) => {
        val attributeEqualityPredicates: Seq[(UnboundAttributeValue, FixedValue)] = predicates.collect {
          case EqualityPredicate(a: UnboundAttributeValue, fv: FixedValue) => a -> fv
          case EqualityPredicate(fv: FixedValue, a: UnboundAttributeValue) => a -> fv
        }

        logger.info("Generating index lookup for %s", attributeEqualityPredicates)
        val keyGenerator = ns.keySchema.getFields.map(f => {
          logger.info("Looking for attribute %s", f.name)
          attributeEqualityPredicates.find(_._1.name equals f.name).getOrElse(throw new ImplementationLimitation("Invalid prefix"))._2
        })

        IndexLookup(ns, keyGenerator)
      }
    }
  }

  case class Ordering(attributes: Seq[Value], ascending: Boolean)
  /**
   * Groups sets of logical operations that can be executed as a
   * single get operations against the key value store
   */
  protected object GetOperation {
    def unapply(logicalPlan: Queryable): Option[(Namespace, Seq[Predicate], Option[Limit],  Option[Ordering])] = {
      var predicates: List[Predicate] = Nil
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
        case Selection(p, _) => predicates ::= p
        case Relation(ns) => return Some((ns, predicates, limit, ordering))
      }
      None
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
