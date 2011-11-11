package edu.berkeley.cs
package scads
package piql
package opt

import plans._
import storage.client.index._

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.util.Utf8
import scala.collection.JavaConversions._
import net.lag.logging.Logger

case class ImplementationLimitation(desc: String) extends Exception

object OptimizedQuery {
  val physicalPlans = new java.util.concurrent.ConcurrentHashMap[Int, QueryPlan]()
}

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

object Optimizer {
  val logger = Logger()
  val defaultFetchSize = 10

  case class OptimizedSubPlan(physicalPlan: QueryPlan, schema: TupleSchema)

  def apply(logicalPlan: LogicalPlan): OptimizedSubPlan = {
    logger.info("Optimizing subplan: %s", logicalPlan)

    logicalPlan match {
      case IndexRange(equalityPreds, None, None, Relation(ns, _)) if ((equalityPreds.size == ns.keySchema.getFields.size) &&
        isPrefix(equalityPreds.map(_.attribute.unqualifiedName), ns)) => {
        val tupleSchema = ns :: Nil
        OptimizedSubPlan(
          IndexLookup(ns, makeKeyGenerator(ns, tupleSchema, equalityPreds)),
          tupleSchema)
      }
      case IndexRange(equalityPreds, Some(TupleLimit(count, dataStop)), None, Relation(ns, _)) => {
        if (isPrefix(equalityPreds.map(_.attribute.unqualifiedName), ns)) {
          logger.info("Using primary index for predicates: %s", equalityPreds)
          val tupleSchema = ns :: Nil
          val idxScanPlan = IndexScan(ns, makeKeyGenerator(ns, tupleSchema, equalityPreds), count, true)
          val fullPlan = dataStop match {
            case true => idxScanPlan
            case false => LocalStopAfter(count, idxScanPlan)
          }
          OptimizedSubPlan(fullPlan, tupleSchema)
        } else {
          logger.info("Using secondary index for predicates: %s", equalityPreds)

          //TODO: Fix type hack
          val idx = ns.asInstanceOf[IndexedNamespace].getOrCreateIndex(equalityPreds.map(p => AttributeIndex(p.attribute.unqualifiedName)))
          val tupleSchema = idx :: ns :: Nil
          val idxScanPlan = IndexScan(idx, makeKeyGenerator(idx, tupleSchema, equalityPreds), count, true)
          val derefedPlan = derefPlan(ns, idxScanPlan)

          val fullPlan = dataStop match {
            case true => derefedPlan
            case false => LocalStopAfter(count, derefedPlan)
          }
          OptimizedSubPlan(fullPlan, tupleSchema)
        }
      }
      case IndexRange(equalityPreds, bound, Some(Ordering(attrs, asc)), Relation(ns, _)) => {
        val limitHint = bound.map(_.count).getOrElse {
          logger.warning("UnboundedPlan %s: %s", ns, logicalPlan)
          FixedLimit(defaultFetchSize)
        }
        val isDataStop = bound.map(_.isDataStop).getOrElse(true)
        val prefixAttrs = equalityPreds.map(_.attribute.unqualifiedName) ++ attrs.map(_.unqualifiedName)
        val (idxScanPlan, tupleSchema) =
          if (isPrefix(prefixAttrs, ns)) {
            val tupleSchema = ns :: Nil
            (IndexScan(ns, makeKeyGenerator(ns, tupleSchema, equalityPreds), limitHint, asc), tupleSchema)
          }
          else {
            logger.debug("Creating index for attributes: %s", prefixAttrs)
            val idx = ns.asInstanceOf[IndexedNamespace].getOrCreateIndex(prefixAttrs.map(p => AttributeIndex(p)))
            val tupleSchema = idx :: ns :: Nil
            (derefPlan(ns,
              IndexScan(idx,
                makeKeyGenerator(idx, tupleSchema, equalityPreds),
                limitHint,
                asc)),
              tupleSchema)
          }

        val fullPlan = isDataStop match {
          case true => idxScanPlan
          case false => LocalStopAfter(limitHint, idxScanPlan)
        }
        OptimizedSubPlan(fullPlan, tupleSchema)
      }
      case IndexRange(equalityPreds, None, None, Join(child, Relation(ns, _))) if (equalityPreds.size == ns.keySchema.getFields.size) &&
        isPrefix(equalityPreds.map(_.attribute.unqualifiedName), ns) => {
        val optChild = apply(child)
        val tupleSchema = optChild.schema :+ ns
        OptimizedSubPlan(
          IndexLookupJoin(ns, makeKeyGenerator(ns, tupleSchema, equalityPreds), optChild.physicalPlan),
          tupleSchema)
      }
      case IndexRange(equalityPreds, Some(TupleLimit(count, dataStop)), Some(Ordering(attrs, asc)), Join(child, Relation(ns, _))) => {
        val prefixAttrs = equalityPreds.map(_.attribute.unqualifiedName) ++ attrs.map(_.unqualifiedName)
        val optChild = apply(child)

        val (joinPlan, tupleSchema) =
          if (isPrefix(prefixAttrs, ns)) {
            val tupleSchema = optChild.schema :+ ns
            logger.debug("Using index special orders for %s", attrs)

            (IndexMergeJoin(ns,
              makeKeyGenerator(ns, tupleSchema, equalityPreds),
              attrs.map(bindValue(_, tupleSchema)),
              count,
              asc,
              optChild.physicalPlan),
              tupleSchema)
          } else {
            val idx = ns.asInstanceOf[IndexedNamespace].getOrCreateIndex(prefixAttrs.map(p => AttributeIndex(p)))
            val tupleSchema = idx :: ns :: Nil

            val idxJoinPlan = IndexScanJoin(idx,
              makeKeyGenerator(idx, tupleSchema, equalityPreds),
              count,
              asc,
              optChild.physicalPlan)
            (derefPlan(ns, idxJoinPlan), tupleSchema)
          }

        val fullPlan = dataStop match {
          case true => joinPlan
          case false => LocalStopAfter(count, joinPlan)
        }

        OptimizedSubPlan(fullPlan, tupleSchema)
      }
      case Selection(pred, child) => {
        val optChild = apply(child)
        val boundPred = bindPredicate(pred, optChild.schema)
        optChild.copy(physicalPlan = LocalSelection(boundPred, optChild.physicalPlan))
      }
    }
  }

  protected def derefPlan(ns: Namespace, idxPlan: RemotePlan): QueryPlan = {
    val keyFields = ns.keySchema.getFields
    val idxFields = getFields(idxPlan.namespace)
    val keyGenerator = keyFields.map(kf => AttributeValue(0, idxFields.indexWhere(_.name equals kf.name)))
    IndexLookupJoin(ns, keyGenerator, idxPlan)
  }

  /**
   * Returns true only if the given equality predicates can be satisfied by a prefix scan
   * over the given namespace
   */
  protected def isPrefix(attrNames: Seq[String], ns: Namespace): Boolean = {
    val primaryKeyAttrs = ns.keySchema.getFields.take(attrNames.size).map(_.name)
    attrNames.map(primaryKeyAttrs.contains(_)).reduceLeft(_ && _)
  }

  /**
   * Returns the key/value schemas concatenated
   * TODO: push into storage client
   */
  protected def getFields(ns: Namespace): Seq[Field] =
    ns.keySchema.getFields.toSeq ++ ns.valueSchema.getFields

  /**
   * Given a list of predicates, replace all UnboundAttributeValues with Attribute values
   * with the correct record/field positions
   */
  protected def bindPredicate(predicate: Predicate, schema: TupleSchema): Predicate = predicate match {
    case EqualityPredicate(l, r) => EqualityPredicate(bindValue(l, schema), bindValue(r, schema))
  }

  protected val qualifiedAttribute = """([^\.]+)\.([^\.]+)""".r
88
  protected def bindValue(value: Value, schema: TupleSchema): Value = value match {
    case UnboundAttributeValue(qualifiedAttribute(relationName, attrName)) => {
      logger.debug("attempting to bind qualified attribute: %s.%s in %s", relationName, attrName, schema)
      val relationNames = schema.map(_.namespace)
      val recordPosition = relationNames.indexWhere(_ equals relationName)
      val fields = getFields(schema(recordPosition))
      val fieldPosition = fields.indexWhere(_.name equals attrName)
      logger.debug("selecting position (%s):%d, (%s):%d ", relationNames.mkString(","), recordPosition, fields.mkString(","), fieldPosition)
      AttributeValue(recordPosition, fieldPosition)
    }
    case UnboundAttributeValue(name: String) => {
      //TODO: Throw execption when ambiguious
      logger.info("attempting to bind %s in %s", name, schema)
      val recordSchemas = schema.map(getFields)
      val recordPosition = recordSchemas.indexWhere(_.map(_.name) contains name)
      val fieldPosition = recordSchemas(recordPosition).indexWhere(_.name equals name)
      AttributeValue(recordPosition, fieldPosition)
    }
    case otherValue => otherValue
  }

  /**
   * Given a namespace and a set of attribute equality predicates return
   * at the keyGenerator
   */
  protected def makeKeyGenerator(ns: Namespace, schema: TupleSchema, equalityPreds: Seq[AttributeEquality]): KeyGenerator = {
    ns.keySchema.getFields.take(equalityPreds.size).map(f => {
      logger.info("Looking for key generator value for field %s in %s", f.name, equalityPreds)
      val value = equalityPreds.find(_.attribute.unqualifiedName equals f.name).getOrElse(throw new ImplementationLimitation("Invalid prefix")).value
      bindValue(value, schema)
    })
  }

  case class AttributeEquality(attribute: UnboundAttributeValue, value: Value)

  case class Ordering(attributes: Seq[UnboundAttributeValue], ascending: Boolean)

  case class TupleLimit(count: Limit, isDataStop: Boolean)

  /**
   * Groups sets of logical operations that can be executed as a
   * single get operations against the key value store
   */
  protected object IndexRange {
    def unapply(logicalPlan: LogicalPlan): Option[(Seq[AttributeEquality], Option[TupleLimit], Option[Ordering], LogicalPlan)] = {
      val (limit, planWithoutStop) = logicalPlan match {
        case StopAfter(count, child) => (Some(TupleLimit(count, false)), child)
        case DataStopAfter(count, child) => (Some(TupleLimit(count, true)), child)
        case otherOp => (None, otherOp)
      }

      //TODO: check to make sure these are fields in the base relation
      val (ordering, planWithoutSort) = planWithoutStop match {
        case Sort(attrs, asc, child) if (attrs.map(_.isInstanceOf[UnboundAttributeValue]).reduceLeft(_ && _)) => {
          (Some(Ordering(attrs.asInstanceOf[Seq[UnboundAttributeValue]], asc)), child)
        }
        case otherOp => (None, otherOp)
      }

      val (predicates, planWithoutPredicates) = planWithoutSort.gatherUntil {
        case Selection(pred, _) => pred
      }

      val basePlan = planWithoutPredicates.getOrElse {
        logger.info("IndexRange match failed.  No base plan")
        return None
      }

      val ns = basePlan match {
        case Relation(ns, _) => ns
        case Join(_, Relation(ns, _)) => ns
        case otherOp => {
          logger.info("IndexRange match failed.  Invalid base plan: %s", otherOp)
          return None
        }
      }

      val fields = getFields(ns)

      val idxEqPreds = predicates.map {
        case EqualityPredicate(v: Value, u@UnboundAttributeValue(qualifiedAttribute(relName, attrName))) if relName.equals(ns.namespace) && fields.map(_.name).contains(attrName) =>
          AttributeEquality(u, v)
        case EqualityPredicate(u@UnboundAttributeValue(qualifiedAttribute(relName, attrName)), v: Value) if relName.equals(ns.namespace) && fields.map(_.name).contains(attrName) =>
          AttributeEquality(u, v)
        case EqualityPredicate(v: Value, u@UnboundAttributeValue(attrName)) if fields.map(_.name).contains(attrName) =>
          AttributeEquality(u, v)
        case EqualityPredicate(u@UnboundAttributeValue(attrName), v: Value) if fields.map(_.name).contains(attrName) =>
          AttributeEquality(u, v)
        case otherPred => {
          logger.info("IndexScan match failed.  Can't apply %s to index scan of %s.{%s}", otherPred, ns.namespace, ns.keySchema.getFields.map(_.name))
          return None
        }
      }

      val getOp = (idxEqPreds, limit, ordering, basePlan)
      logger.info("Matched IndexRange%s", getOp)
      Some(getOp)
    }
  }

}
