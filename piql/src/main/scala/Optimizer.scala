package edu.berkeley.cs
package scads
package piql

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import scala.collection.JavaConversions._
import net.lag.logging.Logger

case class ImplementationLimitation(desc: String) extends Exception

object Optimizer {
  val logger = Logger()

  case class OptimizedSubPlan(physicalPlan: QueryPlan, schema: TupleSchema)
  def apply(logicalPlan: Queryable): OptimizedSubPlan = {
    logger.info("Optimizing subplan: %s", logicalPlan)

    logicalPlan match {
      //TODO: Check fields match key schema
      case IndexRange(equalityPreds, None, None, Relation(ns)) if (equalityPreds.size == ns.keySchema.getFields.size) &&
								  isPrefix(equalityPreds, ns) => {
	val tupleSchema = ns :: Nil
        OptimizedSubPlan(
          IndexLookup(ns, makeKeyGenerator(ns, tupleSchema, equalityPreds)),
          tupleSchema)
      }
      case IndexRange(equalityPreds, Some(TupleLimit(count, dataStop)), None, Relation(ns)) => {
	val tupleSchema = ns :: Nil
	val idxScanPlan = IndexScan(ns, makeKeyGenerator(ns, tupleSchema, equalityPreds), count, true)
	val fullPlan = dataStop match {
	  case true => idxScanPlan
	  case false => LocalStopAfter(count, idxScanPlan)
	}
        OptimizedSubPlan(fullPlan, ns :: Nil)
      }
      //TODO: Check to make sure the index has the right ordering.
      case IndexRange(equalityPreds, Some(TupleLimit(count, dataStop)), Some(Ordering(attrs, asc)), Relation(ns)) => {
	val tupleSchema = ns :: Nil
	val idxScanPlan = IndexScan(ns, makeKeyGenerator(ns, tupleSchema, equalityPreds), count, asc)
	val fullPlan = dataStop match {
	  case true => idxScanPlan
	  case false => LocalStopAfter(count, idxScanPlan)
	}

        OptimizedSubPlan(fullPlan, ns :: Nil)
      }
      case IndexRange(equalityPreds, None, None, Join(child, Relation(ns))) if (equalityPreds.size == ns.keySchema.getFields.size) => {
        val optChild = apply(child)
	val tupleSchema = optChild.schema :+ ns
        OptimizedSubPlan(
          IndexLookupJoin(ns, makeKeyGenerator(ns, tupleSchema, equalityPreds), optChild.physicalPlan),
          tupleSchema)
      }
      //TODO: Check to make sure the index has the desired ordering.
      case IndexRange(equalityPreds, Some(TupleLimit(count, dataStop)), Some(Ordering(attrs, asc)), Join(child, Relation(ns))) => {
	val optChild = apply(child)
	val tupleSchema = optChild.schema :+ ns

	val joinPlan = IndexMergeJoin(ns,
	      makeKeyGenerator(ns, tupleSchema, equalityPreds),
	      attrs.map(bindValue(_, tupleSchema)),
	      count,
	      asc,
	      optChild.physicalPlan)

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

  /**
   * Returns true only if the given equality predicates can be satisfied by a prefix scan
   * over the given namespace
   */
  protected def isPrefix(equalityPredicates: Seq[AttributeEquality], ns: Namespace): Boolean = {
    val attrNames = equalityPredicates.map(_.attributeName)
    val primaryKeyAttrs = ns.keySchema.getFields.take(attrNames.size).map(_.name)
    attrNames.map(primaryKeyAttrs.contains(_)).reduceLeft(_ && _)
  }

  /**
   * Returns the key/value schemas concatenated
   */
  protected def getFields(ns: Namespace): Seq[Field] = ns match {
    case idx: edu.berkeley.cs.scads.storage.IndexNamespace => idx.keySchema.getFields
    case primaryNs => primaryNs.keySchema.getFields ++ primaryNs.valueSchema.getFields
  }

  /**
   * Given a list of predicates, replace all UnboundAttributeValues with Attribute values
   * with the correct record/field positions
   */
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
      val fieldPosition = getFields(schema(recordPosition)).findIndexOf(_.name equals attrName)
      AttributeValue(recordPosition, fieldPosition)
    }
    case UnboundAttributeValue(name: String) => {
      //TODO: Throw execption when ambiguious
      logger.info("attempting to bind %s in %s", name, schema)
      val recordSchemas = schema.map(getFields)
      val recordPosition = recordSchemas.findIndexOf(_.map(_.name) contains name)
      val fieldPosition = recordSchemas(recordPosition).findIndexOf(_.name equals name)
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
      val value = equalityPreds.find(_.attributeName equals f.name).getOrElse(throw new ImplementationLimitation("Invalid prefix")).value
      bindValue(value, schema)
    })
  }

  case class AttributeEquality(attributeName: String, value: Value)
  case class Ordering(attributes: Seq[Value], ascending: Boolean)
  case class TupleLimit(count: Limit, data: Boolean)
  /**
   * Groups sets of logical operations that can be executed as a
   * single get operations against the key value store
   */
  protected object IndexRange {
    def unapply(logicalPlan: Queryable): Option[(Seq[AttributeEquality], Option[TupleLimit], Option[Ordering], Queryable)] = {
      val (limit, planWithoutStop) = logicalPlan match {
        case StopAfter(count, child) => (Some(TupleLimit(FixedLimit(count), false)), child)
	case DataStopAfter(count, child) => (Some(TupleLimit(FixedLimit(count), true)), child)
        case otherOp => (None, otherOp)
      }

      val (ordering, planWithoutSort) = planWithoutStop match {
        case Sort(attrs, asc, child) => (Some(Ordering(attrs, asc)), child)
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
        case Relation(ns) => ns
        case Join(_, Relation(ns)) => ns
	case otherOp => {
	  logger.info("IndexRange match failed.  Invalid base plan: %s", otherOp)
	  return None
	}
      }

      val idxEqPreds = predicates.map {
        case EqualityPredicate(v: Value, UnboundAttributeValue(qualifiedAttribute(relName, attrName))) if relName.equals(ns.namespace) && ns.keySchema.getFields.map(_.name).contains(attrName) =>
          AttributeEquality(attrName, v)
        case EqualityPredicate(UnboundAttributeValue(qualifiedAttribute(relName, attrName)), v: Value) if relName.equals(ns.namespace) && ns.keySchema.getFields.map(_.name).contains(attrName) =>
          AttributeEquality(attrName, v)
        case EqualityPredicate(v: Value, UnboundAttributeValue(attrName)) if ns.keySchema.getFields.map(_.name).contains(attrName) =>
          AttributeEquality(attrName, v)
        case EqualityPredicate(UnboundAttributeValue(attrName), v:Value) if ns.keySchema.getFields.map(_.name).contains(attrName) =>
          AttributeEquality(attrName, v)
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
