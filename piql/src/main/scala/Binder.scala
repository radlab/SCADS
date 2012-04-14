package edu.berkeley.cs.scads.piql
package opt

import plans._

import collection.JavaConversions._
import java.lang.RuntimeException

/**
 * Checks a logical query plan for invalid attributes and binds attribute strings to the
 * corresponding ns/field.
 */
class Qualifier(plan: LogicalPlan) {

  lazy val relations = getRelations(plan)

  protected def getRelations(plan: LogicalPlan): Seq[Relation] = plan match {
    case in: InnerNode => in.children.map(getRelations).reduceLeft(_ ++ _)
    case r: Relation => r :: Nil
  }

  trait NamedAttribute

  case class UniqueAttribute(attr: QualifiedAttributeValue) extends NamedAttribute

  object AmbiguousAttribute extends NamedAttribute

  lazy val attributeMap =
    relations
      .flatMap(r => r.schema.getFields.map(QualifiedAttributeValue(r, _)))
      .groupBy(a => a.field.name)
      .map {
      case (name, attrs) if attrs.size == 1 => (name, UniqueAttribute(attrs.head))
      case (name, _) => (name, AmbiguousAttribute)
    }.toMap

  trait NamedRelation

  case class UniqueRelation(r: Relation) extends NamedRelation

  object AmbiguousRelation extends NamedRelation

  lazy val relationMap =
    relations
      .groupBy(r => r.name)
      .map {
      case (name, rs) if rs.size == 1 => (name, UniqueRelation(rs.head))
      case (name, _) => (name, AmbiguousRelation)
    }.toMap

  lazy val qualifiedPlan = qualifyAttributes(plan)

  protected def qualifyAttributes(plan: LogicalPlan): LogicalPlan = plan match {
    case Selection(pred, child) =>
      Selection(qualifyAttributes(pred), qualifyAttributes(child))
    case Sort(attrs, asc, child) =>
      Sort(attrs.map(qualifyAttributes(_)), asc, qualifyAttributes(child))
    case StopAfter(count, child) =>
      StopAfter(count, qualifyAttributes(child))
    case DataStopAfter(count, child) =>
      DataStopAfter(count, qualifyAttributes(child))
    case Paginate(cnt, c) =>
      Paginate(cnt, qualifyAttributes(c))
    case Join(left, right) =>
      Join(qualifyAttributes(left), qualifyAttributes(right))
    case Project(values, child, schema) =>
      Project(values.map(qualifyAttributes), qualifyAttributes(child), schema)
    case r: TupleProvider => r
  }

  protected def qualifyAttributes(plan: Predicate): Predicate = plan match {
    case EqualityPredicate(v1, v2) =>
      EqualityPredicate(qualifyAttributes(v1), qualifyAttributes(v2))
    case LikePredicate(v1, v2) =>
      LikePredicate(qualifyAttributes(v1), qualifyAttributes(v2))
    case InPredicate(v1, v2) =>
      InPredicate(qualifyAttributes(v1), qualifyAttributes(v2))
  }

  protected val qualifiedAttribute = """([^\.]+)\.([^\.]+)""".r

  protected def qualifyAttributes(plan: Value): Value = plan match {
    case UnboundAttributeValue(qualifiedAttribute(relationName, attrName)) =>
      relationMap.get(relationName).getOrElse(throw new RuntimeException("No such relation: " + relationName)) match {
        case UniqueRelation(r) => QualifiedAttributeValue(r, r.schema.getField(attrName))
        case AmbiguousRelation => throw new RuntimeException("Ambiguous reference to relation: " + relationName)
      }
    case UnboundAttributeValue(name: String) =>
      attributeMap.get(name).getOrElse(throw new RuntimeException("No such attribute:" + name)) match {
        case UniqueAttribute(a) => a
        case AmbiguousAttribute => throw new RuntimeException("Ambiguous reference to attribute: " + name)
      }
    case otherValue => otherValue
  }
}

class Binder(plan: QueryPlan) {
  lazy val tupleSchema: TupleSchema = getTupleSchema(plan).reverse

  protected def getTupleSchema(plan: QueryPlan): TupleSchema = {
    val t: TupleSchema = plan match {
      case rp: RemotePlan => rp.namespace :: Nil
      case lp => Nil
    }
    val c: TupleSchema = plan match {
      case in: InnerPlan => getTupleSchema(in.child)
      case _ => Nil
    }
    t ++ c
  }

  lazy val boundPlan = bindPlan(plan)

  protected def bindPlan(plan: QueryPlan): QueryPlan = plan match {
    case IndexLookup(ns, kp) =>
      IndexLookup(ns, kp.map(bindValue))
    case IndexScan(ns, kp, l, asc) =>
      IndexScan(ns, kp.map(bindValue), l, asc)
    case IndexLookupJoin(ns, kp, c) =>
      IndexLookupJoin(ns, kp.map(bindValue), bindPlan(c))
    case IndexScanJoin(ns, kp, l, asc, c) =>
      IndexScanJoin(ns, kp.map(bindValue), l, asc, c)
    case IndexMergeJoin(ns, kp, sf, l, asc, c) =>
      IndexMergeJoin(ns, kp.map(bindValue), sf.map(bindValue), l, asc, bindPlan(c))
    case LocalSelection(p, c) =>
      LocalSelection(bindPredicate(p), bindPlan(c))
    case LocalSort(sf, asc, c) =>
      LocalSort(sf.map(bindValue), asc, bindPlan(c))
    case LocalStopAfter(cnt, c) =>
      LocalStopAfter(cnt, bindPlan(c))
    case LocalProjection(attrs, c, schema) =>
      LocalProjection(attrs.map(bindValue(_)), bindPlan(c), schema)
  }

  protected def bindValue(v: Value): Value = v match {
    case QualifiedAttributeValue(r, f) => {
      val recPos = tupleSchema.indexOf(r)
      assert(recPos >= 0, "invalid relation in value " + r)
      assert(f.pos >= 0, "invalid field in value " + r)
      AttributeValue(recPos, f.pos)
    }
    case otherValue => otherValue
  }

    /**
   * Given a  predicate, replace all UnboundAttributeValues with Attribute values
   * with the correct record/field positions
   */
  protected def bindPredicate(predicate: Predicate): Predicate = predicate match {
    case EqualityPredicate(l, r) => EqualityPredicate(bindValue(l), bindValue(r))
  }
}
