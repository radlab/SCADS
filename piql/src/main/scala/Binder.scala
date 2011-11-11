package edu.berkeley.cs.scads.piql
package opt

import plans._

import collection.JavaConversions._
import java.lang.RuntimeException

class Binder(plan: LogicalPlan) {

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
      .flatMap(r => r.ns.schema.getFields.map(QualifiedAttributeValue(r, _)))
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
      .groupBy(r => r.alias.getOrElse(r.ns.name))
      .map {
      case (name, rs) if rs.size == 1 => (name, UniqueRelation(rs.head))
      case (name, _) => (name, AmbiguousRelation)
    }.toMap

  lazy val qualifiedPlan = qualifyAttributes(plan)

  protected def qualifyAttributes(plan: LogicalPlan): LogicalPlan = plan match {
    case Selection(pred, child) =>
      Selection(qualifyAttributes(pred), qualifyAttributes(child))
    case Sort(attrs, asc, child) =>
      Sort(attrs.map(qualifyAttributes(_)), asc, child)
    case StopAfter(count, child) =>
      StopAfter(count, qualifyAttributes(child))
    case DataStopAfter(count, child) =>
      DataStopAfter(count, qualifyAttributes(child))
    case Join(left, right) =>
      Join(qualifyAttributes(left), qualifyAttributes(right))
    case r: Relation => r
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
        case UniqueRelation(r) => QualifiedAttributeValue(r, r.ns.schema.getField(attrName))
        case AmbiguousRelation => throw new RuntimeException("Ambiguous reference to relation: "+ relationName)
      }
    case UnboundAttributeValue(name: String) =>
      attributeMap.get(name).getOrElse(throw new RuntimeException("No such attribute:" + name)) match {
        case UniqueAttribute(a) => a
        case AmbiguousAttribute => throw new RuntimeException("Ambiguous reference to attribute: " + name)
    }
    case otherValue => otherValue
  }
}