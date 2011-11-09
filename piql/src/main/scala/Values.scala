package edu.berkeley.cs.scads.piql
package plans

abstract class Value {
  def ===(value: Value) = EqualityPredicate(this, value)
  def like(value: Value) = LikePredicate(this, value)
}

/* Fixed Values.  i.e. Values that arent depended on a specific tuple */
abstract class FixedValue extends Value
case class ConstantValue(v: Any) extends FixedValue
case class ParameterValue(ordinal: Int) extends FixedValue

/* Attibute Values */
case class AttributeValue(recordPosition: Int, fieldPosition: Int) extends Value

//TODO: write qualifier.
case class QualifiedAttributeValue(ns: Namespace, field: Field)

case class UnboundAttributeValue(name: String) extends Value {
  protected val qualifiedAttribute = """([^\.]+)\.([^\.]+)""".r
  def relationName: Option[String] = name match {
    case qualifiedAttribute(r,f) => Some(r)
    case _ => None
  }

  def unqualifiedName: String  = name match {
    case qualifiedAttribute(r,f) => f
    case _ => name
  }
}

